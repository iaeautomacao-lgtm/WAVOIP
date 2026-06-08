from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import requests
import os
import time
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from tasks import celery, make_call_task
load_dotenv()

app = Flask(__name__)
CORS(app)

# ── Credenciais ────────────────────────────────────────────────
WAVOIP_EMAIL      = os.getenv("WAVOIP_EMAIL")
WAVOIP_PASSWORD   = os.getenv("WAVOIP_PASSWORD")
WAVOIP_BASE       = "https://api.wavoip.com"

VAPI_API_KEY      = os.getenv("VAPI_API_KEY")
VAPI_ASSISTANT_ID = os.getenv("VAPI_ASSISTANT_ID")
VAPI_BASE         = "https://api.vapi.ai"

SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Mapeamento token Wavoip → phoneNumberId Vapi ──────────────
WAVOIP_VAPI_MAP = {
    "88b232ad-5c1d-404f-8652-f5399e6a6f51": os.getenv("VAPI_PHONE_NUMBER_ID_1"),  # Cruzeiro
    "ed49616d-fb19-46df-96b8-decab4cde3cf": os.getenv("VAPI_PHONE_NUMBER_ID_2"),  # Dispositivo 1
    "c8af8686-ca83-4eef-b757-f53940426011": os.getenv("VAPI_PHONE_NUMBER_ID_3"),  # Dispositivo 2
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43": os.getenv("VAPI_PHONE_NUMBER_ID_4"),  # Dispositivo 3
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb": os.getenv("VAPI_PHONE_NUMBER_ID_5"),  # Dispositivo 4
}

# Ordem de prioridade das linhas
DEVICE_PRIORITY = [
    "88b232ad-5c1d-404f-8652-f5399e6a6f51",  # Cruzeiro
    "ed49616d-fb19-46df-96b8-decab4cde3cf",  # Dispositivo 1
    "c8af8686-ca83-4eef-b757-f53940426011",  # Dispositivo 2
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43",  # Dispositivo 3
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb",  # Dispositivo 4
]

# Round-robin global
_round_robin_idx = 0

# ── Token cache Wavoip ─────────────────────────────────────────
_wavoip_token    = None
_wavoip_token_ts = 0
TOKEN_TTL        = 600

def wavoip_login() -> str:
    global _wavoip_token, _wavoip_token_ts
    if _wavoip_token and (time.time() - _wavoip_token_ts) < TOKEN_TTL:
        return _wavoip_token
    res = requests.post(f"{WAVOIP_BASE}/v2/login", json={
        "email": WAVOIP_EMAIL, "password": WAVOIP_PASSWORD
    }, timeout=10)
    res.raise_for_status()
    _wavoip_token    = res.json()["data"]["token"]
    _wavoip_token_ts = time.time()
    return _wavoip_token

def wavoip_get_devices(token: str) -> list:
    res = requests.get(f"{WAVOIP_BASE}/v2/devices/me",
        headers={"Authorization": f"Bearer {token}"}, timeout=10)
    res.raise_for_status()
    return res.json().get("data", [])

def normalize_cpf(cpf: str) -> str:
    if not cpf:
        return ""
    return str(cpf).replace(".", "").replace("-", "").replace("/", "").strip().zfill(11)

def get_healthy_lines() -> list:
    """
    Retorna lista de dispositivos saudáveis ordenada por DEVICE_PRIORITY.
    Saudável = status open + não desativado + tem número vinculado.
    """
    token      = wavoip_login()
    devices    = wavoip_get_devices(token)
    device_map = {d.get("token"): d for d in devices}

    return [
        device_map[t] for t in DEVICE_PRIORITY
        if t in device_map
        and device_map[t].get("status") == "open"
        and device_map[t].get("disabled") == 0
        and device_map[t].get("phone") is not None
    ]

def vapi_call(phone: str) -> dict:
    """
    Normaliza número pra E.164, tenta cada linha saudável em
    sequência (round-robin com fallback automático).
    Só falha se todas as linhas falharem.
    """
    global _round_robin_idx

    digits = ''.join(filter(str.isdigit, phone))
    if not digits.startswith("55"):
        digits = "55" + digits
    phone_e164 = "+" + digits

    try:
        healthy = get_healthy_lines()
    except Exception as e:
        raise Exception(f"Erro ao consultar linhas Wavoip: {str(e)}")

    if not healthy:
        raise Exception("Nenhuma linha SIP disponível no momento")

    # ponto de início do round-robin
    start_idx        = _round_robin_idx % len(healthy)
    _round_robin_idx += 1

    last_error = None

    for i in range(len(healthy)):
        line     = healthy[(start_idx + i) % len(healthy)]
        phone_id = WAVOIP_VAPI_MAP.get(line.get("token"))

        if not phone_id:
            # linha ainda não mapeada no Vapi, pula
            last_error = f"Linha '{line.get('name')}' sem phoneNumberId mapeado"
            continue

        try:
            res = requests.post(f"{VAPI_BASE}/call/phone", json={
                "phoneNumberId": phone_id,
                "assistantId":   VAPI_ASSISTANT_ID,
                "customer":      {"number": phone_e164}
            }, headers={
                "Authorization": f"Bearer {VAPI_API_KEY}",
                "Content-Type":  "application/json"
            }, timeout=15)

            if res.ok:
                return res.json()  # ✅ sucesso

            # falhou nessa linha → tenta próxima
            try:    detail = res.json()
            except: detail = res.text
            last_error = f"Linha '{line.get('name')}' — Vapi {res.status_code}: {detail}"

        except Exception as ex:
            last_error = f"Linha '{line.get('name')}' — erro: {str(ex)}"
            continue

    raise Exception(f"Todas as linhas falharam. Último erro: {last_error}")

# ── Rotas ─────────────────────────────────────────────────────
@app.route("/api/campaign/start", methods=["POST"])
def campaign_start():
    """
    Recebe lista de contatos com débito e enfileira
    uma tarefa Celery por contato.
    """
    try:
        body      = request.json
        contacts  = body.get("contacts", [])
        campaign_id = body.get("campaign_id") or str(int(time.time()))

        if not contacts:
            return jsonify({"ok": False, "error": "Nenhum contato"}), 400

        enqueued = 0
        for c in contacts:
            if not c.get("phone"):
                continue
            make_call_task.delay(campaign_id, c)
            enqueued += 1

        return jsonify({
            "ok":          True,
            "campaign_id": campaign_id,
            "enqueued":    enqueued
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaign/<campaign_id>/status", methods=["GET"])
def campaign_status(campaign_id):
    """Retorna progresso da campanha pelo Supabase."""
    try:
        result = supabase.table("campaign_calls").select(
            "status", count="exact"
        ).eq("campaign_id", campaign_id).execute()

        total     = result.count or 0
        disparado = sum(1 for r in result.data if r["status"] == "disparado")
        erro      = sum(1 for r in result.data if r["status"] == "erro")
        pendente  = sum(1 for r in result.data if r["status"] not in ["disparado","erro","sem_telefone","falha_sem_linha"])

        return jsonify({
            "ok":        True,
            "campaign_id": campaign_id,
            "total":     total,
            "disparado": disparado,
            "erro":      erro,
            "pendente":  pendente,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/lines", methods=["GET"])
def get_lines():
    try:
        token   = wavoip_login()
        devices = wavoip_get_devices(token)
        lines   = [{
            "id":            d.get("id"),
            "name":          d.get("name"),
            "phone":         d.get("phone"),
            "status":        d.get("status"),
            "disabled":      d.get("disabled"),
            "calls_made":    d.get("calls_made"),
            "needs_restart": d.get("needs_restart"),
            "token":         d.get("token"),
            "healthy":       d.get("status") == "open"
                             and d.get("disabled") == 0
                             and d.get("phone") is not None
        } for d in devices]
        return jsonify({"ok": True, "data": lines})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/contacts", methods=["GET"])
def get_contacts():
    try:
        page     = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))
        search   = request.args.get("search", "").strip()
        offset   = (page - 1) * per_page

        query = supabase.table("contacts").select(
            "id, name, cpf, institution, phones, cpf_norm", count="exact"
        )
        if search:
            query = query.ilike("name", f"%{search}%")

        result = query.range(offset, offset + per_page - 1).execute()
        return jsonify({
            "ok":    True,
            "data":  result.data,
            "total": result.count,
            "page":  page,
            "pages": -(-result.count // per_page)
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/calls", methods=["GET"])
def get_calls():
    try:
        page     = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))
        offset   = (page - 1) * per_page

        result = supabase.table("calls").select(
            "*", count="exact"
        ).order("created_at", desc=True).range(offset, offset + per_page - 1).execute()

        return jsonify({
            "ok":    True,
            "data":  result.data,
            "total": result.count,
            "page":  page,
            "pages": -(-result.count // per_page)
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/call", methods=["POST"])
def make_call():
    try:
        body  = request.json
        phone = body.get("phone")
        if not phone:
            return jsonify({"ok": False, "error": "phone obrigatório"}), 400

        vapi_data = vapi_call(phone)
        return jsonify({"ok": True, "call_id": vapi_data.get("id"), "data": vapi_data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/import/preview", methods=["POST"])
def import_preview():
    try:
        if "file" not in request.files:
            return jsonify({"ok": False, "error": "Nenhum arquivo enviado"}), 400

        file  = request.files["file"]
        fname = file.filename.lower()

        if fname.endswith(".csv"):
            df = pd.read_csv(file, dtype=str)
        elif fname.endswith(".xlsx") or fname.endswith(".xls"):
            df = pd.read_excel(file, dtype=str)
        else:
            return jsonify({"ok": False, "error": "Formato inválido. Use .csv ou .xlsx"}), 400

        df.columns = [c.strip().lower() for c in df.columns]

        cpf_col  = next((c for c in df.columns if "cpf"    in c), None)
        nome_col = next((c for c in df.columns if "nome"   in c or "name"    in c), None)
        tel_col  = next((c for c in df.columns if "tel"    in c or "fone"    in c
                                                or "phone" in c or "celular" in c), None)

        if not cpf_col:
            return jsonify({"ok": False, "error": "Coluna CPF não encontrada"}), 400
        if not nome_col:
            return jsonify({"ok": False, "error": "Coluna Nome não encontrada"}), 400

        rows = []
        cpfs = []
        for _, row in df.iterrows():
            cpf_raw  = str(row.get(cpf_col, "") or "").strip()
            cpf_norm = normalize_cpf(cpf_raw)
            if not cpf_norm:
                continue
            cpfs.append(cpf_norm)
            rows.append({
                "cpf":     cpf_norm,
                "cpf_raw": cpf_raw,
                "name":    str(row.get(nome_col, "") or "").strip(),
                "phone":   str(row.get(tel_col,  "") or "").strip() if tel_col else "",
            })

        if not rows:
            return jsonify({"ok": False, "error": "Nenhum CPF válido encontrado"}), 400

        found_cpfs = set()
        for i in range(0, len(cpfs), 100):
            batch = cpfs[i:i+100]
            res1 = supabase.table("contacts").select("cpf, cpf_norm").in_("cpf",      batch).execute()
            res2 = supabase.table("contacts").select("cpf, cpf_norm").in_("cpf_norm", batch).execute()
            for r in (res1.data or []):
                found_cpfs.add(normalize_cpf(r.get("cpf",      "")))
                found_cpfs.add(normalize_cpf(r.get("cpf_norm", "") or ""))
            for r in (res2.data or []):
                found_cpfs.add(normalize_cpf(r.get("cpf",      "")))
                found_cpfs.add(normalize_cpf(r.get("cpf_norm", "") or ""))

        for row in rows:
            row["has_debt"] = row["cpf"] in found_cpfs

        total     = len(rows)
        with_debt = sum(1 for r in rows if r["has_debt"])

        return jsonify({
            "ok":          True,
            "rows":        rows[:200],
            "total":       total,
            "with_debt":   with_debt,
            "without_debt":total - with_debt,
            "columns":     {"cpf": cpf_col, "name": nome_col, "phone": tel_col}
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/import/call-batch", methods=["POST"])
def call_batch():
    try:
        body     = request.json
        contacts = body.get("contacts", [])
        if not contacts:
            return jsonify({"ok": False, "error": "Nenhum contato"}), 400

        results = []
        for c in contacts:
            phone = c.get("phone", "").strip()
            if not phone:
                results.append({"cpf": c.get("cpf"), "status": "sem_telefone"})
                continue
            try:
                data = vapi_call(phone)
                results.append({"cpf": c.get("cpf"), "status": "disparado", "call_id": data.get("id")})
            except Exception as ex:
                results.append({"cpf": c.get("cpf"), "status": "erro", "error": str(ex)})

        return jsonify({"ok": True, "results": results, "total": len(results)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000) 