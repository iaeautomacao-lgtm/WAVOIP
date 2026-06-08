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
from ddm import processar_debito


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
    token      = wavoip_login()
    devices    = wavoip_get_devices(token)
    device_map = {d.get("token"): d for d in devices}

    healthy = []
    for t in DEVICE_PRIORITY:
        if t not in device_map:
            continue
        d = device_map[t]
        if d.get("status") != "open":
            continue
        if d.get("disabled") != 0:
            continue
        if d.get("phone") is None:
            continue
        if not WAVOIP_VAPI_MAP.get(t):  # pula se não tem phoneNumberId mapeado
            continue
        healthy.append(d)

    return healthy

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
@app.route("/api/monitor", methods=["GET"])
def monitor():
    try:
        # Busca campanhas em andamento
        camps = supabase.table("campaigns")\
            .select("id, name")\
            .eq("status", "em_andamento")\
            .execute().data

        if not camps:
            return jsonify({"ok": True, "data": [], "stats": {"em_andamento": 0, "atendido": 0, "erro": 0}})

        camp_ids  = [c["id"] for c in camps]
        camp_map  = {c["id"]: c["name"] for c in camps}

        # Busca chamadas ativas
        result = supabase.table("campaign_calls")\
            .select("*")\
            .in_("campaign_id", camp_ids)\
            .in_("status", ["em_andamento", "atendido", "enfileirado", "erro"])\
            .order("created_at", desc=True)\
            .limit(100)\
            .execute()

        rows = []
        for r in result.data:
            rows.append({
                "id":          r["id"],
                "cpf":         r["cpf"],
                "phone":       r["phone"],
                "status":      r["status"],
                "campaign":    camp_map.get(r["campaign_id"], "—"),
                "created_at":  r["created_at"],
            })

        stats = {
            "em_andamento": sum(1 for r in rows if r["status"] in ("em_andamento", "enfileirado")),
            "atendido":     sum(1 for r in rows if r["status"] == "atendido"),
            "erro":         sum(1 for r in rows if r["status"] == "erro"),
        }

        return jsonify({"ok": True, "data": rows, "stats": stats})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    
@app.route("/api/campaign/start", methods=["POST"])
def campaign_start():
    try:
        body        = request.json
        contacts    = body.get("contacts", [])
        campaign_id = body.get("campaign_id") or str(int(time.time()))

        if not contacts:
            return jsonify({"ok": False, "error": "Nenhum contato"}), 400

        # Insere todos como "pendente" com order_idx
        rows = []
        for i, c in enumerate(contacts):
            if not c.get("phone"):
                continue
            rows.append({
                "campaign_id": campaign_id,
                "cpf":         c.get("cpf", ""),
                "phone":       c.get("phone", "").strip(),
                "status":      "pendente",
                "order_idx":   i,
            })

        if not rows:
            return jsonify({"ok": False, "error": "Nenhum contato com telefone"}), 400

        result = supabase.table("campaign_calls").insert(rows).execute()
        inserted = result.data  # lista com ids gerados

        # Descobre quantas linhas saudáveis tem agora
        try:
            healthy = get_healthy_lines()
            window  = max(1, len(healthy))
        except Exception:
            window = 1

        # Enfileira os primeiros N (janela)
        fired = 0
        for row in inserted[:window]:
            supabase.table("campaign_calls").update({"status": "enfileirado"})\
                .eq("id", row["id"]).execute()
            make_call_task.delay(campaign_id, {
                "row_id":      row["id"],
                "cpf":         row["cpf"],
                "phone":       row["phone"],
                "name":        row.get("name", ""),
                "debito_data": row.get("debito_data"),
        })

        return jsonify({
            "ok":          True,
            "campaign_id": campaign_id,
            "total":       len(rows),
            "fired":       fired,
            "window":      window,
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/webhook/vapi", methods=["POST"])
@app.route("/api/webhook/vapi", methods=["POST"])
def vapi_webhook():
    try:
        body     = request.json
        call_id  = body.get("message", {}).get("call", {}).get("id") or body.get("call", {}).get("id") or body.get("id")
        msg_type = body.get("message", {}).get("type") or body.get("type")
        # Detecta quando atende
        
        if msg_type == "status-update":
            status_call = body.get("message", {}).get("status")
            call_id_log = body.get("message", {}).get("call", {}).get("id")
            if status_call == "in-progress" and call_id_log:
                print(f"ATENDEU call_id={call_id_log}", flush=True)
                try:
                    supabase.table("campaign_calls").update({"status": "atendido"})\
                        .eq("vapi_call_id", call_id_log).execute()
                except Exception:
                    pass
            return jsonify({"ok": True}), 200

        if msg_type not in ("end-of-call-report", "call-ended"):
            return jsonify({"ok": True}), 200

        if not call_id:
            return jsonify({"ok": True}), 200

        res = supabase.table("campaign_calls")\
            .select("*").eq("vapi_call_id", call_id).execute()

        if not res.data:
            return jsonify({"ok": True}), 200

        row          = res.data[0]
        campaign_id  = row["campaign_id"]
        ended_reason = body.get("message", {}).get("endedReason") or body.get("endedReason", "")
        duration     = body.get("message", {}).get("durationSeconds") or body.get("durationSeconds", 0)

        # Atualiza chamada
        supabase.table("campaign_calls").update({
            "status":   "finalizado",
            "error":    ended_reason,
            "duration": duration,
        }).eq("id", row["id"]).execute()

        # Atualiza contadores da campanha
        camp = supabase.table("campaigns").select("*").eq("id", campaign_id).execute().data
        if camp:
            camp = camp[0]
            if camp["status"] not in ("pausada", "finalizada"):
                finished = (camp.get("finished") or 0) + 1
                supabase.table("campaigns").update({
                    "finished":   finished,
                    "updated_at": "now()",
                }).eq("id", campaign_id).execute()

                # Verifica se concluiu tudo
                if finished >= (camp.get("total") or 0):
                    supabase.table("campaigns").update({
                        "status": "finalizada", "updated_at": "now()"
                    }).eq("id", campaign_id).execute()
                else:
                    # Busca próximo pendente
                    next_res = supabase.table("campaign_calls")\
                        .select("*")\
                        .eq("campaign_id", campaign_id)\
                        .eq("status", "pendente")\
                        .order("order_idx")\
                        .limit(1).execute()

                    if next_res.data:
                        next_row = next_res.data[0]
                        supabase.table("campaign_calls").update({"status": "enfileirado"})\
                            .eq("id", next_row["id"]).execute()
                        make_call_task.delay(campaign_id, {
                            "row_id":      next_row["id"],
                            "cpf":         next_row["cpf"],
                            "phone":       next_row["phone"],
                            "name":        next_row.get("name", ""),
                            "debito_data": next_row.get("debito_data"),
})
                    else:
                        supabase.table("campaigns").update({
                            "status": "finalizada", "updated_at": "now()"
                        }).eq("id", campaign_id).execute()

        return jsonify({"ok": True}), 200
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
@app.route("/api/campaigns", methods=["GET"])
def list_campaigns():
    try:
        result = supabase.table("campaigns")\
            .select("*")\
            .order("created_at", desc=True)\
            .execute()
        return jsonify({"ok": True, "data": result.data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns", methods=["POST"])
def create_campaign():
    try:
        body    = request.json
        name    = body.get("name", "").strip()
        
        contacts = body.get("contacts", [])

        if not name:
            return jsonify({"ok": False, "error": "Nome obrigatório"}), 400
        if not contacts:
            return jsonify({"ok": False, "error": "Nenhum contato"}), 400

        valid = [c for c in contacts if c.get("phone")]
        if not valid:
            return jsonify({"ok": False, "error": "Nenhum contato com telefone"}), 400

        # Cria campanha
        camp = supabase.table("campaigns").insert({
            "name":   name,
            "status": "rascunho",
            "total":  len(valid),
        }).execute().data[0]

        campaign_id = camp["id"]

        # Insere contatos como pendente
        rows = []
        for i, c in enumerate(valid):
            rows.append({
                "campaign_id": campaign_id,
                "cpf":         c.get("cpf", ""),
                "phone":       c.get("phone", "").strip(),
                "name":        c.get("name", ""),   # ← adiciona isso
                "status":      "pendente",
            "order_idx":   i,
})

        supabase.table("campaign_calls").insert(rows).execute()

        return jsonify({"ok": True, "campaign": camp})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns/<campaign_id>/start", methods=["POST"])
def start_campaign(campaign_id):
    try:
        print(f"START CAMPAIGN: {campaign_id}", flush=True)  # log temporário
        camp = supabase.table("campaigns")\
            .select("*").eq("id", campaign_id).execute().data
        if not camp:
            return jsonify({"ok": False, "error": "Campanha não encontrada"}), 404
        camp = camp[0]

        if camp["status"] == "em_andamento":
            return jsonify({"ok": False, "error": "Campanha já em andamento"}), 400

       
        supabase.table("campaigns").update({
            "status":     "em_andamento",
            "updated_at": "now()",
        }).eq("id", campaign_id).execute()

     
        pending = supabase.table("campaign_calls")\
            .select("*")\
            .eq("campaign_id", campaign_id)\
            .eq("status", "pendente")\
            .order("order_idx")\
            .execute().data

        if not pending:
            supabase.table("campaigns").update({"status": "finalizada"}).eq("id", campaign_id).execute()
            return jsonify({"ok": True, "fired": 0, "message": "Nenhum pendente"})

        # Janela dinâmica
        try:
            healthy = get_healthy_lines()
            window  = max(1, len(healthy))
        except Exception:
            window = 1

        fired = 0
        for row in pending[:window]:
            supabase.table("campaign_calls").update({"status": "enfileirado"})\
                .eq("id", row["id"]).execute()
            make_call_task.delay(campaign_id, {
                "row_id": row["id"],
                "cpf":    row["cpf"],
                "phone":  row["phone"],
            })
            fired += 1

        supabase.table("campaigns").update({"fired": fired}).eq("id", campaign_id).execute()

        return jsonify({"ok": True, "fired": fired, "window": window, "total": len(pending)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns/<campaign_id>/pause", methods=["POST"])
def pause_campaign(campaign_id):
    try:
        supabase.table("campaigns").update({
            "status": "pausada", "updated_at": "now()"
        }).eq("id", campaign_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns/<campaign_id>", methods=["DELETE"])
def delete_campaign(campaign_id):
    try:
        supabase.table("campaign_calls").delete().eq("campaign_id", campaign_id).execute()
        supabase.table("campaigns").delete().eq("id", campaign_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/contacts/<contact_id>", methods=["DELETE"])
def delete_contact(contact_id):
    try:
        supabase.table("contacts").delete().eq("id", contact_id).execute()
        return jsonify({"ok": True})
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

        result = supabase.table("campaign_calls").select(
            "*, campaigns(name)", count="exact"
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

from ddm import processar_debito
from tasks import process_import

@app.route("/api/import/upload", methods=["POST"])
def import_upload():
    try:
        if "file" not in request.files:
            return jsonify({"ok": False, "error": "Nenhum arquivo enviado"}), 400

        file  = request.files["file"]
        fname = file.filename.lower()

        if fname.endswith(".csv"):
            df = pd.read_csv(file, dtype=str, sep=None, engine="python")
        elif fname.endswith(".xlsx") or fname.endswith(".xls"):
            df = pd.read_excel(file, dtype=str)
        else:
            return jsonify({"ok": False, "error": "Formato inválido. Use .csv ou .xlsx"}), 400

        df.columns = [c.strip().lower() for c in df.columns]

        # Detecta colunas
        cpf_col  = next((c for c in df.columns if "cpf" in c), None)
        nome_col = next((c for c in df.columns if "nome" in c or "name" in c), None)
        tel_col  = next((c for c in df.columns if c in ("fone1", "tel", "telefone", "celular", "phone")), None)

        # Detecta se é formato DDM
        val_col  = next((c for c in df.columns if "val_atualizado" in c or "val_nominal" in c), None)
        is_ddm   = val_col is not None

        if not cpf_col:
            return jsonify({"ok": False, "error": "Coluna CPF não encontrada"}), 400
        if not nome_col:
            return jsonify({"ok": False, "error": "Coluna Nome não encontrada"}), 400

        # Monta lista de rows
        raw_rows = []
        for _, row in df.iterrows():
            cpf_raw  = str(row.get(cpf_col, "") or "").strip()
            cpf_norm = normalize_cpf(cpf_raw)
            if not cpf_norm:
                continue

            # Telefone — tenta múltiplas colunas
            phone = ""
            for col in ["fone1", "tel", "telefone", "celular", "phone"]:
                if col in df.columns:
                    val = str(row.get(col, "") or "").strip()
                    if val and val != "nan":
                        phone = val
                        break

            raw_rows.append({
                "cpf":   cpf_norm,
                "name":  str(row.get(nome_col, "") or "").strip(),
                "phone": phone,
            })

        if not raw_rows:
            return jsonify({"ok": False, "error": "Nenhum CPF válido encontrado"}), 400

        if is_ddm:
            # Formato DDM — processa na hora sem consultar API
            rows = []
            for i, row in enumerate(df.iterrows()):
                _, r = row
                cpf_raw  = str(r.get(cpf_col, "") or "").strip()
                cpf_norm = normalize_cpf(cpf_raw)
                if not cpf_norm:
                    continue

                phone = ""
                for col in ["fone1", "tel", "telefone", "celular", "phone"]:
                    if col in df.columns:
                        val = str(r.get(col, "") or "").strip()
                        if val and val != "nan":
                            phone = val
                            break

                val_avista = str(r.get(val_col, "0") or "0").strip().replace(",", ".")
                try:
                    tem_debito = float(val_avista) > 0
                except Exception:
                    tem_debito = False

                debito = None
                if tem_debito:
                    debito = {
                        "instituicao":    str(r.get("carteira", "") or "").strip(),
                        "nome_devedor":   str(r.get(nome_col, "") or "").strip(),
                        "numero_debitos": str(r.get("parcelas", "1") or "1").strip(),
                        "PgtoAvista": {
                            "ValorTotal":    str(r.get("val_nominal", "0,00") or "0,00").strip(),
                            "PercDesconto":  "0",
                            "ValorDesconto": "0,00",
                            "ValorFinal":    str(r.get(val_col, "0,00") or "0,00").strip(),
                        },
                        "CalculoBoleto": {
                            "SubtotalBoleto":    str(r.get(val_col, "0,00") or "0,00").strip(),
                            "HonorarioBoleto":   "0,00",
                            "ValorCobrarBoleto": "0,00",
                        },
                        "ParcelasBoleto": str(r.get("parcelas", "1") or "1").strip(),
                        "PgtoParceladoCartao": {
                            "Parcelas":     str(r.get("parcelas", "1") or "1").strip(),
                            "ValorParcela": "0,00",
                            "ValorFinal":   str(r.get(val_col, "0,00") or "0,00").strip(),
                        },
                    }

                rows.append({
                    "cpf":      cpf_norm,
                    "cpf_raw":  cpf_raw,
                    "name":     str(r.get(nome_col, "") or "").strip(),
                    "phone":    phone,
                    "has_debt": tem_debito,
                    "debito":   debito,
                })

            total     = len(rows)
            with_debt = sum(1 for r in rows if r["has_debt"])

            return jsonify({
                "ok":           True,
                "mode":         "ddm",
                "rows":         rows[:200],
                "total":        total,
                "with_debt":    with_debt,
                "without_debt": total - with_debt,
            })

        else:
            # Formato genérico — enfileira no Celery
            job = supabase.table("import_jobs").insert({
                "status": "processing",
                "total":  len(raw_rows),
            }).execute().data[0]

            process_import.delay(job["id"], raw_rows)

            return jsonify({
                "ok":     True,
                "mode":   "async",
                "job_id": job["id"],
                "total":  len(raw_rows),
            })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/import/status/<job_id>", methods=["GET"])
def import_status(job_id):
    try:
        job = supabase.table("import_jobs").select("*").eq("id", job_id).execute().data
        if not job:
            return jsonify({"ok": False, "error": "Job não encontrado"}), 404
        job = job[0]
        return jsonify({
            "ok":        True,
            "status":    job["status"],
            "total":     job["total"],
            "processed": job["processed"],
            "with_debt": job["with_debt"],
            "result":    job["result"] if job["status"] == "done" else None,
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