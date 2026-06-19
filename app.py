from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import requests
import os
import re
import time
import uuid
import json
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
from supabase import create_client, Client
from tasks import process_file, process_import_from_storage, formalizar_acordo, fill_campaign_capacity, fill_campaign_capacity_task, _get_import_rows

app = Flask(__name__)
CORS(app)

def env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip().strip('"').strip("'") if isinstance(value, str) else value


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


WAVOIP_EMAIL      = env("WAVOIP_EMAIL")
WAVOIP_PASSWORD   = env("WAVOIP_PASSWORD")
WAVOIP_BASE       = "https://api.wavoip.com"
VAPI_API_KEY      = env("VAPI_API_KEY")
VAPI_ASSISTANT_ID = env("VAPI_ASSISTANT_ID")
VAPI_BASE         = "https://api.vapi.ai"
SUPABASE_URL      = env("SUPABASE_URL")
SUPABASE_KEY      = env("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = env("SUPABASE_SERVICE_KEY", SUPABASE_KEY)
SUPABASE_BUCKET   = env("SUPABASE_BUCKET", "imports")
REDIS_URL         = env("REDIS_URL", "redis://localhost:6379/0")
LINE_MAX_CONCURRENT = int(env("LINE_MAX_CONCURRENT", env("SIP_MAX_CONCURRENT", "2")))

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
supabase_admin: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

WAVOIP_VAPI_MAP = {
    "88b232ad-5c1d-404f-8652-f5399e6a6f51": env("VAPI_PHONE_NUMBER_ID_1"),
    "ed49616d-fb19-46df-96b8-decab4cde3cf": env("VAPI_PHONE_NUMBER_ID_2"),
    "c8af8686-ca83-4eef-b757-f53940426011": env("VAPI_PHONE_NUMBER_ID_3"),
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43": env("VAPI_PHONE_NUMBER_ID_4"),
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb": env("VAPI_PHONE_NUMBER_ID_5"),
}

DEVICE_PRIORITY = [
    "88b232ad-5c1d-404f-8652-f5399e6a6f51",
    "ed49616d-fb19-46df-96b8-decab4cde3cf",
    "c8af8686-ca83-4eef-b757-f53940426011",
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43",
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb",
]

_round_robin_idx = 0
_wavoip_token    = None
_wavoip_token_ts = 0
TOKEN_TTL        = 600
_redis_client    = None


def _normalize_phone(phone) -> str:
    raw = str(phone or "").strip()
    if not raw or raw.lower() == "nan":
        return ""

    value = raw.replace(" ", "")
    if re.search(r"e[+-]?\d+$", value, re.IGNORECASE):
        try:
            value = str(int(Decimal(value.replace(",", "."))))
        except (InvalidOperation, ValueError):
            pass

    digits = re.sub(r"\D", "", value)
    if digits.endswith("0") and re.search(r"\.0+$", value):
        digits = digits[:-1]
    return digits


def _phone_e164(phone) -> str:
    digits = _normalize_phone(phone)
    if not digits:
        return ""
    if not digits.startswith("55"):
        digits = "55" + digits
    return "+" + digits


def redis_client():
    global _redis_client
    if _redis_client is None:
        import redis as redis_lib
        _redis_client = redis_lib.from_url(REDIS_URL)
    return _redis_client


def _import_state_key(job_id: str) -> str:
    return f"import_job:{job_id}"


def _get_import_state(job_id: str) -> dict:
    try:
        raw = redis_client().get(_import_state_key(job_id))
        if not raw:
            return {}
        return json.loads(raw)
    except Exception:
        return {}


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
    overrides  = _line_overrides_map()
    healthy    = []
    for t in DEVICE_PRIORITY:
        if t not in device_map:          continue
        if (overrides.get(t) or {}).get("paused"): continue
        d = device_map[t]
        if d.get("status") != "open":   continue
        if d.get("disabled") != 0:      continue
        if d.get("phone") is None:      continue
        if not WAVOIP_VAPI_MAP.get(t):  continue
        healthy.append(d)
    return healthy


def _known_line_tokens() -> set:
    return set(DEVICE_PRIORITY)


def _clean_line_tokens(tokens) -> list:
    if isinstance(tokens, str):
        tokens = [x.strip() for x in tokens.split(",") if x.strip()]
    if not isinstance(tokens, list):
        return []
    known = _known_line_tokens()
    cleaned = []
    for token in tokens:
        token = str(token).strip()
        if token and token in known and token not in cleaned:
            cleaned.append(token)
    return cleaned


def _line_meta_from_debito(row: dict) -> dict:
    debito = row.get("debito_data") or {}
    return debito.get("_dialer") if isinstance(debito, dict) else {}


def _active_line_counts(line_tokens: list) -> dict:
    counts = {token: 0 for token in line_tokens}
    if not line_tokens:
        return counts
    try:
        rows = supabase.table("campaign_calls")\
            .select("id, status, line_token, debito_data")\
            .in_("status", ["enfileirado", "em_andamento", "atendido"])\
            .execute().data or []
    except Exception:
        rows = supabase.table("campaign_calls")\
            .select("id, status, debito_data")\
            .in_("status", ["enfileirado", "em_andamento", "atendido"])\
            .execute().data or []

    for row in rows:
        meta = _line_meta_from_debito(row)
        token = row.get("line_token") or (meta or {}).get("line_token")
        if token in counts:
            counts[token] += 1
    return counts


def _line_overrides_map() -> dict:
    try:
        rows = supabase.table("line_overrides").select("*").execute().data or []
        return {r.get("line_token"): r for r in rows if r.get("line_token")}
    except Exception:
        return {}


def _is_line_paused(token: str) -> bool:
    return bool((_line_overrides_map().get(token) or {}).get("paused"))


def _line_name_map() -> dict:
    names = {}
    try:
        token = wavoip_login()
        devices = wavoip_get_devices(token)
        names.update({d.get("token"): (d.get("name") or d.get("phone") or d.get("token")) for d in devices})
    except Exception:
        pass
    for idx, token in enumerate(DEVICE_PRIORITY):
        names.setdefault(token, f"Linha {idx + 1}")
    return names


def _group_map() -> dict:
    try:
        rows = supabase.table("sip_groups").select("*").execute().data or []
        return {r.get("id"): r for r in rows if r.get("id")}
    except Exception:
        return {}


def vapi_call(phone: str) -> dict:
    global _round_robin_idx
    phone_e164 = _phone_e164(phone)
    digits = re.sub(r"\D", "", phone_e164)
    if len(digits) < 12:
        raise Exception(f"telefone invalido: {phone}")

    try:
        healthy = get_healthy_lines()
    except Exception as e:
        raise Exception(f"Erro ao consultar linhas Wavoip: {str(e)}")

    if not healthy:
        raise Exception("Nenhuma linha SIP disponível no momento")

    start_idx        = _round_robin_idx % len(healthy)
    _round_robin_idx += 1
    last_error       = None

    for i in range(len(healthy)):
        line     = healthy[(start_idx + i) % len(healthy)]
        phone_id = WAVOIP_VAPI_MAP.get(line.get("token"))
        if not phone_id:
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
                return res.json()
            try:    detail = res.json()
            except: detail = res.text
            last_error = f"Linha '{line.get('name')}' — Vapi {res.status_code}: {detail}"
        except Exception as ex:
            last_error = f"Linha '{line.get('name')}' — erro: {str(ex)}"
            continue

    raise Exception(f"Todas as linhas falharam. Último erro: {last_error}")


def _detectar_acordo_formalizado(transcript: str) -> bool:
    if not transcript:
        return False
    t = transcript.lower()
    frases = [
        "acordo formalizado",
        "negociação concluída",
        "negociacao concluida",
        "acordo fechado",
        "formalizado com sucesso",
        "vou formalizar agora",
        # frases reais da júlia
        "vou enviar o boleto",
        "enviando o boleto",
        "boleto para o seu email",
        "boleto no seu email",
        "confirmo o acordo",
        "confirmando o acordo",
        "acordo confirmado",
        "pagamento confirmado",
        "negociação realizada",
        "negociacao realizada",
        "regularizar sua situação",
        "quitar a vista",
        "fechamos o acordo",
        "combinado então",
        "combinado entao",
        "trato feito",
    ]
    return any(f in t for f in frases)


def _extrair_forma_pagamento(transcript: str) -> str:
    if not transcript:
        return "À vista"
    t = transcript.lower()
    if "boleto" in t:
        return "Boleto"
    if "cartão" in t or "cartao" in t:
        return "Cartão"
    return "À vista"


def _extrair_valor(summary: str) -> str:
    if not summary:
        return ""
    match = re.search(r'R\$\s*([\d.,]+)', summary)
    return match.group(1) if match else ""


# ── ROTAS ─────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/lines", methods=["GET"])
def get_lines():
    try:
        token   = wavoip_login()
        devices = wavoip_get_devices(token)
        device_map = {d.get("token"): d for d in devices}
        active_counts = _active_line_counts(DEVICE_PRIORITY)
        overrides = _line_overrides_map()
        lines   = [{
            "id":              (device_map.get(t) or {}).get("id"),
            "name":            (device_map.get(t) or {}).get("name") or f"Linha {idx + 1}",
            "phone":           (device_map.get(t) or {}).get("phone"),
            "status":          (device_map.get(t) or {}).get("status") or "missing",
            "disabled":        (device_map.get(t) or {}).get("disabled"),
            "calls_made":      (device_map.get(t) or {}).get("calls_made"),
            "needs_restart":   (device_map.get(t) or {}).get("needs_restart"),
            "token":           t,
            "phone_number_id": WAVOIP_VAPI_MAP.get(t) or "",
            "configured":      bool(WAVOIP_VAPI_MAP.get(t)),
            "max_concurrent":  LINE_MAX_CONCURRENT,
            "active_calls":    active_counts.get(t, 0),
            "free_slots":      max(0, LINE_MAX_CONCURRENT - active_counts.get(t, 0)),
            "paused":          bool((overrides.get(t) or {}).get("paused")),
            "pause_reason":    (overrides.get(t) or {}).get("reason", ""),
            "healthy":         t in device_map
                               and (device_map[t].get("status") == "open")
                               and device_map[t].get("disabled") == 0
                               and device_map[t].get("phone") is not None
                               and bool(WAVOIP_VAPI_MAP.get(t))
                               and not bool((overrides.get(t) or {}).get("paused")),
        } for idx, t in enumerate(DEVICE_PRIORITY)]
        return jsonify({"ok": True, "data": lines})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/lines/<line_token>/pause", methods=["POST"])
def pause_line(line_token):
    try:
        if line_token not in _known_line_tokens():
            return jsonify({"ok": False, "error": "Linha desconhecida"}), 404
        body = request.json or {}
        payload = {
            "line_token": line_token,
            "paused": True,
            "reason": (body.get("reason") or "pausada pelo dashboard").strip(),
            "updated_at": now_iso(),
        }
        supabase.table("line_overrides").upsert(payload, on_conflict="line_token").execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/lines/<line_token>/resume", methods=["POST"])
def resume_line(line_token):
    try:
        if line_token not in _known_line_tokens():
            return jsonify({"ok": False, "error": "Linha desconhecida"}), 404
        payload = {
            "line_token": line_token,
            "paused": False,
            "reason": "",
            "updated_at": now_iso(),
        }
        supabase.table("line_overrides").upsert(payload, on_conflict="line_token").execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/sip-groups", methods=["GET"])
def list_sip_groups():
    try:
        rows = supabase.table("sip_groups").select("*").order("name").execute().data or []
        return jsonify({"ok": True, "data": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "data": []}), 200


@app.route("/api/sip-groups", methods=["POST"])
def create_sip_group():
    try:
        body = request.json or {}
        name = (body.get("name") or "").strip()
        line_tokens = _clean_line_tokens(body.get("line_tokens", []))
        if not name:
            return jsonify({"ok": False, "error": "Nome do grupo obrigatorio"}), 400
        if not line_tokens:
            return jsonify({"ok": False, "error": "Selecione ao menos uma SIP"}), 400
        row = supabase.table("sip_groups").insert({
            "name": name,
            "line_tokens": line_tokens,
        }).execute().data[0]
        return jsonify({"ok": True, "group": row})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/sip-groups/<group_id>", methods=["PUT"])
def update_sip_group(group_id):
    try:
        body = request.json or {}
        name = (body.get("name") or "").strip()
        line_tokens = _clean_line_tokens(body.get("line_tokens", []))
        if not name:
            return jsonify({"ok": False, "error": "Nome do grupo obrigatorio"}), 400
        if not line_tokens:
            return jsonify({"ok": False, "error": "Selecione ao menos uma SIP"}), 400
        supabase.table("sip_groups").update({
            "name": name,
            "line_tokens": line_tokens,
            "updated_at": now_iso(),
        }).eq("id", group_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/sip-groups/<group_id>", methods=["DELETE"])
def delete_sip_group(group_id):
    try:
        supabase.table("sip_groups").delete().eq("id", group_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/contacts", methods=["GET"])
def get_contacts():
    try:
        page     = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))
        search   = request.args.get("search", "").strip()
        offset   = (page - 1) * per_page
        query    = supabase.table("contacts").select(
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


@app.route("/api/contacts/<contact_id>", methods=["DELETE"])
def delete_contact(contact_id):
    try:
        supabase.table("contacts").delete().eq("id", contact_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/calls", methods=["GET"])
def get_calls():
    try:
        page     = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))
        offset   = (page - 1) * per_page
        result   = supabase.table("campaign_calls").select(
            "id, cpf, status, created_at, duration, campaign_id", count="exact"
        ).order("created_at", desc=True).range(offset, offset + per_page - 1).execute()
        rows = []
        camp_ids = set()
        for row in (result.data or []):
            r = {
                "id": row.get("id"),
                "cpf": row.get("cpf"),
                "status": row.get("status"),
                "duration": row.get("duration") or row.get("call_duration"),
                "created_at": row.get("created_at"),
                "campaign_id": row.get("campaign_id"),
            }
            rows.append(r)
            cid = r.get("campaign_id")
            if cid:
                camp_ids.add(cid)
        name_map = {}
        if camp_ids:
            try:
                camps = supabase.table("campaigns").select("id, name").in_("id", list(camp_ids)).execute().data or []
                for c in camps:
                    name_map[str(c.get("id"))] = c.get("name")
            except Exception:
                pass
        for r in rows:
            r["campaign_name"] = name_map.get(str(r.get("campaign_id") or "")) or None
        return jsonify({
            "ok": True,
            "data": rows,
            "total": result.count,
            "page": page,
            "pages": -(-result.count // per_page) if result.count else 1,
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


@app.route("/api/monitor", methods=["GET"])
def monitor():
    try:
        camps = supabase.table("campaigns")\
            .select("id, name").eq("status", "em_andamento").execute().data
        if not camps:
            return jsonify({"ok": True, "data": [], "stats": {"em_andamento": 0, "atendido": 0, "erro": 0}})

        camp_ids = [c["id"] for c in camps]
        camp_map = {c["id"]: c["name"] for c in camps}

        result = supabase.table("campaign_calls")\
            .select("*")\
            .in_("campaign_id", camp_ids)\
            .in_("status", ["em_andamento", "atendido", "enfileirado", "erro"])\
            .order("created_at", desc=True)\
            .limit(100).execute()

        rows = []
        for r in result.data:
            meta = _line_meta_from_debito(r)
            rows.append({
                "id":              r["id"],
                "cpf":             r["cpf"],
                "phone":           r["phone"],
                "status":          r["status"],
                "campaign":        camp_map.get(r["campaign_id"], "—"),
                "created_at":      r["created_at"],
                "line_token":      r.get("line_token") or (meta or {}).get("line_token", ""),
                "line_name":       r.get("line_name") or (meta or {}).get("line_name", ""),
                "phone_number_id": r.get("phone_number_id") or (meta or {}).get("phone_number_id", ""),
            })

        stats = {
            "em_andamento": sum(1 for r in rows if r["status"] in ("em_andamento", "enfileirado")),
            "atendido":     sum(1 for r in rows if r["status"] == "atendido"),
            "erro":         sum(1 for r in rows if r["status"] == "erro"),
        }
        return jsonify({"ok": True, "data": rows, "stats": stats})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns", methods=["GET"])
def list_campaigns():
    try:
        result = supabase.table("campaigns")\
            .select("*").order("created_at", desc=True).execute()
        return jsonify({"ok": True, "data": result.data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/dashboard/summary", methods=["GET"])
def dashboard_summary():
    try:
        campaigns = supabase.table("campaigns")\
            .select("*").order("created_at", desc=True).limit(100).execute().data or []
        calls = supabase.table("campaign_calls")\
            .select("*").order("created_at", desc=True).limit(5000).execute().data or []

        try:
            accords = supabase.table("acordos_formalizados")\
                .select("*").order("created_at", desc=True).limit(5000).execute().data or []
        except Exception:
            accords = []

        line_names = _line_name_map()
        groups = _group_map()
        groups_by_token = {}
        for group in groups.values():
            for token in group.get("line_tokens") or []:
                groups_by_token.setdefault(token, []).append(group.get("name"))

        formalized_call_ids = {a.get("campaign_call_id") for a in accords if a.get("campaign_call_id")}
        email_sent = sum(1 for a in accords if a.get("email_enviado"))
        campaign_map = {c["id"]: c for c in campaigns}

        totals = {
            "campaigns": len(campaigns),
            "campaigns_active": sum(1 for c in campaigns if c.get("status") == "em_andamento"),
            "campaigns_finished": sum(1 for c in campaigns if c.get("status") == "finalizada"),
            "campaigns_paused": sum(1 for c in campaigns if c.get("status") == "pausada"),
            "calls": len(calls),
            "pending": sum(1 for r in calls if r.get("status") == "pendente"),
            "active": sum(1 for r in calls if r.get("status") in ("enfileirado", "em_andamento", "atendido")),
            "answered": sum(1 for r in calls if r.get("answered") or r.get("status") == "atendido"),
            "finished": sum(1 for r in calls if r.get("status") == "finalizado"),
            "errors": sum(1 for r in calls if r.get("status") in ("erro", "falha_sem_linha", "sem_telefone")),
            "formalized": len(accords),
            "email_sent": email_sent,
        }

        by_campaign = []
        calls_by_campaign = {}
        for row in calls:
            calls_by_campaign.setdefault(row.get("campaign_id"), []).append(row)

        for camp in campaigns:
            rows = calls_by_campaign.get(camp.get("id"), [])
            camp_formalized = sum(1 for r in rows if r.get("id") in formalized_call_ids)
            by_campaign.append({
                "id": camp.get("id"),
                "name": camp.get("name"),
                "status": camp.get("status"),
                "line_tokens": camp.get("line_tokens") or [],
                "sip_group_id": camp.get("sip_group_id") or "",
                "group": (groups.get(camp.get("sip_group_id")) or {}).get("name", ""),
                "total": camp.get("total") or len(rows),
                "finished": camp.get("finished") or sum(1 for r in rows if r.get("status") == "finalizado"),
                "active": sum(1 for r in rows if r.get("status") in ("enfileirado", "em_andamento", "atendido")),
                "answered": sum(1 for r in rows if r.get("answered") or r.get("status") == "atendido"),
                "errors": sum(1 for r in rows if r.get("status") in ("erro", "falha_sem_linha", "sem_telefone")),
                "formalized": camp_formalized,
                "created_at": camp.get("created_at"),
            })

        by_group_map = {}
        for row in by_campaign:
            group_id = row.get("sip_group_id") or "sem-grupo"
            group_name = row.get("group") or "Sem grupo"
            item = by_group_map.setdefault(group_id, {
                "id": group_id,
                "name": group_name,
                "campaigns": 0,
                "total": 0,
                "active": 0,
                "answered": 0,
                "formalized": 0,
                "errors": 0,
            })
            item["campaigns"] += 1
            item["total"] += row.get("total") or 0
            item["active"] += row.get("active") or 0
            item["answered"] += row.get("answered") or 0
            item["formalized"] += row.get("formalized") or 0
            item["errors"] += row.get("errors") or 0

        by_line = []
        for token in DEVICE_PRIORITY:
            rows = []
            for row in calls:
                meta = _line_meta_from_debito(row)
                if (row.get("line_token") or (meta or {}).get("line_token")) == token:
                    rows.append(row)
            by_line.append({
                "token": token,
                "name": line_names.get(token, token[:6]),
                "groups": groups_by_token.get(token, []),
                "total": len(rows),
                "active": sum(1 for r in rows if r.get("status") in ("enfileirado", "em_andamento", "atendido")),
                "answered": sum(1 for r in rows if r.get("answered") or r.get("status") == "atendido"),
                "finished": sum(1 for r in rows if r.get("status") == "finalizado"),
                "errors": sum(1 for r in rows if r.get("status") in ("erro", "falha_sem_linha", "sem_telefone")),
                "formalized": sum(1 for r in rows if r.get("id") in formalized_call_ids),
            })

        return jsonify({
            "ok": True,
            "totals": totals,
            "campaigns": by_campaign,
            "groups_report": list(by_group_map.values()),
            "lines": by_line,
            "groups": list(groups.values()),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns", methods=["POST"])
def create_campaign():
    try:
        body       = request.json
        name       = body.get("name", "").strip()
        session_id = body.get("session_id")
        contacts   = body.get("contacts", [])
        line_tokens = _clean_line_tokens(body.get("line_tokens", []))
        sip_group_id = (body.get("sip_group_id") or "").strip()

        if not name:
            return jsonify({"ok": False, "error": "Nome obrigatório"}), 400

        if session_id:
            job = supabase.table("import_jobs")\
                .select("status, result, with_debt").eq("id", session_id).execute().data
            if not job:
                return jsonify({"ok": False, "error": "Sessão não encontrada"}), 404
            if job[0].get("status") != "done":
                return jsonify({"ok": False, "error": "Importacao ainda processando"}), 400
            result_rows = _get_import_rows(session_id)
            if not result_rows:
                result_rows = job[0].get("result") or []
            if isinstance(result_rows, dict):
                result_rows = result_rows.get("rows") or result_rows.get("sample") or []
            contacts = [r for r in result_rows if r.get("phone")]

        if not contacts:
            return jsonify({"ok": False, "error": "Nenhum contato com telefone"}), 400

        camp_payload = {
            "name":   name,
            "status": "rascunho",
            "total":  len(contacts),
            "line_tokens": line_tokens,
        }
        if sip_group_id:
            camp_payload["sip_group_id"] = sip_group_id
        try:
            camp = supabase.table("campaigns").insert(camp_payload).execute().data[0]
        except Exception:
            camp_payload.pop("line_tokens", None)
            camp_payload.pop("sip_group_id", None)
            camp = supabase.table("campaigns").insert(camp_payload).execute().data[0]

        campaign_id = camp["id"]

        rows = []
        for i, c in enumerate(contacts):
            debito = c.get("debito") if isinstance(c.get("debito"), dict) else {}
            meta = c.get("meta") if isinstance(c.get("meta"), dict) else {}
            if meta:
                debito = {**debito, **meta}
            rows.append({
                "campaign_id": campaign_id,
                "cpf":         c.get("cpf", ""),
                "phone":       c.get("phone", "").strip(),
                "name":        c.get("name", ""),
                "status":      "pendente",
                "order_idx":   i,
                "debito_data": debito,
            })

        for i in range(0, len(rows), 500):
            supabase.table("campaign_calls").insert(rows[i:i+500]).execute()

        return jsonify({"ok": True, "campaign": camp})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns/<campaign_id>/lines", methods=["PUT"])
def update_campaign_lines(campaign_id):
    try:
        body = request.json or {}
        line_tokens = _clean_line_tokens(body.get("line_tokens", []))
        sip_group_id = (body.get("sip_group_id") or "").strip()
        if not line_tokens:
            return jsonify({"ok": False, "error": "Selecione ao menos uma SIP"}), 400

        update = {
            "line_tokens": line_tokens,
            "updated_at": now_iso(),
        }
        if "sip_group_id" in body:
            update["sip_group_id"] = sip_group_id or None

        try:
            supabase.table("campaigns").update(update).eq("id", campaign_id).execute()
        except Exception:
            update.pop("line_tokens", None)
            update.pop("sip_group_id", None)
            if update:
                supabase.table("campaigns").update(update).eq("id", campaign_id).execute()

        camp = supabase.table("campaigns").select("status").eq("id", campaign_id).execute().data
        if camp and camp[0].get("status") == "em_andamento":
            fill_campaign_capacity_task.delay(campaign_id)

        return jsonify({"ok": True, "line_tokens": line_tokens, "sip_group_id": sip_group_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns/<campaign_id>/start", methods=["POST"])
def start_campaign(campaign_id):
    try:
        camp = supabase.table("campaigns")\
            .select("*").eq("id", campaign_id).execute().data
        if not camp:
            return jsonify({"ok": False, "error": "Campanha não encontrada"}), 404
        camp = camp[0]

        already_running = camp["status"] == "em_andamento"
        if not already_running:
            supabase.table("campaigns").update({
                "status": "em_andamento", "updated_at": "now()"
            }).eq("id", campaign_id).execute()

        pending_count = supabase.table("campaign_calls")\
            .select("id", count="exact")\
            .eq("campaign_id", campaign_id)\
            .eq("status", "pendente")\
            .execute()

        if not (pending_count.count or 0):
            supabase.table("campaigns").update({"status": "finalizada"}).eq("id", campaign_id).execute()
            return jsonify({"ok": True, "fired": 0, "message": "Nenhum pendente"})

        result = fill_campaign_capacity(campaign_id)
        if not result.get("ok"):
            return jsonify({"ok": False, "error": result.get("error", "erro ao iniciar campanha")}), 500
        if result.get("locked"):
            fill_campaign_capacity_task.apply_async(args=[campaign_id], countdown=2)

        return jsonify({
            "ok": True,
            "already_running": already_running,
            "locked": bool(result.get("locked")),
            "skipped": result.get("skipped", 0),
            "fired": result.get("fired", 0),
            "capacity": result.get("capacity", 0),
            "active": result.get("active", 0),
            "healthy_lines": result.get("healthy_lines", 0),
            "line_max_concurrent": result.get("line_max_concurrent", 2),
            "selected_lines": result.get("selected_lines", 0),
            "total": pending_count.count or 0,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns/<campaign_id>/pause", methods=["POST"])
def pause_campaign(campaign_id):
    try:
        supabase.table("campaigns").update({
            "status": "pausada", "updated_at": "now()"
        }).eq("id", campaign_id).execute()
        supabase.table("campaign_calls").update({"status": "pendente"})\
            .eq("campaign_id", campaign_id).eq("status", "enfileirado").execute()
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


@app.route("/api/webhook/vapi", methods=["POST"])
def vapi_webhook():
    try:
        body = request.json or {}

        # O Vapi envia tudo dentro de body["message"]
        msg      = body.get("message", {})
        msg_type = msg.get("type") or body.get("type", "")

        # Extrai call_id de todas as posições possíveis
        call_id = (
            msg.get("call", {}).get("id") or
            body.get("call", {}).get("id") or
            msg.get("callId") or
            body.get("callId") or
            body.get("id")
        )

        logging.warning(f"[WEBHOOK] msg_type={msg_type} call_id={call_id}")

        # ── status-update ─────────────────────────────────────────────
        if msg_type == "status-update":
            if msg.get("status") == "in-progress" and call_id:
                try:
                    supabase.table("campaign_calls").update({"status": "atendido", "answered": True})\
                        .eq("vapi_call_id", call_id).execute()
                except Exception:
                    try:
                        supabase.table("campaign_calls").update({"status": "atendido"})\
                            .eq("vapi_call_id", call_id).execute()
                    except Exception:
                        pass
            return jsonify({"ok": True}), 200

        # ── ignora eventos que não são fim de chamada ─────────────────
        if msg_type not in ("end-of-call-report", "call-ended"):
            return jsonify({"ok": True}), 200

        if not call_id:
            logging.warning("[WEBHOOK] end-of-call sem call_id — ignorando")
            return jsonify({"ok": True}), 200

        # ── busca o campaign_call pelo vapi_call_id ───────────────────
        res = supabase.table("campaign_calls")\
            .select("*").eq("vapi_call_id", call_id).execute()
        if not res.data:
            logging.warning(f"[WEBHOOK] call_id {call_id} não encontrado em campaign_calls")
            return jsonify({"ok": True}), 200

        row         = res.data[0]
        campaign_id = row["campaign_id"]
        debito      = row.get("debito_data") or {}

        ended_reason = msg.get("endedReason") or body.get("endedReason", "")
        duration = int(float(msg.get("durationSeconds") or body.get("durationSeconds") or 0))

        # Extrai transcrição — pode estar em artifact.transcript ou messages
        transcript = (
            msg.get("artifact", {}).get("transcript") or
            body.get("artifact", {}).get("transcript") or
            ""
        )
        # Se não veio no artifact, reconstrói das messages
        if not transcript:
            messages = msg.get("artifact", {}).get("messages") or []
            lines = []
            for m in messages:
                role = "AI" if m.get("role") in ("assistant", "bot") else "User"
                txt  = m.get("message") or m.get("content") or ""
                if txt:
                    lines.append(f"{role}: {txt}")
            transcript = "\n".join(lines)

        summary = (
            msg.get("analysis", {}).get("summary") or
            body.get("analysis", {}).get("summary") or
            ""
        )

        logging.warning(f"[WEBHOOK] end-of-call call_id={call_id} transcript_len={len(transcript)} ended_reason={ended_reason}")

        supabase.table("campaign_calls").update({
            "status":   "finalizado",
            "error":    ended_reason,
            "duration": duration,
        }).eq("id", row["id"]).execute()

        # ── Detecta acordo formalizado e dispara task de email ────────
        if _detectar_acordo_formalizado(transcript):
            logging.warning(f"[WEBHOOK] ACORDO DETECTADO para call_id={call_id}")
            try:
                formalizar_acordo.delay({
                    "cpf":              row.get("cpf", ""),
                    "nome":             row.get("name", ""),
                    "email":            debito.get("email", ""),
                    "instituicao":      debito.get("instituicao", ""),
                    "idcalc":           debito.get("idcalc", ""),
                    "debito":           debito,
                    "valor":            _extrair_valor(summary),
                    "forma_pagamento":  _extrair_forma_pagamento(transcript),
                    "vapi_call_id":     call_id,
                    "campaign_call_id": row["id"],
                })
            except Exception as e:
                logging.warning(f"[WEBHOOK] erro ao enfileirar formalizar_acordo: {e}")

        # ── Avança fila da campanha ───────────────────────────────────
        camp = supabase.table("campaigns").select("*").eq("id", campaign_id).execute().data
        if camp:
            camp = camp[0]
            if camp["status"] not in ("pausada", "finalizada"):
                finished = (camp.get("finished") or 0) + 1
                try:
                    supabase.table("campaigns").update({
                        "finished": finished, "updated_at": "now()"
                    }).eq("id", campaign_id).execute()
                except Exception:
                    supabase.table("campaigns").update({
                        "updated_at": "now()"
                    }).eq("id", campaign_id).execute()

                if finished >= (camp.get("total") or 0):
                    supabase.table("campaigns").update({
                        "status": "finalizada", "updated_at": "now()"
                    }).eq("id", campaign_id).execute()
                else:
                    fill_campaign_capacity_task.delay(campaign_id)

        return jsonify({"ok": True}), 200
    except Exception as e:
        logging.warning(f"[WEBHOOK] ERRO: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ── IMPORT: legado Redis ───────────────────────────────────────

@app.route("/api/import/upload", methods=["POST"])
def import_upload():
    try:
        if "file" not in request.files:
            return jsonify({"ok": False, "error": "Nenhum arquivo enviado"}), 400

        file  = request.files["file"]
        fname = file.filename.lower()

        if not (fname.endswith(".csv") or fname.endswith(".xlsx") or fname.endswith(".xls")):
            return jsonify({"ok": False, "error": "Formato inválido. Use .csv ou .xlsx"}), 400

        file_bytes = file.read()
        file_id    = str(uuid.uuid4())

        r = redis_client()
        r.setex(f"import:{file_id}", 3600, file_bytes)

        job = supabase.table("import_jobs").insert({
            "status":    "processing",
            "total":     0,
            "with_debt": 0,
            "processed": 0,
        }).execute().data[0]

        process_file.apply_async(args=[job["id"], file_id, fname], queue="imports")

        return jsonify({"ok": True, "mode": "async", "job_id": job["id"]})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── IMPORT: Supabase Storage ───────────────────────────────────

@app.route("/api/upload/presigned", methods=["GET"])
def get_presigned_url():
    try:
        ext       = request.args.get("ext", "csv").lstrip(".")
        file_path = f"uploads/{uuid.uuid4()}.{ext}"

        res = supabase_admin.storage.from_(SUPABASE_BUCKET).create_signed_upload_url(file_path)

        if isinstance(res, dict):
            res_dict = res
        else:
            res_dict = vars(res) if hasattr(res, "__dict__") else {}

        upload_url = (
            res_dict.get("signed_url") or
            res_dict.get("signedUrl") or
            res_dict.get("signedURL", "")
        )
        if not upload_url:
            raise Exception(f"SDK não retornou URL de upload. Resposta: {res_dict}")

        return jsonify({
            "ok":         True,
            "upload_url": upload_url,
            "token":      res_dict.get("token", ""),
            "path":       file_path,
        })
    except Exception as e:
        logging.warning(
            "[UPLOAD_PRESIGNED] erro=%s url=%s bucket=%s key_len=%s service_key_len=%s service_starts_eyJ=%s",
            str(e),
            SUPABASE_URL,
            SUPABASE_BUCKET,
            len(SUPABASE_KEY or ""),
            len(SUPABASE_SERVICE_KEY or ""),
            str(SUPABASE_SERVICE_KEY or "").startswith("eyJ"),
        )
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/import/from-storage", methods=["POST"])
def import_from_storage():
    try:
        data         = request.json or {}
        storage_path = data.get("path")
        filename     = data.get("filename", "import.csv")
        size_bytes   = data.get("size_bytes", 0)

        if not storage_path:
            return jsonify({"ok": False, "error": "path obrigatório"}), 400

        fname = filename.lower()
        if not (fname.endswith(".csv") or fname.endswith(".xlsx") or fname.endswith(".xls")):
            return jsonify({"ok": False, "error": "Formato inválido. Use .csv ou .xlsx"}), 400

        job = supabase.table("import_jobs").insert({
            "status":    "queued",
            "total":     0,
            "with_debt": 0,
            "processed": 0,
            "result":    {"filename": filename, "storage_path": storage_path, "size_bytes": size_bytes},
        }).execute().data[0]

        process_import_from_storage.apply_async(args=[job["id"], storage_path, fname], queue="imports")

        return jsonify({"ok": True, "job_id": job["id"], "status": "queued"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/import/stop/<job_id>", methods=["POST"])
def import_stop(job_id):
    import tasks as tasks_mod
    try:
        celery_app = tasks_mod.celery
        celery_app.control.revoke(job_id, terminate=True, signal='SIGKILL')

        job = supabase.table("import_jobs").select("*").eq("id", job_id).execute()
        if job.data:
            supabase.table("import_jobs").update({
                "status": "stopped"
            }).eq("id", job_id).execute()
    except Exception as e:
        logging.warning(f"[IMPORT_STOP] erro=%s", e)
        supabase.table("import_jobs").update({"status": "stopped"}).eq("id", job_id).execute()
    return jsonify({"ok": True, "status": "stopped"})


@app.route("/api/import/status/<job_id>", methods=["GET"])
def import_status(job_id):
    try:
        job = supabase.table("import_jobs").select("*").eq("id", job_id).execute().data
        if not job:
            return jsonify({"ok": False, "error": "Job não encontrado"}), 404
        job = job[0]

        live_state = {}
        if job.get("status") not in ("done", "error", "stopped"):
            live_state = _get_import_state(job_id)
        if live_state:
            return jsonify({
                "ok":         True,
                "status":     live_state.get("status") or job["status"],
                "total":      live_state.get("total", job["total"]),
                "processed":  live_state.get("processed", job["processed"]),
                "with_debt":  live_state.get("with_debt", job["with_debt"]),
                "session_id": None,
                "result":     live_state.get("sample") or [],
            })

        result_preview = None
        if job["result"]:
            if isinstance(job["result"], list):
                result_preview = job["result"][:200]
            elif isinstance(job["result"], dict):
                result_preview = job["result"].get("sample") or (job["result"].get("rows") or [])[:200]

        return jsonify({
            "ok":         True,
            "status":     job["status"],
            "total":      job["total"],
            "processed":  job["processed"],
            "with_debt":  job["with_debt"],
            "session_id": job["id"] if job["status"] == "done" else None,
            "result":     result_preview,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
