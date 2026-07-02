from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import requests
import os
import re
import time
import uuid
import json
import logging
import hmac
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, date
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
from mysql_adapter import create_client, MySQLClient as Client
from tasks import process_file, process_import_from_storage, formalizar_acordo, fill_campaign_capacity, fill_campaign_capacity_task, _get_import_rows

app = Flask(__name__)

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
VAPI_ASSISTANT_ID_CRUZEIRO = env("VAPI_ASSISTANT_ID_CRUZEIRO", VAPI_ASSISTANT_ID)
VAPI_ASSISTANT_ID_DDM = env("VAPI_ASSISTANT_ID_DDM", VAPI_ASSISTANT_ID)
VAPI_BASE         = "https://api.vapi.ai"


def _get_assistant_id(debito: dict) -> str:
    if not isinstance(debito, dict):
        return VAPI_ASSISTANT_ID
    inst = str(debito.get("instituicao", "")).lower()
    if "cruzeiro" in inst:
        return VAPI_ASSISTANT_ID_CRUZEIRO
    # Se não for Cruzeiro, usa o do Veiga / DDM
    return VAPI_ASSISTANT_ID_DDM
REDIS_URL         = env("REDIS_URL", "redis://localhost:6379/0")
LINE_MAX_CONCURRENT = int(env("LINE_MAX_CONCURRENT", env("SIP_MAX_CONCURRENT", "2")))
LINE_COOLDOWN_SECONDS = int(env("LINE_COOLDOWN_SECONDS", "120"))
API_AUTH_TOKEN    = env("API_AUTH_TOKEN")
VAPI_WEBHOOK_SECRET = env("VAPI_WEBHOOK_SECRET")
CORS_ORIGINS      = [x.strip() for x in env("CORS_ORIGINS").split(",") if x.strip()]

if CORS_ORIGINS:
    CORS(app, resources={r"/api/*": {"origins": CORS_ORIGINS}})


def _bearer_token() -> str:
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    token = request.headers.get("X-API-Token", "").strip()
    if token:
        return token
    if request.path == "/api/stream":
        return request.args.get("token", "").strip()
    return ""


def _valid_secret(provided: str, expected: str) -> bool:
    return bool(provided and expected and hmac.compare_digest(provided, expected))


@app.before_request
def require_api_auth():
    if not API_AUTH_TOKEN:
        return None
    if not request.path.startswith("/api/"):
        return None
    if request.path == "/api/webhook/vapi":
        return None
    if _valid_secret(_bearer_token(), API_AUTH_TOKEN):
        return None
    return jsonify({"ok": False, "error": "unauthorized"}), 401

supabase: Client = create_client()
supabase_admin: Client = supabase

def run_migrations():
    try:
        conn = supabase.connect()
        with conn.cursor() as cur:
            cur.execute("SHOW COLUMNS FROM `campaign_calls` LIKE 'recording_url'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE `campaign_calls` ADD COLUMN `recording_url` VARCHAR(512) DEFAULT NULL")
                print("Migration: Added recording_url to campaign_calls")
            cur.execute("SHOW COLUMNS FROM `campaign_calls` LIKE 'transcript'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE `campaign_calls` ADD COLUMN `transcript` LONGTEXT DEFAULT NULL")
                print("Migration: Added transcript to campaign_calls")
            cur.execute("SHOW COLUMNS FROM `acordos_formalizados` LIKE 'deletado_painel'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE `acordos_formalizados` ADD COLUMN `deletado_painel` BOOLEAN NOT NULL DEFAULT FALSE")
                print("Migration: Added deletado_painel to acordos_formalizados")
    except Exception as e:
        print(f"Migration warning: {e}")

run_migrations()

WAVOIP_VAPI_MAP = {
    "ed49616d-fb19-46df-96b8-decab4cde3cf": env("VAPI_PHONE_NUMBER_ID_1"),
    "c8af8686-ca83-4eef-b757-f53940426011": env("VAPI_PHONE_NUMBER_ID_2"),
}

WAVOIP_ENV_MAP = {
    "ed49616d-fb19-46df-96b8-decab4cde3cf": "VAPI_PHONE_NUMBER_ID_1",
    "c8af8686-ca83-4eef-b757-f53940426011": "VAPI_PHONE_NUMBER_ID_2",
}

DEVICE_PRIORITY = [
    "ed49616d-fb19-46df-96b8-decab4cde3cf",
    "c8af8686-ca83-4eef-b757-f53940426011",
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


def _line_cooldown_key(line_token: str) -> str:
    return f"dialer:line_cooldown:{line_token}"


def _is_line_in_cooldown(line_token: str) -> bool:
    try:
        return bool(redis_client().exists(_line_cooldown_key(line_token)))
    except Exception:
        return False


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
    }, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }, timeout=10)
    res.raise_for_status()
    _wavoip_token    = res.json()["data"]["token"]
    _wavoip_token_ts = time.time()
    return _wavoip_token


def wavoip_get_devices(token: str) -> list:
    res = requests.get(f"{WAVOIP_BASE}/v2/devices/me",
        headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }, timeout=10)
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
        if _is_line_in_cooldown(t):      continue
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


def vapi_call(phone: str, name: str = "", cpf: str = "", debito: dict = None) -> dict:
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

        payload = {
            "phoneNumberId": phone_id,
            "assistantId":   _get_assistant_id(debito),
            "customer":      {"number": phone_e164, "name": name}
        }

        if debito:
            payload["assistantOverrides"] = {
                "variableValues": {
                    "instituicao":         debito.get("instituicao", ""),
                    "Valorcpf":            cpf,
                    "NominalPrinc":        debito.get("PgtoAvista", {}).get("ValorTotal", "0,00"),
                    "PgtoAvista":          debito.get("PgtoAvista", {}),
                    "CalculoBoleto":       debito.get("CalculoBoleto", {}),
                    "ParcelasBoleto":      debito.get("ParcelasBoleto", "0"),
                    "PgtoParceladoCartao": debito.get("PgtoParceladoCartao", {}),
                    "PrimeiroVencto":      debito.get("PrimeiroVencto", "em dois dias"),
                    "QuantidadeMensalidades": debito.get("numero_debitos", "1"),
                    "ValorFinalAVista":    debito.get("PgtoAvista", {}).get("ValorFinal", "0,00"),
                }
            }

        try:
            res = requests.post(f"{VAPI_BASE}/call/phone", json=payload, headers={
                "Authorization": f"Bearer {VAPI_API_KEY}",
                "Content-Type":  "application/json",
                "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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
    # Tenta achar R$ 1.234,56 ou BRL 1.234,56
    match = re.search(r'(?:R\$\s*|BRL\s*)([\d.,]+)', summary, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Tenta achar 1.234,56 BRL ou 1.234,56 reais
    match = re.search(r'([\d.,]+)\s*(?:BRL|R\$|reais)', summary, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Procura por qualquer número decimal formatado com vírgula ou ponto (e.g. 2.598,93)
    match = re.search(r'\b([\d]{1,3}(?:\.[\d]{3})*,[\d]{2}|[\d]{1,3}(?:\,[\d]{3})*\.[\d]{2})\b', summary)
    if match:
        return match.group(1).strip()
        
    return ""


# ── ROTAS ─────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/lines", methods=["GET"])
def get_lines():
    try:
        wavoip_error = None
        try:
            token   = wavoip_login()
            devices = wavoip_get_devices(token)
            device_map = {d.get("token"): d for d in devices}
        except Exception as api_err:
            logging.error(f"Erro ao obter dispositivos da Wavoip: {api_err}")
            device_map = {}
            wavoip_error = str(api_err)

        active_counts = _active_line_counts(DEVICE_PRIORITY)
        overrides = _line_overrides_map()
        cooldowns = {t: _is_line_in_cooldown(t) for t in DEVICE_PRIORITY}
        lines   = [{
            "id":              (device_map.get(t) or {}).get("id"),
            "name":            (device_map.get(t) or {}).get("name") or f"Linha {idx + 1}",
            "phone":           (device_map.get(t) or {}).get("phone"),
            "status":          "erro api" if wavoip_error else ((device_map.get(t) or {}).get("status") or "missing"),
            "disabled":        (device_map.get(t) or {}).get("disabled"),
            "calls_made":      (device_map.get(t) or {}).get("calls_made"),
            "needs_restart":   (device_map.get(t) or {}).get("needs_restart"),
            "token":           t,
            "phone_number_id": WAVOIP_VAPI_MAP.get(t) or "",
            "phone_number_short": ((WAVOIP_VAPI_MAP.get(t) or "")[:8] + "...") if WAVOIP_VAPI_MAP.get(t) else "",
            "env_name":        WAVOIP_ENV_MAP.get(t) or "",
            "configured":      bool(WAVOIP_VAPI_MAP.get(t)),
            "max_concurrent":  LINE_MAX_CONCURRENT,
            "active_calls":    active_counts.get(t, 0),
            "free_slots":      max(0, LINE_MAX_CONCURRENT - active_counts.get(t, 0)),
            "paused":          bool((overrides.get(t) or {}).get("paused")),
            "pause_reason":    (overrides.get(t) or {}).get("reason", ""),
            "cooldown":        bool(cooldowns.get(t)),
            "healthy":         not wavoip_error
                               and t in device_map
                               and (device_map[t].get("status") == "open")
                               and device_map[t].get("disabled") == 0
                               and device_map[t].get("phone") is not None
                               and bool(WAVOIP_VAPI_MAP.get(t))
                               and not bool(cooldowns.get(t))
                               and not bool((overrides.get(t) or {}).get("paused")),
        } for idx, t in enumerate(DEVICE_PRIORITY)]
        return jsonify({
            "ok": True, 
            "data": lines,
            "wavoip_error": wavoip_error
        })
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
            "id, cpf, status, created_at, duration, campaign_id, recording_url, transcript", count="exact"
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
                "recording_url": row.get("recording_url"),
                "transcript": row.get("transcript"),
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


@app.route("/api/acordos", methods=["GET"])
def get_acordos():
    try:
        page     = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))
        offset   = (page - 1) * per_page
        conn = supabase.connect()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM acordos_formalizados WHERE deletado_painel = FALSE OR deletado_painel IS NULL")
            total = (cur.fetchone() or {}).get("total", 0)

            sql = """
                SELECT a.*, c.recording_url, c.transcript
                FROM acordos_formalizados a
                LEFT JOIN campaign_calls c ON a.campaign_call_id = c.id
                WHERE a.deletado_painel = FALSE OR a.deletado_painel IS NULL
                ORDER BY a.created_at DESC
                LIMIT %s OFFSET %s
            """
            cur.execute(sql, [per_page, offset])
            rows = cur.fetchall()

            # Normalização de tipos
            for r in rows:
                for k, v in r.items():
                    if isinstance(v, (datetime, date)):
                        r[k] = v.isoformat()
                    elif isinstance(v, Decimal):
                        r[k] = float(v)

        return jsonify({
            "ok":    True,
            "data":  rows,
            "total": total,
            "page":  page,
            "pages": -(-total // per_page) if total else 1,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/acordos/<acordo_id>", methods=["DELETE"])
def delete_acordo(acordo_id):
    try:
        supabase.table("acordos_formalizados").update({"deletado_painel": True}).eq("id", acordo_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/acordos/<acordo_id>/resend-email", methods=["POST"])
def resend_acordo_email(acordo_id):
    try:
        acordo = supabase.table("acordos_formalizados").select("*").eq("id", acordo_id).execute().data
        if not acordo:
            return jsonify({"ok": False, "error": "Acordo não encontrado"}), 404
        acordo = acordo[0]

        from tasks import verificar_boleto_acordo
        dados = {
            "cpf":             acordo.get("cpf"),
            "nome":            acordo.get("nome"),
            "email":           acordo.get("email"),
            "instituicao":     acordo.get("instituicao"),
            "valor":           acordo.get("valor"),
            "forma_pagamento": acordo.get("forma_pagamento"),
            "vencimento":      acordo.get("vencimento"),
            "nr_acordo":       acordo.get("nr_acordo"),
            "idcalc":          acordo.get("vapi_call_id"), # fallback
            "vapi_call_id":    acordo.get("vapi_call_id"),
            "campaign_call_id": acordo.get("campaign_call_id"),
            "link_boleto":     acordo.get("link_boleto"),
            "link_pix":        acordo.get("link_pix"),
            "linha_dig":       acordo.get("linha_dig"),
        }

        verificar_boleto_acordo.delay(dados, tentativa=1)
        return jsonify({"ok": True, "message": "Reenvio de e-mail enfileirado com sucesso"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/acordos/formalizar", methods=["POST"])
def manual_formalizar_acordo():
    try:
        body = request.json or {}
        cpf = body.get("cpf")
        if not cpf:
            return jsonify({"ok": False, "error": "cpf obrigatório"}), 400
            
        dados = {
            "cpf": cpf,
            "nome": body.get("nome") or "Cliente",
            "email": body.get("email") or "",
            "instituicao": body.get("instituicao") or "",
            "valor": body.get("valor") or "",
            "forma_pagamento": body.get("forma_pagamento") or "À vista",
            "campaign_call_id": body.get("campaign_call_id"),
            "vapi_call_id": body.get("vapi_call_id") or "manual",
        }
        
        # Enfileira no Celery para processamento síncrono/assíncrono
        formalizar_acordo.delay(dados)
        return jsonify({"ok": True, "message": "Formalização de acordo enfileirada com sucesso"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/vapi/assistants", methods=["GET"])
def get_vapi_assistants():
    try:
        r = requests.get(f"{VAPI_BASE}/assistant", headers={
            "Authorization": f"Bearer {VAPI_API_KEY}"
        }, timeout=15)
        r.raise_for_status()
        assistants = []
        for a in r.json():
            assistants.append({
                "id": a.get("id"),
                "name": a.get("name") or "Sem nome"
            })
        return jsonify({"ok": True, "assistants": assistants})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/debug-update")
def debug_update():
    import time
    from mysql_adapter import create_client
    db = create_client()
    steps = []
    try:
        t0 = time.time()
        steps.append("Connecting & selecting...")
        res = db.table("acordos_formalizados").select("*").eq("nr_acordo", "16668562").execute().data
        steps.append(f"Select done in {time.time()-t0:.2f}s, found: {len(res)} rows")
        
        t0 = time.time()
        steps.append("Updating row...")
        db.table("acordos_formalizados").update({"deletado_painel": 0}).eq("nr_acordo", "16668562").execute()
        steps.append(f"Update done in {time.time()-t0:.2f}s")
        return jsonify({"ok": True, "steps": steps})
    except Exception as e:
        steps.append(f"Error: {e}")
        return jsonify({"ok": False, "error": str(e), "steps": steps}), 500


@app.route("/api/debug-import")
def debug_import():
    try:
        acordos = supabase.table("acordos_formalizados").select("*").order("created_at", desc=True).limit(5).execute().data
        calls = supabase.table("campaign_calls").select("*").order("updated_at", desc=True).limit(20).execute().data
        jobs = supabase.table("import_jobs").select("*").order("created_at", desc=True).limit(5).execute().data
        count_calls = len(supabase.table("campaign_calls").select("id").execute().data or [])
        return jsonify({
            "acordos": acordos,
            "calls": calls,
            "jobs": jobs,
            "count_calls": count_calls
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/check-calls-by-cpf/<cpf>")
def check_calls_by_cpf(cpf):
    try:
        from mysql_adapter import create_client
        db = create_client()
        res = db.table("campaign_calls").select("*").eq("cpf", cpf).execute()
        return jsonify({"ok": True, "data": res.data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/call", methods=["POST"])
def make_call():
    try:
        body  = request.json
        phone = body.get("phone")
        contact_id = body.get("contact_id")
        if not phone:
            return jsonify({"ok": False, "error": "phone obrigatório"}), 400

        name = ""
        cpf = ""
        debito = None

        if contact_id:
            try:
                contact = supabase.table("contacts").select("*").eq("id", contact_id).execute().data
                if contact:
                    name = contact[0].get("name", "")
                    cpf = contact[0].get("cpf", "")
                    # Busca ultimo debito daquele CPF registrado nas chamadas de campanha
                    calls = supabase.table("campaign_calls").select("debito_data").eq("cpf", cpf).order("updated_at", desc=True).limit(1).execute().data
                    if calls and calls[0].get("debito_data"):
                        debito = calls[0].get("debito_data")
            except Exception as e:
                logging.error(f"Erro ao obter dados do contato para chamada manual: {e}")

        vapi_data = vapi_call(phone, name=name, cpf=cpf, debito=debito)
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
            accords = [a for a in accords if not a.get("deletado_painel")]
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
        attempted_calls = sum(1 for r in calls if r.get("status") not in ("pendente", "enfileirado"))
        answered_calls = sum(1 for r in calls if r.get("answered") or r.get("status") == "atendido")
        alo_rate = round((answered_calls / attempted_calls * 100), 1) if attempted_calls > 0 else 0.0

        totals = {
            "campaigns": len(campaigns),
            "campaigns_active": sum(1 for c in campaigns if c.get("status") == "em_andamento"),
            "campaigns_finished": sum(1 for c in campaigns if c.get("status") == "finalizada"),
            "campaigns_paused": sum(1 for c in campaigns if c.get("status") == "pausada"),
            "calls": len(calls),
            "pending": sum(1 for r in calls if r.get("status") == "pendente"),
            "active": sum(1 for r in calls if r.get("status") in ("enfileirado", "em_andamento", "atendido")),
            "answered": answered_calls,
            "finished": sum(1 for r in calls if r.get("status") == "finalizado"),
            "errors": sum(1 for r in calls if r.get("status") in ("erro", "falha_sem_linha", "sem_telefone")),
            "formalized": len(accords),
            "email_sent": email_sent,
            "alo_rate": alo_rate,
        }

        by_campaign = []
        calls_by_campaign = {}
        for row in calls:
            calls_by_campaign.setdefault(row.get("campaign_id"), []).append(row)

        for camp in campaigns:
            rows = calls_by_campaign.get(camp.get("id"), [])
            camp_formalized = sum(1 for r in rows if r.get("id") in formalized_call_ids)
            camp_attempted = sum(1 for r in rows if r.get("status") not in ("pendente", "enfileirado"))
            camp_answered = sum(1 for r in rows if r.get("answered") or r.get("status") == "atendido")
            camp_alo_rate = round((camp_answered / camp_attempted * 100), 1) if camp_attempted > 0 else 0.0
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
                "answered": camp_answered,
                "errors": sum(1 for r in rows if r.get("status") in ("erro", "falha_sem_linha", "sem_telefone")),
                "formalized": camp_formalized,
                "alo_rate": camp_alo_rate,
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
        assistant_id = (body.get("assistant_id") or "").strip()

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
        if assistant_id:
            camp_payload["assistant_id"] = assistant_id
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
        if VAPI_WEBHOOK_SECRET:
            provided = (
                request.headers.get("X-Vapi-Secret", "").strip() or
                request.headers.get("X-Webhook-Secret", "").strip() or
                _bearer_token() or
                request.args.get("secret", "").strip()
            )
            if not _valid_secret(provided, VAPI_WEBHOOK_SECRET):
                return jsonify({"ok": False, "error": "unauthorized"}), 401

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

        # ── tool-calls ────────────────────────────────────────────────
        if msg_type == "tool-calls":
            tool_calls = msg.get("toolCalls") or body.get("toolCalls") or []
            results = []
            for tc in tool_calls:
                tc_id = tc.get("id")
                func  = tc.get("function") or {}
                name  = func.get("name")
                args  = func.get("arguments") or {}
                
                logging.warning(f"[WEBHOOK] tool-call name={name} args={args}")
                
                if name == "capturar_cpf":
                    cpf_prefixo3 = str(args.get("cpf_prefixo3") or "").strip()
                    cpf_esperado = str(args.get("cpf_esperado") or "").strip()
                    
                    # Limpa caracteres não numéricos
                    clean_prefix = re.sub(r"\D", "", cpf_prefixo3)
                    clean_expected = re.sub(r"\D", "", cpf_esperado)
                    expected_prefix3 = clean_expected[:3] if len(clean_expected) >= 3 else ""
                    
                    logging.warning(f"[WEBHOOK] capturar_cpf clean_prefix={clean_prefix} expected_prefix3={expected_prefix3}")
                    
                    if clean_prefix and expected_prefix3 and clean_prefix == expected_prefix3:
                        res_val = {"cpf_prefixo3": clean_prefix}
                    else:
                        res_val = {"cpf_prefixo3": "invalid"}
                        
                    results.append({
                        "toolCallId": tc_id,
                        "result": res_val
                    })
                else:
                    results.append({
                        "toolCallId": tc_id,
                        "result": {"ok": True}
                    })
            return jsonify({"results": results}), 200

        # ── status-update ─────────────────────────────────────────────
        if msg_type == "status-update":
            if msg.get("status") == "in-progress" and call_id:
                try:
                    supabase.table("campaign_calls").update({"status": "atendido", "answered": True})\
                        .eq("vapi_call_id", call_id).neq("status", "finalizado").execute()
                except Exception:
                    try:
                        supabase.table("campaign_calls").update({"status": "atendido"})\
                            .eq("vapi_call_id", call_id).neq("status", "finalizado").execute()
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

        if row.get("status") == "finalizado":
            logging.warning(f"[WEBHOOK] call_id={call_id} ja finalizado; ignorando duplicado")
            return jsonify({"ok": True, "duplicate": True}), 200

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

        recording_url = (
            msg.get("recordingUrl") or
            body.get("recordingUrl") or
            msg.get("artifact", {}).get("recordingUrl") or
            body.get("artifact", {}).get("recordingUrl") or
            ""
        )

        logging.warning(f"[WEBHOOK] end-of-call call_id={call_id} transcript_len={len(transcript)} ended_reason={ended_reason}")

        supabase.table("campaign_calls").update({
            "status":        "finalizado",
            "error":         ended_reason,
            "duration":      duration,
            "recording_url": recording_url,
            "transcript":    transcript,
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
                    "valor":            _extrair_valor(summary) or _extrair_valor(transcript),
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
                finished_res = supabase.table("campaign_calls")\
                    .select("id", count="exact")\
                    .eq("campaign_id", campaign_id)\
                    .in_("status", ["finalizado", "erro", "sem_debito", "sem_telefone", "falha_sem_linha"])\
                    .execute()
                finished = finished_res.count or 0
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

        task = process_file.apply_async(args=[job["id"], file_id, fname], queue="imports")
        try:
            supabase.table("import_jobs").update({"celery_task_id": task.id}).eq("id", job["id"]).execute()
        except Exception:
            pass

        return jsonify({"ok": True, "mode": "async", "job_id": job["id"], "task_id": task.id})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── IMPORT: Supabase Storage ───────────────────────────────────

@app.route("/api/upload/presigned", methods=["GET"])
def get_presigned_url():
    return jsonify({
        "ok": False,
        "error": "Upload via Storage foi desativado. Use /api/import/upload.",
    }), 410
    try:
        ext = request.args.get("ext", "csv").lstrip(".")
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
    return jsonify({
        "ok": False,
        "error": "Importacao via Storage foi desativada. Use /api/import/upload.",
    }), 410
    try:
        data = request.json or {}
        storage_path = data.get("path")
        filename= data.get("filename", "import.csv")
        size_bytes= data.get("size_bytes", 0)

        if not storage_path:
            return jsonify({"ok": False, "error": "path obrigatório"}), 400

        fname = filename.lower()
        if not (fname.endswith(".csv") or fname.endswith(".xlsx") or fname.endswith(".xls")):
            return jsonify({"ok": False, "error": "Formato inválido. Use .csv ou .xlsx"}), 400

        job = supabase.table("import_jobs").insert({
            "status": "queued",
            "total":     0,
            "with_debt": 0,
            "processed": 0,
            "result":    {"filename": filename, "storage_path": storage_path, "size_bytes": size_bytes},
        }).execute().data[0]

        task = process_import_from_storage.apply_async(args=[job["id"], storage_path, fname], queue="imports")
        try:
            supabase.table("import_jobs").update({"celery_task_id": task.id}).eq("id", job["id"]).execute()
        except Exception:
            pass

        return jsonify({"ok": True, "job_id": job["id"], "task_id": task.id, "status": "queued"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/import/stop/<job_id>", methods=["POST"])
def import_stop(job_id):
    import tasks as tasks_mod
    try:
        celery_app = tasks_mod.celery
        job = supabase.table("import_jobs").select("*").eq("id", job_id).execute()
        task_id = ""
        if job.data:
            task_id = (job.data[0].get("celery_task_id") or "").strip()
        redis_client().setex(f"import_job:{job_id}:stop", 86400, "stopped")
        celery_app.control.revoke(task_id or job_id, terminate=True, signal='SIGTERM')
        if job.data:
            supabase.table("import_jobs").update({
                "status": "stopped"
            }).eq("id", job_id).execute()
    except Exception as e:
        logging.warning(f"[IMPORT_STOP] erro=%s", e)
        try:
            redis_client().setex(f"import_job:{job_id}:stop", 86400, "stopped")
        except Exception:
            pass
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

from flask import Response, stream_with_context

@app.route("/api/stream")
def stream():
    token = _bearer_token()
    if API_AUTH_TOKEN and not _valid_secret(token, API_AUTH_TOKEN):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    def _build_payload():
        payload = {}
        try:
            wavoip_token = wavoip_login()
            devices      = wavoip_get_devices(wavoip_token)
            device_map   = {d.get("token"): d for d in devices}
            active_counts = _active_line_counts(DEVICE_PRIORITY)
            overrides    = _line_overrides_map()
            cooldowns    = {t: _is_line_in_cooldown(t) for t in DEVICE_PRIORITY}
            payload["lines"] = [{
                "id":               (device_map.get(t) or {}).get("id"),
                "name":             (device_map.get(t) or {}).get("name") or f"Linha {idx + 1}",
                "phone":            (device_map.get(t) or {}).get("phone"),
                "status":           (device_map.get(t) or {}).get("status") or "missing",
                "disabled":         (device_map.get(t) or {}).get("disabled"),
                "calls_made":       (device_map.get(t) or {}).get("calls_made"),
                "needs_restart":    (device_map.get(t) or {}).get("needs_restart"),
                "token":            t,
                "phone_number_id":  WAVOIP_VAPI_MAP.get(t) or "",
                "phone_number_short": ((WAVOIP_VAPI_MAP.get(t) or "")[:8] + "...") if WAVOIP_VAPI_MAP.get(t) else "",
                "env_name":         WAVOIP_ENV_MAP.get(t) or "",
                "configured":       bool(WAVOIP_VAPI_MAP.get(t)),
                "max_concurrent":   LINE_MAX_CONCURRENT,
                "active_calls":     active_counts.get(t, 0),
                "free_slots":       max(0, LINE_MAX_CONCURRENT - active_counts.get(t, 0)),
                "paused":           bool((overrides.get(t) or {}).get("paused")),
                "pause_reason":     (overrides.get(t) or {}).get("reason", ""),
                "cooldown":         bool(cooldowns.get(t)),
                "healthy":          t in device_map
                                    and (device_map[t].get("status") == "open")
                                    and device_map[t].get("disabled") == 0
                                    and device_map[t].get("phone") is not None
                                    and bool(WAVOIP_VAPI_MAP.get(t))
                                    and not bool(cooldowns.get(t))
                                    and not bool((overrides.get(t) or {}).get("paused")),
            } for idx, t in enumerate(DEVICE_PRIORITY)]
        except Exception as e:
            payload["lines_error"] = str(e)

        try:
            payload["campaigns"] = supabase.table("campaigns")\
                .select("*").order("created_at", desc=True).execute().data or []
        except Exception as e:
            payload["campaigns_error"] = str(e)

        try:
            camps = supabase.table("campaigns")\
                .select("id, name").eq("status", "em_andamento").execute().data or []
            if camps:
                camp_ids = [c["id"] for c in camps]
                camp_map = {c["id"]: c["name"] for c in camps}
                result   = supabase.table("campaign_calls")\
                    .select("*")\
                    .in_("campaign_id", camp_ids)\
                    .in_("status", ["em_andamento", "atendido", "enfileirado", "erro"])\
                    .order("created_at", desc=True)\
                    .limit(100).execute()
                monitor_rows = []
                for r in (result.data or []):
                    meta = _line_meta_from_debito(r)
                    monitor_rows.append({
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
                payload["monitor"] = {
                    "data": monitor_rows,
                    "stats": {
                        "em_andamento": sum(1 for r in monitor_rows if r["status"] in ("em_andamento", "enfileirado")),
                        "atendido":     sum(1 for r in monitor_rows if r["status"] == "atendido"),
                        "erro":         sum(1 for r in monitor_rows if r["status"] == "erro"),
                    }
                }
            else:
                payload["monitor"] = {"data": [], "stats": {"em_andamento": 0, "atendido": 0, "erro": 0}}
        except Exception as e:
            payload["monitor_error"] = str(e)

        return payload

    def generate():
        # heartbeat inicial
        yield "event: connected\ndata: {}\n\n"
        while True:
            try:
                data = _build_payload()
                yield f"data: {json.dumps(data, default=str)}\n\n"
            except GeneratorExit:
                break
            except Exception as e:
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
            time.sleep(4)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
        }
    )
if __name__ == "__main__":
    app.run(debug=True, port=5000)
