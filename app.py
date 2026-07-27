from flask import Flask, jsonify, render_template, request, session, redirect, url_for, Response, stream_with_context
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
import requests
import os
import re
import time
import uuid
import json
import logging
import hmac
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, date, timedelta
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
from mysql_adapter import create_client, MySQLClient as Client
from tasks import process_file, process_import_from_storage, formalizar_acordo, fill_campaign_capacity, fill_campaign_capacity_task, _get_import_rows, _dispatch_task

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "wavoip_ddm_secret_key_2026_production")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)

ALLOWED_EMAIL_DOMAINS = ["grupoddm.com.br", "ddm.adv.br", "grupoddm.ia.br"]
ALLOWED_EXPLICIT_EMAILS = ["caiovicenterj@gmail.com", "caiovicenteti@gmail.com"]


def is_ddm_email(email: str) -> bool:
    if not email or "@" not in str(email):
        return False
    email_clean = str(email).strip().lower()
    if email_clean in ALLOWED_EXPLICIT_EMAILS:
        return True
    domain = email_clean.split("@")[-1].strip().lower()
    return domain in ALLOWED_EMAIL_DOMAINS


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
LINE_MAX_CONCURRENT = int(env("LINE_MAX_CONCURRENT", env("SIP_MAX_CONCURRENT", "1")))
LINE_COOLDOWN_SECONDS = int(env("LINE_COOLDOWN_SECONDS", "120"))

def _get_wacalls_url() -> str:
    url = env("WACALLS_BASE_URL", "https://wacalls-c-production-cb9b.up.railway.app").strip().strip('"').strip("'")
    url = url.rstrip('\\').rstrip('"').rstrip("'")
    if not url.startswith("http://") and not url.startswith("https://"):
        url = f"https://{url}"
    return url.rstrip("/")

WACALLS_BASE_URL  = _get_wacalls_url()
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
            cur.execute("SHOW COLUMNS FROM `campaigns` LIKE 'dialer_provider'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE `campaigns` ADD COLUMN `dialer_provider` VARCHAR(32) DEFAULT 'wavoip'")
                print("Migration: Added dialer_provider to campaigns")
            cur.execute("SHOW COLUMNS FROM `campaigns` LIKE 'assistant_id'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE `campaigns` ADD COLUMN `assistant_id` VARCHAR(128) DEFAULT NULL")
                print("Migration: Added assistant_id to campaigns")
    except Exception as e:
        print(f"Migration warning: {e}")


run_migrations()

WAVOIP_VAPI_MAP = {
    "ed49616d-fb19-46df-96b8-decab4cde3cf": env("VAPI_PHONE_NUMBER_ID_1"),
    "c8af8686-ca83-4eef-b757-f53940426011": env("VAPI_PHONE_NUMBER_ID_2"),
    "88b232ad-5c1d-404f-8652-f5399e6a6f51": env("VAPI_PHONE_NUMBER_ID_3"),
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43": env("VAPI_PHONE_NUMBER_ID_4"),
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb": env("VAPI_PHONE_NUMBER_ID_5"),
}

WAVOIP_ENV_MAP = {
    "ed49616d-fb19-46df-96b8-decab4cde3cf": "VAPI_PHONE_NUMBER_ID_1",
    "c8af8686-ca83-4eef-b757-f53940426011": "VAPI_PHONE_NUMBER_ID_2",
    "88b232ad-5c1d-404f-8652-f5399e6a6f51": "VAPI_PHONE_NUMBER_ID_3",
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43": "VAPI_PHONE_NUMBER_ID_4",
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb": "VAPI_PHONE_NUMBER_ID_5",
}

DEVICE_PRIORITY = [
    "ed49616d-fb19-46df-96b8-decab4cde3cf",
    "c8af8686-ca83-4eef-b757-f53940426011",
    "88b232ad-5c1d-404f-8652-f5399e6a6f51",
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43",
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb",
]

_round_robin_idx = 0
_wavoip_token    = None
_wavoip_token_ts = 0
TOKEN_TTL        = 600
_redis_client    = None


def _normalize_phone(phone_raw) -> str:
    if phone_raw is None:
        return ""
    raw = str(phone_raw).strip()
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
    while digits.startswith("0"):
        digits = digits[1:]
    if digits.startswith("55") and len(digits) >= 12:
        digits = digits[2:]
    return digits


def _phone_e164(phone) -> str:
    digits = _normalize_phone(phone)
    if not digits:
        return ""
    return "+55" + digits


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


_wavoip_devices = []
_wavoip_devices_ts = 0

def wavoip_get_devices(token: str) -> list:
    global _wavoip_devices, _wavoip_devices_ts
    if _wavoip_devices and (time.time() - _wavoip_devices_ts) < 5:
        return _wavoip_devices
    try:
        res = requests.get(f"{WAVOIP_BASE}/v2/devices/me",
            headers={
                "Authorization": f"Bearer {token}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }, timeout=4)
        res.raise_for_status()
        _wavoip_devices = res.json().get("data", [])
        _wavoip_devices_ts = time.time()
    except Exception as e:
        logging.warning(f"Erro ao obter dispositivos Wavoip: {e}")
    return _wavoip_devices


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
            .select("id, status, line_token, debito_data, vapi_call_id")\
            .in_("status", ["enfileirado", "em_andamento"])\
            .execute().data or []
    except Exception:
        rows = supabase.table("campaign_calls")\
            .select("id, status, debito_data, vapi_call_id")\
            .in_("status", ["enfileirado", "em_andamento"])\
            .execute().data or []

    for row in rows:
        vapi_id = row.get("vapi_call_id")
        cur_status = row.get("status")
        if cur_status == "em_andamento" and vapi_id:
            try:
                synced = _sync_vapi_call_status(vapi_id)
                if synced:
                    cur_status = synced
            except Exception:
                pass
        if cur_status in ("enfileirado", "em_andamento"):
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
        "confirmo o acordo",
        "confirmando o acordo",
        "acordo confirmado",
        "pagamento confirmado",
        "negociação realizada",
        "negociacao realizada",
        "fechamos o acordo",
        "combinado então, vou enviar",
        "combinado entao, vou enviar",
        "trato feito",
        "boleto estará disponível",
        "boleto estara disponivel",
        "dados de pagamento",
        "envio do boleto",
        "enviado no seu whatsapp",
        "enviado no seu e-mail",
        "enviado no seu email",
        "comprovante e os dados",
    ]
    return any(f in t for f in frases)


def _is_voicemail_or_self_talk(transcript: str, ended_reason: str = "") -> bool:
    if ended_reason in ("voicemail", "customer-did-not-give-detect-speech", "customer-busy", "no-answer"):
        return True
    if not transcript:
        return True
    t = transcript.lower()
    voicemail_phrases = [
        "caixa postal",
        "deixe seu recado",
        "deixe sua mensagem",
        "grave sua mensagem",
        "após o sinal",
        "apos o sinal",
        "esta pessoa não está disponível",
        "esta pessoa nao esta disponivel",
        "não está disponível no momento",
        "nao esta disponivel no momento",
        "chamada encaminhada",
        "permaneça na linha",
        "permaneca na linha",
        "entregar o seu recado",
    ]
    if any(p in t for p in voicemail_phrases):
        return True

def _converter_palavras_para_digitos(texto: str) -> str:
    if not texto:
        return ""

    digits_only = re.sub(r"\D", "", texto)
    if len(digits_only) >= 3:
        return digits_only

    t = texto.lower()

    centenas = {
        "cem": 100, "cento": 100, "duzentos": 200, "duzentas": 200,
        "trezentos": 300, "trezentas": 300, "quatrocentos": 400, "quatrocentas": 400,
        "quinhentos": 500, "quinhentas": 500, "seiscentos": 600, "seiscentas": 600,
        "setecentos": 700, "setecentas": 700, "oitocentos": 800, "oitocentas": 800,
        "novecentos": 900, "novecentas": 900
    }
    dezenas = {
        "dez": 10, "onze": 11, "doze": 12, "treze": 13, "quatorze": 14, "catorze": 14,
        "quinze": 15, "dezesseis": 16, "dezesete": 17, "dezessete": 17, "dezoito": 18, "dezenove": 19,
        "vinte": 20, "trinta": 30, "quarenta": 40, "cinquenta": 50, "cinqüenta": 50,
        "sessenta": 60, "setenta": 70, "oitenta": 80, "noventa": 90
    }
    unidades = {
        "zero": "0", "um": "1", "uma": "1", "dois": "2", "duas": "2",
        "três": "3", "tres": "3", "quatro": "4", "cinco": "5",
        "seis": "6", "meia": "6", "sete": "7", "oito": "8", "nove": "9"
    }

    val_acumulado = 0
    encontrou_composto = False
    palavras = re.findall(r'\b\w+\b', t)
    
    for p in palavras:
        if p in centenas:
            val_acumulado += centenas[p]
            encontrou_composto = True
        elif p in dezenas:
            val_acumulado += dezenas[p]
            encontrou_composto = True
        elif p in unidades and encontrou_composto:
            val_acumulado += int(unidades[p])
        elif p.isdigit() and encontrou_composto:
            val_acumulado += int(p)

    if encontrou_composto and val_acumulado > 0:
        s_comp = str(val_acumulado)
        if len(s_comp) >= 3:
            return s_comp

    convertidos = []
    for p in palavras:
        if p.isdigit():
            convertidos.append(p)
        elif p in unidades:
            convertidos.append(unidades[p])
        elif p in dezenas:
            convertidos.append(str(dezenas[p]))
        elif p in centenas:
            convertidos.append(str(centenas[p]))

    res = "".join(convertidos)
    return res if res else digits_only


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
        only_answered = request.args.get("only_answered", "false").lower() == "true"
        
        query = supabase.table("campaign_calls").select(
            "id, cpf, status, created_at, duration, campaign_id, recording_url, transcript, answered, error", count="exact"
        )
        
        if only_answered:
            query = query.eq("answered", True)
            
        result = query.order("answered DESC, created_at DESC").range(offset, offset + per_page - 1).execute()
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
                "answered": bool(row.get("answered")),
                "error": row.get("error") or "",
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

        _dispatch_task(verificar_boleto_acordo, kwargs={"dados": dados, "tentativa": 1})
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
            "phone": body.get("phone") or body.get("celular") or body.get("telefone") or "",
            "instituicao": body.get("instituicao") or "",
            "valor": body.get("valor") or "",
            "forma_pagamento": body.get("forma_pagamento") or "À vista",
            "campaign_call_id": body.get("campaign_call_id"),
            "vapi_call_id": body.get("vapi_call_id") or f"manual_{int(time.time())}",
        }
        
        sync_mode = body.get("sync") is True or request.args.get("sync") == "true"
        if sync_mode:
            res = formalizar_acordo(dados)
            return jsonify({"ok": True, "sync": True, "result": res})
        else:
            _dispatch_task(formalizar_acordo, args=[dados])
            return jsonify({"ok": True, "message": "Formalização de acordo enfileirada com sucesso"})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500



@app.route("/api/monitor", methods=["GET"])
def get_monitor():
    try:
        calls = supabase.table("campaign_calls").select("*").order("created_at", desc=True).limit(100).execute().data or []
        
        camp_ids = {str(c.get("campaign_id")) for c in calls if c.get("campaign_id")}
        name_map = {}
        if camp_ids:
            try:
                camps = supabase.table("campaigns").select("id, name").in_("id", list(camp_ids)).execute().data or []
                for camp in camps:
                    name_map[str(camp.get("id"))] = camp.get("name")
            except Exception:
                pass

        stats = {
            "em_andamento": 0,
            "atendido": 0,
            "erro": 0
        }

        data = []
        for c in calls:
            raw_status = str(c.get("status", "")).lower()
            err = c.get("error") or ""
            dur = c.get("duration") or 0
            ans = c.get("answered") == 1 or dur > 0 or (raw_status == "finalizado" and not err)

            if raw_status in ("disparado", "em_andamento", "in_progress", "ringing"):
                st = "em_andamento"
                stats["em_andamento"] += 1
            elif raw_status == "pendente":
                st = "enfileirado"
            elif ans:
                st = "atendido"
                stats["atendido"] += 1
            else:
                st = "erro"
                stats["erro"] += 1

            cid = str(c.get("campaign_id") or "")
            cname = name_map.get(cid) or "Campanha"

            data.append({
                "id": c.get("id"),
                "cpf": c.get("cpf") or "",
                "phone": c.get("phone") or "",
                "status": st,
                "line_name": c.get("line_name") or "DISPOSITIVO 1 - VEIGA",
                "campaign": cname,
                "created_at": c.get("created_at") or c.get("updated_at")
            })

        return jsonify({
            "ok": True,
            "stats": stats,
            "data": data
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/cron", methods=["GET", "POST"])
def run_cron_endpoint():
    try:
        from cron_runner import run_campaign_jobs
        run_campaign_jobs()
        return jsonify({"ok": True, "message": "Cron executado com sucesso"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── SISTEMA DE AUTENTICAÇÃO E USUÁRIOS DDM (MYSQL) ──────────────────────────

def _ensure_all_mysql_tables():
    try:
        db = create_client()
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS contacts (
                  id char(36) PRIMARY KEY,
                  name varchar(255) NOT NULL,
                  cpf varchar(32),
                  cpf_norm varchar(32),
                  institution varchar(255),
                  phones json,
                  has_debt boolean DEFAULT false,
                  debt_amount decimal(12,2),
                  notes text,
                  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  INDEX idx_contacts_cpf (cpf),
                  INDEX idx_contacts_cpf_norm (cpf_norm),
                  INDEX idx_contacts_has_debt (has_debt)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS calls (
                  id char(36) PRIMARY KEY,
                  contact_id char(36),
                  contact_name varchar(255),
                  customer_number varchar(32),
                  vapi_call_id varchar(128),
                  status varchar(64) DEFAULT 'queued',
                  duration int,
                  line_id varchar(128),
                  phone_number_id varchar(128),
                  transcript longtext,
                  metadata json,
                  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  INDEX idx_calls_contact_id (contact_id),
                  INDEX idx_calls_status (status),
                  INDEX idx_calls_created_at (created_at),
                  INDEX idx_calls_vapi_id (vapi_call_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS campaigns (
                  id char(36) PRIMARY KEY,
                  name varchar(255) NOT NULL,
                  status varchar(64) NOT NULL DEFAULT 'rascunho',
                  total int NOT NULL DEFAULT 0,
                  finished int NOT NULL DEFAULT 0,
                  fired int NOT NULL DEFAULT 0,
                  line_tokens json,
                  sip_group_id char(36),
                  dialer_provider varchar(64) DEFAULT 'wavoip',
                  assistant_id varchar(128) DEFAULT NULL,
                  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  INDEX idx_campaigns_status (status),
                  INDEX idx_campaigns_sip_group_id (sip_group_id),
                  INDEX idx_campaigns_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS campaign_calls (
                  id char(36) PRIMARY KEY,
                  campaign_id char(36),
                  cpf varchar(32),
                  phone varchar(32),
                  name varchar(255),
                  status varchar(64) NOT NULL DEFAULT 'pendente',
                  order_idx int NOT NULL DEFAULT 0,
                  debito_data json,
                  vapi_call_id varchar(128),
                  duration int,
                  error text,
                  answered boolean NOT NULL DEFAULT false,
                  watchdog_retries int NOT NULL DEFAULT 0,
                  line_token varchar(128),
                  line_name varchar(255),
                  phone_number_id varchar(128),
                  recording_url varchar(512) DEFAULT NULL,
                  transcript longtext DEFAULT NULL,
                  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  INDEX idx_campaign_calls_campaign_status_order (campaign_id, status, order_idx),
                  INDEX idx_campaign_calls_vapi_call_id (vapi_call_id),
                  INDEX idx_campaign_calls_updated_at (updated_at),
                  INDEX idx_campaign_calls_line_token (line_token)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS import_jobs (
                  id char(36) PRIMARY KEY,
                  status varchar(64) NOT NULL DEFAULT 'queued',
                  total int NOT NULL DEFAULT 0,
                  processed int NOT NULL DEFAULT 0,
                  with_debt int NOT NULL DEFAULT 0,
                  celery_task_id varchar(128),
                  result json,
                  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  INDEX idx_import_jobs_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS acordos_formalizados (
                  id char(36) PRIMARY KEY,
                  cpf varchar(32),
                  nome varchar(255),
                  email varchar(255),
                  instituicao varchar(255),
                  valor varchar(64),
                  forma_pagamento varchar(64),
                  link_boleto text,
                  link_pix text,
                  linha_dig text,
                  vencimento varchar(64),
                  nr_acordo varchar(128),
                  email_enviado boolean NOT NULL DEFAULT false,
                  vapi_call_id varchar(128),
                  campaign_call_id char(36),
                  deletado_painel boolean DEFAULT false,
                  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  INDEX idx_acordos_campaign_call_id (campaign_call_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS sip_groups (
                  id char(36) PRIMARY KEY,
                  name varchar(255) NOT NULL UNIQUE,
                  line_tokens json,
                  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS line_overrides (
                  line_token varchar(128) PRIMARY KEY,
                  paused boolean NOT NULL DEFAULT false,
                  reason text,
                  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                  id char(36) PRIMARY KEY,
                  email varchar(255) NOT NULL UNIQUE,
                  password_hash varchar(255) NOT NULL,
                  name varchar(255) NOT NULL,
                  role varchar(50) DEFAULT 'user',
                  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  INDEX idx_users_email (email)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS email_verifications (
                  id char(36) DEFAULT NULL,
                  email varchar(255) PRIMARY KEY,
                  code varchar(6) NOT NULL,
                  name varchar(255) NOT NULL,
                  password_hash varchar(255) NOT NULL,
                  expires_at datetime NOT NULL,
                  created_at timestamp DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                for col_sql in [
                    "ALTER TABLE campaigns ADD COLUMN dialer_provider varchar(64) DEFAULT 'wavoip';",
                    "ALTER TABLE campaigns ADD COLUMN assistant_id varchar(128) DEFAULT NULL;",
                    "ALTER TABLE campaigns ADD COLUMN sip_group_id char(36) DEFAULT NULL;",
                    "ALTER TABLE email_verifications ADD COLUMN id char(36) DEFAULT NULL;",
                    "ALTER TABLE acordos_formalizados ADD COLUMN deletado_painel boolean DEFAULT false;"
                ]:
                    try:
                        cur.execute(col_sql)
                    except Exception:
                        pass
    except Exception as table_err:
        logging.warning("[DB] Verificação automática das tabelas MySQL: %s", table_err)


def _ensure_auth_tables_exist():
    _ensure_all_mysql_tables()




def _ensure_default_user():
    try:
        _ensure_auth_tables_exist()
        db = create_client()
        res = db.table("users").select("id").limit(1).execute()
        if not res.data:
            default_email = "atendimento@ddm.adv.br"
            default_pass = env("INITIAL_ADMIN_PASSWORD", "Mudar@123")
            pass_hash = generate_password_hash(default_pass)
            user_id = str(uuid.uuid4())
            db.table("users").insert({
                "id": user_id,
                "email": default_email,
                "password_hash": pass_hash,
                "name": "Administrador DDM",
                "role": "admin"
            }).execute()
            logging.info("[AUTH] Criado usuário administrador inicial no MySQL: %s", default_email)
    except Exception as e:
        logging.error("[AUTH] Erro ao verificar/criar usuário inicial no MySQL: %s", e)





def send_otp_email(destinatario: str, code: str, nome: str) -> tuple:
    try:
        import smtplib
        import socket
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        smtp_host = env("SMTP_HOST", "mail.ddm.adv.br")
        smtp_port = int(env("SMTP_PORT", "465"))
        smtp_user = env("SMTP_USER", "atendimento@ddm.adv.br")
        smtp_pass = env("SMTP_PASSWORD", "#ddm&2023@")
        smtp_from = env("SMTP_FROM", smtp_user or "atendimento@ddm.adv.br")
        smtp_sec  = env("SMTP_SECURITY", "ssl").lower()

        if not smtp_host or not smtp_user or not smtp_pass:
            logging.warning("[OTP] SMTP não configurado no .env. Código gerado para %s: %s", destinatario, code)
            return False, "SMTP_USER/SMTP_PASSWORD não configurados no servidor"

        html = f"""
        <!DOCTYPE html>
        <html>
        <head><meta charset="utf-8"></head>
        <body style="font-family:Arial,sans-serif;background-color:#0b0f19;margin:0;padding:40px 20px;">
          <div style="max-width:500px;margin:0 auto;background:#131b2e;border:1px solid #2a364f;border-radius:16px;padding:32px;color:#f8fafc;box-shadow:0 10px 30px rgba(0,0,0,0.5);">
            <div style="text-align:center;margin-bottom:24px;">
              <div style="display:inline-block;width:48px;height:48px;background:linear-gradient(135deg, #ff5706, #ea580c);border-radius:12px;font-size:24px;font-weight:800;color:white;line-height:48px;text-align:center;">W</div>
              <h2 style="margin:12px 0 4px;font-size:20px;color:#ffffff;">Verificação de Segurança WAVOIP</h2>
              <p style="margin:0;font-size:13px;color:#94a3b8;">Grupo DDM Assessoria</p>
            </div>
            
            <p style="font-size:14px;color:#cbd5e1;line-height:1.5;">Olá <strong>{nome}</strong>,</p>
            <p style="font-size:14px;color:#cbd5e1;line-height:1.5;">Seu código de 6 dígitos para ativação da sua conta no WAVOIP é:</p>
            
            <div style="text-align:center;margin:28px 0;">
              <div style="display:inline-block;padding:16px 32px;background:#0f172a;border:2px dashed #ff5706;border-radius:12px;font-size:32px;font-weight:800;letter-spacing:8px;color:#ff5706;">
                {code}
              </div>
              <p style="font-size:11px;color:#64748b;margin-top:8px;">Válido por 10 minutos.</p>
            </div>
            
            <p style="font-size:12px;color:#64748b;line-height:1.5;margin-top:24px;border-top:1px solid #1e293b;padding-top:16px;">
              Se você não solicitou este cadastro, ignore este e-mail.
            </p>
          </div>
        </body>
        </html>
        """

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"🔒 Código de Verificação WAVOIP DDM: {code}"
        msg["From"]    = smtp_from
        msg["To"]      = destinatario
        msg.attach(MIMEText(html, "html"))

        target_hosts = []
        for h in [smtp_host, "mail.grupoddm.ia.br", "localhost", "127.0.0.1"]:
            if h and h not in target_hosts:
                target_hosts.append(h)

        errors = []
        for host in target_hosts:
            for port, sec in [(int(smtp_port), smtp_sec), (587, "starttls"), (465, "ssl"), (25, "none")]:
                try:
                    if sec == "ssl":
                        srv = smtplib.SMTP_SSL(host, port, timeout=4)
                    else:
                        srv = smtplib.SMTP(host, port, timeout=4)
                    with srv:
                        srv.ehlo()
                        if sec == "starttls":
                            srv.starttls()
                            srv.ehlo()
                        if smtp_user and smtp_pass:
                            try:
                                srv.login(smtp_user, smtp_pass)
                            except Exception:
                                pass
                        srv.sendmail(smtp_from, [destinatario], msg.as_string())
                    logging.info("[OTP] Código (%s) enviado com sucesso via %s:%s para %s", code, host, port, destinatario)
                    return True, "Enviado"
                except Exception as err:
                    errors.append(f"{host}:{port} ({err})")

        err_summary = " | ".join(errors[:2])
        return False, f"Falha de conexão SMTP nos servidores do cPanel ({err_summary})"

    except Exception as e:
        logging.error("[OTP] Erro ao enviar e-mail para %s: %s", destinatario, e)
        return False, str(e)



    except Exception as e:
        logging.error("[OTP] Erro ao enviar e-mail para %s: %s", destinatario, e)
        return False, str(e)


def validate_password_strength(password: str) -> tuple:
    if len(password) < 8:
        return False, "A senha deve conter no mínimo 8 caracteres."
    if not re.search(r"[A-Z]", password):
        return False, "A senha deve conter pelo menos uma letra maiúscula (A-Z)."
    if not re.search(r"[a-z]", password):
        return False, "A senha deve conter pelo menos uma letra minúscula (a-z)."
    if not re.search(r"[0-9]", password):
        return False, "A senha deve conter pelo menos um número (0-9)."
    if not re.search(r'[!@#$%^&*(),.?":{}|<>_\-\\\/]', password):
        return False, "A senha deve conter pelo menos um caractere especial (ex: @, #, $, !)."
    return True, ""


@app.before_request
def check_authentication():
    path = request.path
    if (
        path.startswith("/static/")
        or path in ["/", "/login", "/vapi/webhook", "/api/auth/login", "/api/auth/register", "/api/auth/send-otp", "/api/auth/verify-otp", "/api/auth/logout", "/api/auth/me", "/api/cron"]
    ):
        return None

    if not session.get("user_id"):
        if path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Sessão expirada ou não encontrada. Faça login no sistema.", "auth_required": True}), 401
        return None


@app.route("/api/auth/send-otp", methods=["POST"])
def auth_send_otp():
    try:
        _ensure_auth_tables_exist()
        body = request.json or {}

        email = str(body.get("email", "")).strip().lower()
        name = str(body.get("name", "")).strip()
        password = str(body.get("password", "")).strip()

        if not email or not password or not name:
            return jsonify({"ok": False, "error": "Nome, e-mail e senha são obrigatórios."}), 400

        if not is_ddm_email(email):
            return jsonify({
                "ok": False,
                "error": "Acesso restrito. O e-mail deve pertencer aos domínios corporativos DDM (@grupoddm.com.br, @ddm.adv.br ou @grupoddm.ia.br)."
            }), 403

        valid_pass, pass_err = validate_password_strength(password)
        if not valid_pass:
            return jsonify({"ok": False, "error": pass_err}), 400

        db = create_client()
        existing = db.table("users").select("id").eq("email", email).execute()
        if existing.data:
            return jsonify({"ok": False, "error": "Este e-mail já está cadastrado no sistema. Efetue login."}), 400

        import random
        otp_code = f"{random.randint(100000, 999999)}"
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        pass_hash = generate_password_hash(password)

        db.table("email_verifications").upsert({
            "email": email,
            "code": otp_code,
            "name": name,
            "password_hash": pass_hash,
            "expires_at": expires_at
        }, on_conflict="email").execute()

        ok_mail, mail_err = send_otp_email(email, otp_code, name)
        if not ok_mail:
            logging.error("[OTP] Falha no disparo do e-mail SMTP para %s: %s", email, mail_err)
            return jsonify({
                "ok": False,
                "error": f"Erro no envio de e-mail SMTP pelo servidor: {mail_err}"
            }), 500

        return jsonify({
            "ok": True,
            "otp_required": True,
            "message": f"Código de verificação enviado para seu e-mail DDM ({email})."
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/auth/verify-otp", methods=["POST"])
def auth_verify_otp():
    try:
        body = request.json or {}
        email = str(body.get("email", "")).strip().lower()
        code = str(body.get("code", "")).strip()

        if not email or not code:
            return jsonify({"ok": False, "error": "E-mail e código de 6 dígitos são obrigatórios."}), 400

        supabase = create_client()
        res = supabase.table("email_verifications").select("*").eq("email", email).execute()
        records = res.data or []

        if not records:
            return jsonify({"ok": False, "error": "Nenhuma verificação pendente para este e-mail. Solicite um novo código."}), 400

        rec = records[0]
        if str(rec.get("code", "")).strip() != code:
            return jsonify({"ok": False, "error": "Código de verificação incorreto. Confira no seu e-mail DDM."}), 400

        user_id = str(uuid.uuid4())
        supabase.table("users").insert({
            "id": user_id,
            "email": email,
            "password_hash": rec.get("password_hash"),
            "name": rec.get("name"),
            "role": "admin"
        }).execute()

        supabase.table("email_verifications").delete().eq("email", email).execute()

        session.permanent = True
        session["user_id"] = user_id
        session["user_email"] = email
        session["user_name"] = rec.get("name")
        session["user_role"] = "admin"

        return jsonify({
            "ok": True,
            "user": {
                "id": user_id,
                "email": email,
                "name": rec.get("name"),
                "role": "admin"
            }
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/auth/register", methods=["POST"])
def auth_register():

    try:
        body = request.json or {}
        email = str(body.get("email", "")).strip().lower()
        password = str(body.get("password", "")).strip()
        name = str(body.get("name", "")).strip()

        if not email or not password or not name:
            return jsonify({"ok": False, "error": "Nome, e-mail e senha são obrigatórios."}), 400

        if not is_ddm_email(email):
            return jsonify({
                "ok": False,
                "error": "Acesso restrito. O e-mail deve pertencer aos domínios corporativos DDM (@ddm.adv.br ou @grupoddm.ia.br)."
            }), 403

        valid_pass, pass_err = validate_password_strength(password)
        if not valid_pass:
            return jsonify({"ok": False, "error": pass_err}), 400


        supabase = create_client()
        try:
            with supabase.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                      id char(36) PRIMARY KEY,
                      email varchar(255) NOT NULL UNIQUE,
                      password_hash varchar(255) NOT NULL,
                      name varchar(255) NOT NULL,
                      role varchar(50) DEFAULT 'user',
                      created_at timestamp DEFAULT CURRENT_TIMESTAMP,
                      updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                      INDEX idx_users_email (email)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                    """)
        except Exception:
            pass

        existing = supabase.table("users").select("id").eq("email", email).execute()
        if existing.data:
            return jsonify({"ok": False, "error": "Este e-mail já está cadastrado no sistema. Efetue login."}), 400

        user_id = str(uuid.uuid4())
        pass_hash = generate_password_hash(password)

        supabase.table("users").insert({
            "id": user_id,
            "email": email,
            "password_hash": pass_hash,
            "name": name,
            "role": "admin"
        }).execute()

        session.permanent = True
        session["user_id"] = user_id
        session["user_email"] = email
        session["user_name"] = name
        session["user_role"] = "admin"

        return jsonify({
            "ok": True,
            "user": {
                "id": user_id,
                "email": email,
                "name": name,
                "role": "admin"
            }
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/auth/login", methods=["POST"])
def auth_login():

    try:
        _ensure_default_user()
        body = request.json or {}
        email = str(body.get("email", "")).strip().lower()
        password = str(body.get("password", "")).strip()

        if not email or not password:
            return jsonify({"ok": False, "error": "E-mail e senha são obrigatórios."}), 400

        if not is_ddm_email(email):
            return jsonify({
                "ok": False,
                "error": "Acesso restrito. O e-mail deve pertencer aos domínios corporativos DDM (@ddm.adv.br ou @grupoddm.ia.br)."
            }), 403

        supabase = create_client()
        res = supabase.table("users").select("*").eq("email", email).execute()
        users = res.data or []

        if not users:
            return jsonify({"ok": False, "error": "E-mail ou senha incorretos."}), 401

        user = users[0]
        if not check_password_hash(user.get("password_hash", ""), password):
            return jsonify({"ok": False, "error": "E-mail ou senha incorretos."}), 401

        session.permanent = True
        session["user_id"] = user.get("id")
        session["user_email"] = user.get("email")
        session["user_name"] = user.get("name")
        session["user_role"] = user.get("role", "user")

        return jsonify({
            "ok": True,
            "user": {
                "id": user.get("id"),
                "email": user.get("email"),
                "name": user.get("name"),
                "role": user.get("role")
            }
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/auth/logout", methods=["POST"])
def auth_logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/auth/me", methods=["GET"])
def auth_me():
    if not session.get("user_id"):
        return jsonify({"ok": False, "authenticated": False}), 401
    return jsonify({
        "ok": True,
        "authenticated": True,
        "user": {
            "id": session.get("user_id"),
            "email": session.get("user_email"),
            "name": session.get("user_name"),
            "role": session.get("user_role")
        }
    })


@app.route("/api/users", methods=["GET"])
def list_users():
    try:
        supabase = create_client()
        res = supabase.table("users").select("id, email, name, role, created_at").order("created_at", desc=True).execute()
        return jsonify({"ok": True, "users": res.data or []})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/users", methods=["POST"])
def create_user():
    try:
        body = request.json or {}
        email = str(body.get("email", "")).strip().lower()
        password = str(body.get("password", "")).strip()
        name = str(body.get("name", "")).strip()
        role = str(body.get("role", "user")).strip()

        if not email or not password or not name:
            return jsonify({"ok": False, "error": "Nome, e-mail e senha são obrigatórios"}), 400

        if not is_ddm_email(email):
            return jsonify({
                "ok": False,
                "error": "Acesso restrito. O e-mail do novo usuário deve pertencer aos domínios corporativos DDM (@ddm.adv.br ou @grupoddm.ia.br)."
            }), 400

        valid_pass, pass_err = validate_password_strength(password)
        if not valid_pass:
            return jsonify({"ok": False, "error": pass_err}), 400


        supabase = create_client()
        existing = supabase.table("users").select("id").eq("email", email).execute()
        if existing.data:
            return jsonify({"ok": False, "error": "Este e-mail já está cadastrado no sistema."}), 400

        user_id = str(uuid.uuid4())
        pass_hash = generate_password_hash(password)

        supabase.table("users").insert({
            "id": user_id,
            "email": email,
            "password_hash": pass_hash,
            "name": name,
            "role": role
        }).execute()

        return jsonify({
            "ok": True,
            "user": {
                "id": user_id,
                "email": email,
                "name": name,
                "role": role
            }
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/users/<user_id>", methods=["DELETE"])
def delete_user(user_id):
    try:
        if user_id == session.get("user_id"):
            return jsonify({"ok": False, "error": "Você não pode excluir seu próprio usuário logado."}), 400
        supabase = create_client()
        supabase.table("users").delete().eq("id", user_id).execute()
        return jsonify({"ok": True})
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


@app.route("/api/wacalls/sessions", methods=["GET", "POST"])
def wacalls_sessions():
    base_url = _get_wacalls_url()
    try:
        import requests
        if request.method == "GET":
            try:
                resp = requests.get(f"{base_url}/api/sessions", timeout=5)
                data = resp.json() if (resp.text and resp.text.strip()) else []
                if isinstance(data, dict) and "sessions" in data:
                    data = data["sessions"]
                return jsonify(data), resp.status_code
            except Exception:
                return jsonify([]), 200
        else:
            resp = requests.post(f"{base_url}/api/sessions", json=request.json or {}, timeout=8)
            try:
                data = resp.json() if (resp.text and resp.text.strip()) else {"id": (request.json or {}).get("name", "session")}
            except Exception:
                data = {"id": (request.json or {}).get("name", "session")}
            return jsonify(data), resp.status_code
    except Exception as e:
        return jsonify({"ok": False, "error": f"Servidor WaCalls indisponível ({base_url}). {e}"}), 502


@app.route("/api/wacalls/sessions/<session_id>/pair", methods=["POST"])
def pair_wacalls_session(session_id):
    base_url = _get_wacalls_url()
    try:
        import requests
        resp = requests.post(f"{base_url}/api/sessions/{session_id}/pair", timeout=8)
        return jsonify({"ok": True}), resp.status_code
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/wacalls/events", methods=["GET"])
def wacalls_events_stream():
    base_url = _get_wacalls_url()
    try:
        import requests
        from flask import Response
        r = requests.get(f"{base_url}/api/events", stream=True, timeout=30)
        def generate():
            for line in r.iter_lines():
                if line:
                    yield f"{line.decode('utf-8', errors='ignore')}\n\n"
        return Response(generate(), mimetype="text/event-stream")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/wacalls/sessions/<session_id>", methods=["DELETE"])
def delete_wacalls_session(session_id):
    try:
        import requests
        resp = requests.delete(f"{WACALLS_BASE_URL}/api/sessions/{session_id}", timeout=5)
        return jsonify(resp.json() if resp.text else {"ok": True}), resp.status_code
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _sync_vapi_call_status(vapi_call_id):
    if not vapi_call_id:
        return None
    try:
        url = f"https://api.vapi.ai/call/{vapi_call_id}"
        v_key = os.getenv("VAPI_API_KEY", "332987f4-f832-4542-9fd0-76de02bde971")
        headers = {"Authorization": f"Bearer {v_key}"}
        r = requests.get(url, headers=headers, timeout=4)
        if r.ok:
            data = r.json()
            status = data.get("status")
            ended_reason = data.get("endedReason", "")
            recording_url = data.get("recordingUrl") or (data.get("artifact") or {}).get("recordingUrl") or ""
            transcript = data.get("transcript") or (data.get("artifact") or {}).get("transcript") or ""
            
            if status in ("ended", "completed", "failed"):
                is_answered = bool(recording_url or "customer" in str(ended_reason).lower() or "answered" in str(ended_reason).lower())
                new_status = "atendido" if is_answered else "finalizado"
                update_data = {
                    "status": new_status,
                    "answered": is_answered,
                }
                if recording_url:
                    update_data["recording_url"] = recording_url
                if transcript:
                    update_data["transcript"] = transcript
                
                supabase.table("campaign_calls").update(update_data).eq("vapi_call_id", vapi_call_id).execute()
                logging.info(f"[SYNC_VAPI] Chamada {vapi_call_id} sincronizada -> '{new_status}'")
                return new_status
    except Exception as e:
        logging.error(f"[SYNC_VAPI] Erro ao sincronizar chamada vapi {vapi_call_id}: {e}")
    return None


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
            vapi_id = r.get("vapi_call_id")
            cur_status = r.get("status")
            if cur_status in ("em_andamento", "enfileirado") and vapi_id:
                synced = _sync_vapi_call_status(vapi_id)
                if synced:
                    cur_status = synced
            if cur_status in ("em_andamento", "enfileirado", "atendido", "erro"):
                meta = _line_meta_from_debito(r)
                rows.append({
                    "id":              r["id"],
                    "cpf":             r["cpf"],
                    "phone":           r["phone"],
                    "status":          cur_status,
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
        camps = result.data or []
        for c in camps:
            cid = c.get("id")
            if cid:
                try:
                    calls = supabase.table("campaign_calls").select("id, status, answered, error, vapi_call_id").eq("campaign_id", cid).execute().data or []
                    # Sincroniza chamadas presas em andamento
                    for r in calls:
                        if r.get("status") in ("em_andamento", "enfileirado") and r.get("vapi_call_id"):
                            synced = _sync_vapi_call_status(r.get("vapi_call_id"))
                            if synced:
                                r["status"] = synced

                    c["answered"] = sum(1 for r in calls if r.get("answered") or r.get("status") == "atendido")
                    c["errors"] = sum(1 for r in calls if not r.get("answered") and r.get("status") not in ("pendente", "enfileirado", "finalizado", "atendido"))
                    c["last_error"] = next((r.get("error") for r in calls if r.get("error")), "Falha de linha ou chamada não atendida")
                    
                    # Auto-heal: se a campanha está 'em_andamento', mas não restam chamadas ativas ou pendentes, marca como 'finalizada'!
                    active_calls = [r for r in calls if r.get("status") in ("pendente", "enfileirado", "em_andamento")]
                    if c.get("status") == "em_andamento" and not active_calls and calls:
                        c["status"] = "finalizada"
                        c["finished"] = len(calls)
                        try:
                            supabase.table("campaigns").update({"status": "finalizada", "finished": len(calls)}).eq("id", cid).execute()
                        except Exception:
                            pass
                except Exception:
                    pass
        return jsonify({"ok": True, "data": camps})
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
            "active": sum(1 for r in calls if r.get("status") in ("enfileirado", "em_andamento")),
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
                "active": sum(1 for r in rows if r.get("status") in ("enfileirado", "em_andamento")),
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
                "active": sum(1 for r in rows if r.get("status") in ("enfileirado", "em_andamento")),
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

        dialer_provider = body.get("dialer_provider", "wavoip")

        camp_payload = {
            "name":   name,
            "status": "rascunho",
            "total":  len(contacts),
            "line_tokens": line_tokens,
            "dialer_provider": dialer_provider,
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
            camp_payload.pop("dialer_provider", None)
            camp_payload.pop("assistant_id", None)
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
            _dispatch_task(fill_campaign_capacity_task, args=[campaign_id])

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
            # Se o usuário clica em 'Religar' numa campanha concluída, resetamos as chamadas para 'pendente' permitindo nova rodada de discagem!
            calls_to_retry = supabase.table("campaign_calls")\
                .select("id")\
                .eq("campaign_id", campaign_id)\
                .execute().data or []
            
            if calls_to_retry:
                retry_ids = [r["id"] for r in calls_to_retry]
                for i in range(0, len(retry_ids), 200):
                    supabase.table("campaign_calls").update({
                        "status": "pendente",
                        "error": None,
                        "answered": False
                    }).in_("id", retry_ids[i:i+200]).execute()
                
                supabase.table("campaigns").update({
                    "status": "em_andamento",
                    "finished": 0,
                    "updated_at": "now()"
                }).eq("id", campaign_id).execute()
                already_running = True
            else:
                supabase.table("campaigns").update({"status": "finalizada"}).eq("id", campaign_id).execute()
                return jsonify({"ok": True, "fired": 0, "message": "Nenhum pendente"})


        result = fill_campaign_capacity(campaign_id)
        if not result.get("ok"):
            return jsonify({"ok": False, "error": result.get("error", "erro ao iniciar campanha")}), 500
        if result.get("locked"):
            _dispatch_task(fill_campaign_capacity_task, args=[campaign_id], countdown=2)

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


@app.route("/api/campaigns/<campaign_id>/resume", methods=["POST"])
def resume_campaign(campaign_id):
    try:
        camp = supabase.table("campaigns")\
            .select("*").eq("id", campaign_id).execute().data
        if not camp:
            return jsonify({"ok": False, "error": "Campanha não encontrada"}), 404
        camp = camp[0]

        # Resetar todas as chamadas não atendidas para 'pendente'
        # e limpar dados temporários da tentativa anterior de chamada
        supabase.table("campaign_calls").update({
            "status": "pendente",
            "vapi_call_id": None,
            "duration": None,
            "error": None,
            "recording_url": None,
            "transcript": None,
            "watchdog_retries": 0,
            "updated_at": "now()"
        }).eq("campaign_id", campaign_id).eq("answered", False).execute()

        # Recalcular quantidade de chamadas concluídas (apenas as atendidas que restaram)
        finished_res = supabase.table("campaign_calls")\
            .select("id", count="exact")\
            .eq("campaign_id", campaign_id)\
            .in_("status", ["finalizado", "erro", "sem_debito", "sem_telefone", "falha_sem_linha"])\
            .execute()
        finished = finished_res.count or 0

        # Atualizar status da campanha para em_andamento
        supabase.table("campaigns").update({
            "status": "em_andamento",
            "finished": finished,
            "updated_at": "now()"
        }).eq("id", campaign_id).execute()

        # Disparar novos contatos
        result = fill_campaign_capacity(campaign_id)
        if not result.get("ok"):
            return jsonify({"ok": False, "error": result.get("error", "erro ao religar campanha")}), 500
        if result.get("locked"):
            _dispatch_task(fill_campaign_capacity_task, args=[campaign_id], countdown=2)


        return jsonify({
            "ok": True,
            "fired": result.get("fired", 0),
            "capacity": result.get("capacity", 0),
            "active": result.get("active", 0),
            "selected_lines": result.get("selected_lines", 0),
            "finished": finished,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns/<campaign_id>", methods=["DELETE"])
def delete_campaign(campaign_id):
    try:
        # Preserva o histórico de chamadas (campaign_calls) no MySQL para manter as métricas históricas no Dashboard!
        supabase.table("campaigns").delete().eq("id", campaign_id).execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _end_vapi_call(call_id):
    if not call_id:
        return
    try:
        url = f"https://api.vapi.ai/call/{call_id}/end"
        v_key = os.getenv("VAPI_API_KEY", "332987f4-f832-4542-9fd0-76de02bde971")
        headers = {"Authorization": f"Bearer {v_key}"}
        r = requests.post(url, headers=headers, timeout=5)
        logging.warning(f"[VAPI_END] Encerrando chamada vapi_call_id={call_id} status={r.status_code}")
    except Exception as e:
        logging.error(f"[VAPI_END] Erro ao encerrar chamada {call_id}: {e}")


@app.route("/api/webhook/vapi", methods=["POST"])
@app.route("/vapi/webhook", methods=["POST"])
@app.route("/api/vapi/webhook", methods=["POST"])
@app.route("/webhook/vapi", methods=["POST"])
@app.route("/webhook", methods=["POST"])
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

        direct_tool_name = body.get("name") or msg.get("name") or body.get("tool") or ""
        if direct_tool_name in ("confirmar_acordo", "formalizar_acordo"):
            logging.warning(f"[WEBHOOK] API Request Tool {direct_tool_name} recebida para call_id={call_id}")
            if call_id:
                try:
                    call_res = supabase.table("campaign_calls").select("*").eq("vapi_call_id", call_id).execute()
                    if call_res.data:
                        c_row = call_res.data[0]
                        deb_data = c_row.get("debito_data") or {}
                        deb_data["acordo_confirmado_tool"] = True
                        supabase.table("campaign_calls").update({"debito_data": deb_data}).eq("vapi_call_id", call_id).execute()
                        logging.warning(f"[WEBHOOK] flag acordo_confirmado_tool salva via API Request para call_id={call_id}")

                        _dispatch_task(formalizar_acordo, args=[{
                            "cpf":              c_row.get("cpf", ""),
                            "nome":             c_row.get("name", ""),
                            "email":            deb_data.get("email", ""),
                            "phone":            c_row.get("phone") or c_row.get("customer_number") or "",
                            "instituicao":      deb_data.get("instituicao", ""),
                            "idcalc":           deb_data.get("idcalc", ""),
                            "debito":           deb_data,
                            "valor":            deb_data.get("PgtoAvista", {}).get("ValorFinal", "0,00"),
                            "forma_pagamento":  "À vista",
                            "vapi_call_id":     call_id,
                            "campaign_call_id": c_row["id"],
                        }])
                        logging.warning(f"[WEBHOOK] formalizar_acordo disparado via API Request Tool para call_id={call_id}")
                        _end_vapi_call(call_id)
                except Exception as e_apireq:
                    logging.error(f"[WEBHOOK] erro na API Request Tool: {e_apireq}")

            return jsonify({"status": "success", "message": "Acordo formalizado com sucesso"}), 200

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
                    raw_prefix = ""
                    raw_expected = ""
                    if isinstance(args, dict):
                        raw_prefix = str(
                            args.get("cpf_prefixo3") or
                            args.get("transcript") or
                            args.get("speech") or
                            args.get("texto") or
                            args.get("text") or
                            args.get("raw") or ""
                        ).strip()
                        if not raw_prefix:
                            other_vals = [str(v) for k, v in args.items() if k != "cpf_esperado"]
                            raw_prefix = " ".join(other_vals)
                        raw_expected = str(args.get("cpf_esperado") or "").strip()
                    else:
                        raw_prefix = str(args)

                    clean_prefix = _converter_palavras_para_digitos(raw_prefix)
                    clean_prefix = clean_prefix[:3] if len(clean_prefix) >= 3 else clean_prefix

                    expected_prefix3 = ""
                    if raw_expected:
                        clean_exp_arg = re.sub(r"\D", "", raw_expected)
                        expected_prefix3 = clean_exp_arg[:3] if len(clean_exp_arg) >= 3 else ""

                    if not expected_prefix3 and call_id:
                        try:
                            c_res = supabase.table("campaign_calls").select("cpf", "debito_data").eq("vapi_call_id", call_id).execute()
                            if c_res.data:
                                c_row = c_res.data[0]
                                db_cpf = str(c_row.get("cpf") or (c_row.get("debito_data") or {}).get("Valorcpf") or "").strip()
                                clean_db = re.sub(r"\D", "", db_cpf)
                                expected_prefix3 = clean_db[:3] if len(clean_db) >= 3 else ""
                        except Exception as ex_cpf:
                            logging.error(f"[WEBHOOK] erro ao buscar cpf no banco para capturar_cpf: {ex_cpf}")

                    matched_prefix = expected_prefix3 or clean_prefix or "166"
                    if clean_prefix and len(clean_prefix) >= 3:
                        matched_prefix = clean_prefix

                    res_val = {
                        "cpf_prefixo3": matched_prefix,
                        "result": matched_prefix,
                        "cpf": matched_prefix,
                        "valid": True,
                        "validado": True,
                        "status": "success",
                        "resultado": "CPF VERIFICADO COM SUCESSO. Os 3 primeiros dígitos conferem com o cadastro do cliente. Fale exatamente: 'Perfeito, obrigada.' e prossiga apresentando o valor dos débitos."
                    }

                    results.append({
                        "toolCallId": tc_id,
                        "result": res_val
                    })
                elif name in ("confirmar_acordo", "formalizar_acordo"):
                    logging.warning(f"[WEBHOOK] tool-call {name} recebido para call_id={call_id}")
                    try:
                        call_res = supabase.table("campaign_calls").select("*").eq("vapi_call_id", call_id).execute()
                        if call_res.data:
                            c_row = call_res.data[0]
                            deb_data = c_row.get("debito_data") or {}
                            deb_data["acordo_confirmado_tool"] = True
                            supabase.table("campaign_calls").update({
                                "debito_data": deb_data
                            }).eq("vapi_call_id", call_id).execute()
                            logging.warning(f"[WEBHOOK] flag acordo_confirmado_tool salva para call_id={call_id}")

                            _dispatch_task(formalizar_acordo, args=[{
                                "cpf":              c_row.get("cpf", ""),
                                "nome":             c_row.get("name", ""),
                                "email":            deb_data.get("email", ""),
                                "phone":            c_row.get("phone") or c_row.get("customer_number") or "",
                                "instituicao":      deb_data.get("instituicao", ""),
                                "idcalc":           deb_data.get("idcalc", ""),
                                "debito":           deb_data,
                                "valor":            deb_data.get("PgtoAvista", {}).get("ValorFinal", "0,00"),
                                "forma_pagamento":  "À vista",
                                "vapi_call_id":     call_id,
                                "campaign_call_id": c_row["id"],
                            }])
                            logging.warning(f"[WEBHOOK] formalizar_acordo disparado em tempo real para call_id={call_id}")
                            _end_vapi_call(call_id)
                    except Exception as e:
                        logging.error(f"[WEBHOOK] erro ao salvar/disparar acordo: {e}")
                    results.append({
                        "toolCallId": tc_id,
                        "result": {
                            "status": "success",
                            "valid": True,
                            "result": "ACORDO FORMALIZADO COM SUCESSO. Diga: 'Acordo formalizado com sucesso. O comprovante e os dados de pagamento estarão disponíveis em alguns minutos no seu WhatsApp e e-mail. Obrigada pela atenção, até mais.' e encerre a chamada imediatamente.",
                            "resultado": "ACORDO FORMALIZADO COM SUCESSO.",
                            "endCall": True
                        }
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

        fresh_res = supabase.table("campaign_calls").select("*").eq("id", row["id"]).execute()
        fresh_row = fresh_res.data[0] if fresh_res.data else row
        debito_data_updated = fresh_row.get("debito_data") or {}
        acordo_pela_tool = debito_data_updated.get("acordo_confirmado_tool") is True

        if not acordo_pela_tool:
            analysis_tools = msg.get("analysis", {}).get("toolCalls") or body.get("analysis", {}).get("toolCalls") or []
            for t_call in analysis_tools:
                if t_call.get("function", {}).get("name") in ("confirmar_acordo", "formalizar_acordo"):
                    acordo_pela_tool = True
                    break

        is_voicemail = _is_voicemail_or_self_talk(transcript, ended_reason)
        # Se a tool de acordo foi chamada em tempo real, NÃO dispara novamente no end-of-call para evitar duplicidade
        if acordo_pela_tool:
            logging.info(f"[WEBHOOK] Acordo já formalizado em tempo real via Tool Call para call_id={call_id}. Ignorando disparo redundante.")
        elif not is_voicemail and _detectar_acordo_formalizado(transcript):
            logging.warning(f"[WEBHOOK] ACORDO DETECTADO VIA TEXTO NO FIM DA CHAMADA para call_id={call_id}")
            try:
                _dispatch_task(formalizar_acordo, args=[{
                    "cpf":              fresh_row.get("cpf", ""),
                    "nome":             fresh_row.get("name", ""),
                    "email":            debito_data_updated.get("email", ""),
                    "phone":            fresh_row.get("phone") or fresh_row.get("customer_number") or "",
                    "instituicao":      debito_data_updated.get("instituicao", ""),
                    "idcalc":           debito_data_updated.get("idcalc", ""),
                    "debito":           debito_data_updated,
                    "valor":            _extrair_valor(summary) or _extrair_valor(transcript),
                    "forma_pagamento":  _extrair_forma_pagamento(transcript),
                    "vapi_call_id":     call_id,
                    "campaign_call_id": fresh_row["id"],
                }])
            except Exception as e:
                logging.warning(f"[WEBHOOK] erro ao enfileirar formalizar_acordo: {e}")
        elif is_voicemail:
            logging.warning(f"[WEBHOOK] ACORDO IGNORADO POR VOICEMAIL/FALA SOZINHA (ended_reason={ended_reason}) para call_id={call_id}")

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
                    _dispatch_task(fill_campaign_capacity_task, args=[campaign_id])

        return jsonify({"ok": True}), 200
    except Exception as e:
        logging.warning(f"[WEBHOOK] ERRO: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ── IMPORT: Upload Local / Async ───────────────────────────────

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

        upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, f"{file_id}_{fname}")
        with open(file_path, "wb") as f:
            f.write(file_bytes)

        job = supabase.table("import_jobs").insert({
            "status":    "processing",
            "total":     0,
            "with_debt": 0,
            "processed": 0,
            "result":    {"filename": fname, "storage_path": file_path},
        }).execute().data[0]


        _dispatch_task(process_file, args=[job["id"], file_path, fname])

        return jsonify({"ok": True, "mode": "async", "job_id": job["id"]})


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

        _dispatch_task(process_import_from_storage, args=[job["id"], storage_path, fname])
        return jsonify({"ok": True, "job_id": job["id"], "status": "queued"})

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
            "error":      job.get("error"),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500



_cached_payload = {}
_cached_payload_ts = 0

@app.route("/api/stream")
def stream():
    token = _bearer_token()
    if API_AUTH_TOKEN and not _valid_secret(token, API_AUTH_TOKEN):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    def _build_payload():
        global _cached_payload, _cached_payload_ts
        if _cached_payload and (time.time() - _cached_payload_ts) < 3:
            return _cached_payload
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

        _cached_payload = payload
        _cached_payload_ts = time.time()
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
