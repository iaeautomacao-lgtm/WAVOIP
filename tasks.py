import os
import re
import io
import time
import uuid
import json
import requests
import threading
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
from concurrent.futures import ThreadPoolExecutor, as_completed
from celery import Celery
from supabase import create_client
from typing import Optional, Dict, Any
from ddm import processar_debito as _processar_debito, processar_debito_result as _processar_debito_result



def env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip().strip('"').strip("'") if isinstance(value, str) else value


REDIS_URL            = env("REDIS_URL", "redis://localhost:6379/0")
VAPI_API_KEY         = env("VAPI_API_KEY")
VAPI_ASSISTANT_ID    = env("VAPI_ASSISTANT_ID")
VAPI_BASE            = "https://api.vapi.ai"
WAVOIP_EMAIL         = env("WAVOIP_EMAIL")
WAVOIP_PASSWORD      = env("WAVOIP_PASSWORD")
WAVOIP_BASE          = "https://api.wavoip.com"
SUPABASE_URL         = env("SUPABASE_URL")
SUPABASE_KEY         = env("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = env("SUPABASE_SERVICE_KEY", SUPABASE_KEY)
SUPABASE_BUCKET      = env("SUPABASE_BUCKET", "imports")

# ── Email (SMTP) ──────────────────────────────────────────────
SMTP_HOST     = env("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(env("SMTP_PORT", "587"))
SMTP_USER     = env("SMTP_USER")
SMTP_PASSWORD = env("SMTP_PASSWORD")
SMTP_FROM     = env("SMTP_FROM", "atendimento@ddm.adv.br")
SMTP_TIMEOUT  = int(env("SMTP_TIMEOUT", "10"))
SMTP_SECURITY = env("SMTP_SECURITY", "starttls").lower()
SMTP_FORCE_IPV4 = env("SMTP_FORCE_IPV4", "true").lower() in ("1", "true", "yes", "sim")

# ── DDM Acordos ───────────────────────────────────────────────
DDM_TOKEN    = env("DDM_TOKEN")
DDM_AGREEMENT_TOKEN = env("DDM_AGREEMENT_TOKEN", "")
DDM_BASE     = "https://ddmacordos.com"
DDM_TIMEOUT_SECONDS = int(env("DDM_TIMEOUT_SECONDS", "20"))
DDM_IMPORT_CONCURRENCY = max(1, int(env("DDM_IMPORT_CONCURRENCY", "10")))
DDM_IMPORT_RATE_PER_SEC = max(0.1, float(env("DDM_IMPORT_RATE_PER_SEC", "5")))
DDM_IMPORT_PROGRESS_EVERY = max(1, int(env("DDM_IMPORT_PROGRESS_EVERY", "25")))
DDM_IMPORT_DB_PROGRESS_EVERY = max(1, int(env("DDM_IMPORT_DB_PROGRESS_EVERY", "250")))
DDM_IMPORT_RETRIES = max(0, int(env("DDM_IMPORT_RETRIES", "2")))
DDM_ERROR_RECHECK_ROUNDS = max(0, int(env("DDM_ERROR_RECHECK_ROUNDS", "0")))
DDM_ERROR_RECHECK_DELAY_SECONDS = max(0, int(env("DDM_ERROR_RECHECK_DELAY_SECONDS", "60")))
DDM_ERROR_RECHECK_CONCURRENCY = max(1, int(env("DDM_ERROR_RECHECK_CONCURRENCY", "4")))
IMPORT_REDIS_TTL_SECONDS = max(3600, int(env("IMPORT_REDIS_TTL_SECONDS", "86400")))

supabase       = create_client(SUPABASE_URL, SUPABASE_KEY)
supabase_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

WATCHDOG_TIMEOUT_MIN = int(os.getenv("WATCHDOG_TIMEOUT_MIN", "8"))   # minutos sem webhook → re-dispara
WATCHDOG_MAX_RETRIES = int(os.getenv("WATCHDOG_MAX_RETRIES", "3"))    # tentativas antes de marcar erro
LINE_MAX_CONCURRENT = int(env("LINE_MAX_CONCURRENT", env("SIP_MAX_CONCURRENT", "2")))
LINE_COOLDOWN_SECONDS = int(env("LINE_COOLDOWN_SECONDS", "120"))
ACTIVE_CALL_STATUSES = ["enfileirado", "em_andamento", "atendido"]

celery = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)
celery.conf.update(
    task_serializer                    = "json",
    result_serializer                  = "json",
    accept_content                     = ["json"],
    task_acks_late                     = True,
    task_reject_on_worker_lost         = True,
    worker_prefetch_multiplier         = 1,
    broker_connection_retry_on_startup = True,
    # Watchdog roda a cada 2 minutos via Celery Beat
    beat_schedule = {
        "campaign-watchdog": {
            "task":     "tasks.campaign_watchdog",
            "schedule": 120,  # segundos
        },
    },
)

WAVOIP_VAPI_MAP = {
    "ed49616d-fb19-46df-96b8-decab4cde3cf": env("VAPI_PHONE_NUMBER_ID_1"),
    "c8af8686-ca83-4eef-b757-f53940426011": env("VAPI_PHONE_NUMBER_ID_2"),
    "88b232ad-5c1d-404f-8652-f5399e6a6f51": env("VAPI_PHONE_NUMBER_ID_3"),
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43": env("VAPI_PHONE_NUMBER_ID_4"),
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb": env("VAPI_PHONE_NUMBER_ID_5"),
}

DEVICE_PRIORITY = [
    "ed49616d-fb19-46df-96b8-decab4cde3cf",
    "c8af8686-ca83-4eef-b757-f53940426011",
    "88b232ad-5c1d-404f-8652-f5399e6a6f51",
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43",
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb",
]

_wavoip_token    = None
_wavoip_token_ts = 0
TOKEN_TTL        = 600
_rr_idx          = 0


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


def _line_overrides_map() -> dict:
    try:
        rows = supabase.table("line_overrides").select("*").execute().data or []
        return {r.get("line_token"): r for r in rows if r.get("line_token")}
    except Exception:
        return {}


_redis_client = None


def redis_client():
    global _redis_client
    if _redis_client is None:
        import redis as redis_lib
        _redis_client = redis_lib.from_url(REDIS_URL)
    return _redis_client


def _import_state_key(job_id: str) -> str:
    return f"import_job:{job_id}"


def _import_rows_key(job_id: str) -> str:
    return f"import_job:{job_id}:rows"


def _set_import_state(job_id: str, state: dict):
    try:
        redis_client().setex(
            _import_state_key(job_id),
            IMPORT_REDIS_TTL_SECONDS,
            json.dumps(state, ensure_ascii=False),
        )
    except Exception:
        pass


def _get_import_state(job_id: str) -> dict:
    try:
        raw = redis_client().get(_import_state_key(job_id))
        if not raw:
            return {}
        return json.loads(raw)
    except Exception:
        return {}


def _set_import_rows(job_id: str, rows: list):
    try:
        redis_client().setex(
            _import_rows_key(job_id),
            IMPORT_REDIS_TTL_SECONDS,
            json.dumps(rows, ensure_ascii=False),
        )
    except Exception:
        pass


def _get_import_rows(job_id: str) -> list:
    try:
        raw = redis_client().get(_import_rows_key(job_id))
        if not raw:
            return []
        rows = json.loads(raw)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def _delete_import_state(job_id: str):
    try:
        redis_client().delete(_import_state_key(job_id), _import_rows_key(job_id))
    except Exception:
        pass


def _line_name(line: dict) -> str:
    return str(line.get("name") or line.get("phone") or line.get("token") or "linha")


def _line_phone_id(line: dict) -> str:
    return WAVOIP_VAPI_MAP.get(line.get("token")) or ""


def _line_cooldown_key(line_token: str) -> str:
    return f"dialer:line_cooldown:{line_token}"


def _is_line_in_cooldown(line_token: str) -> bool:
    try:
        return bool(redis_client().exists(_line_cooldown_key(line_token)))
    except Exception:
        return False


def _cooldown_line(line_token: str, reason: str = ""):
    if not line_token:
        return
    try:
        redis_client().setex(_line_cooldown_key(line_token), LINE_COOLDOWN_SECONDS, reason or "call_failed")
    except Exception:
        pass


def get_healthy_lines() -> list:
    token      = wavoip_login()
    devices    = wavoip_get_devices(token)
    device_map = {d.get("token"): d for d in devices}
    overrides  = _line_overrides_map()
    healthy    = []
    for t in DEVICE_PRIORITY:
        if t not in device_map:         continue
        if (overrides.get(t) or {}).get("paused"): continue
        d = device_map[t]
        if d.get("status") != "open":  continue
        if d.get("disabled") != 0:     continue
        if d.get("phone") is None:     continue
        if not WAVOIP_VAPI_MAP.get(t): continue
        if _is_line_in_cooldown(t):     continue
        healthy.append(d)
    return healthy


def _dialer_meta(line: dict) -> dict:
    return {
        "line_token":      line.get("token") or "",
        "line_name":       _line_name(line),
        "phone_number_id": _line_phone_id(line),
        "reserved_at":     int(time.time()),
        "max_concurrent":  LINE_MAX_CONCURRENT,
    }


def _with_dialer_meta(debito: dict, line: dict) -> dict:
    base = dict(debito or {})
    base["_dialer"] = _dialer_meta(line)
    return base


def _active_line_counts(line_tokens: list) -> dict:
    counts = {token: 0 for token in line_tokens}
    if not line_tokens:
        return counts

    try:
        rows = supabase.table("campaign_calls") \
            .select("id, status, line_token, debito_data") \
            .in_("status", ACTIVE_CALL_STATUSES) \
            .execute().data or []
    except Exception:
        try:
            rows = supabase.table("campaign_calls") \
                .select("id, status, debito_data") \
                .in_("status", ACTIVE_CALL_STATUSES) \
                .execute().data or []
        except Exception:
            return counts

    unknown_active = 0
    for row in rows:
        debito = row.get("debito_data") or {}
        meta = debito.get("_dialer") if isinstance(debito, dict) else None
        token = row.get("line_token") or (meta or {}).get("line_token")
        if token in counts:
            counts[token] += 1
        elif not token:
            unknown_active += 1

    for idx in range(unknown_active):
        token = line_tokens[idx % len(line_tokens)]
        counts[token] += 1
    return counts


def _campaign_line_tokens(camp: dict) -> list:
    raw = camp.get("line_tokens") or []
    if isinstance(raw, str):
        raw = [x.strip() for x in raw.split(",") if x.strip()]
    if not isinstance(raw, list):
        return []
    allowed = [str(x).strip() for x in raw if str(x).strip()]
    known = set(DEVICE_PRIORITY)
    return [token for token in allowed if token in known]


def _filter_campaign_lines(healthy: list, camp: dict) -> list:
    allowed = _campaign_line_tokens(camp)
    if not allowed:
        return healthy
    allowed_set = set(allowed)
    return [line for line in healthy if line.get("token") in allowed_set]


def _available_line_slots(healthy: list, counts: dict) -> list:
    slots = []
    added = {line.get("token"): 0 for line in healthy}
    for _ in range(max(1, LINE_MAX_CONCURRENT)):
        for line in healthy:
            token = line.get("token")
            if not token:
                continue
            used = counts.get(token, 0) + added.get(token, 0)
            if used < LINE_MAX_CONCURRENT:
                slots.append(line)
                added[token] = added.get(token, 0) + 1
    return slots


def _acquire_scheduler_lock() -> tuple:
    token = str(uuid.uuid4())
    try:
        ok = redis_client().set("dialer:scheduler_lock", token, nx=True, ex=30)
        return bool(ok), token
    except Exception:
        return True, token


def _release_scheduler_lock(token: str):
    try:
        r = redis_client()
        if r.get("dialer:scheduler_lock") == token.encode():
            r.delete("dialer:scheduler_lock")
    except Exception:
        pass


def vapi_call(
    phone: str,
    cpf: str = "",
    name: str = "",
    debito: dict = None,
    line_token: str = "",
    phone_number_id: str = "",
    line_name: str = "",
) -> Dict:
    phone_e164 = _phone_e164(phone)
    digits = re.sub(r"\D", "", phone_e164)
    if len(digits) < 12:
        raise Exception(f"telefone invalido: {phone}")

    healthy = get_healthy_lines()
    if not healthy:
        raise Exception("Nenhuma linha disponível")

    if line_token and phone_number_id:
        healthy_map = {line.get("token"): line for line in healthy}
        line = healthy_map.get(line_token)
        if not line:
            raise Exception(f"Linha reservada indisponivel: {line_name or line_token}")
        candidates = [line]
    else:
        global _rr_idx
        start    = _rr_idx % len(healthy)
        _rr_idx += 1
        candidates = [healthy[(start + i) % len(healthy)] for i in range(len(healthy))]

    last_err = None

    for line in candidates:
        phone_id = phone_number_id or WAVOIP_VAPI_MAP.get(line.get("token"))
        if not phone_id:
            continue

        payload: Dict[str, Any] = {
            "phoneNumberId": phone_id,
            "assistantId":   VAPI_ASSISTANT_ID,
            "customer":      {"number": phone_e164, "name": name},
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
                }
            }

        try:
            r = requests.post(f"{VAPI_BASE}/call/phone", json=payload,
                headers={
                    "Authorization": f"Bearer {VAPI_API_KEY}",
                    "Content-Type":  "application/json"
                }, timeout=15)
            if r.ok:
                data = r.json()
                if isinstance(data, dict):
                    data["_dialer"] = {
                        "line_token":      line.get("token") or line_token,
                        "line_name":       _line_name(line) or line_name,
                        "phone_number_id": phone_id,
                    }
                return data
            try:    detail = r.json()
            except: detail = r.text
            last_err = f"{line.get('name')} — {r.status_code}: {detail}"
        except Exception as ex:
            last_err = f"{line.get('name')} — {str(ex)}"

    raise Exception(f"Todas as linhas falharam: {last_err}")


# ── HELPERS ───────────────────────────────────────────────────

def _norm_cpf(cpf) -> str:
    try:
        s = re.sub(r'\D', '', str(cpf))
    except Exception:
        s = ''
    return s.zfill(11) if s else ""


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


def _first_phone(row, cols: list) -> str:
    """Busca o primeiro telefone válido com variações comuns de planilhas legadas."""
    candidates = [
        "fone", "fone1", "fone2", "fone3",
        "tel", "telefone", "telefone1", "telefone2",
        "celular", "celular1", "celular2",
        "phone", "phone1", "phone2",
        "contato", "whatsapp", "whats", "cel",
    ]
    cols_lower = {c.lower(): c for c in cols}
    for cand in candidates:
        key = cols_lower.get(cand.lower())
        if key is None:
            continue
        val = _normalize_phone(row.get(key, ""))
        if val and val.lower() != "nan":
            return val
    return ""


def _first_phone_hires(row, cols: list) -> str:
    """Mesmo acesso do parser principal, mantendo nome compatível para o backend."""
    return _first_phone(row, cols)


def _all_phones_ddm(row, cols: list) -> list:
    """Extrai telefones para planilha DDM ou qualquer coluna que pareça DDM."""
    phones = []
    for i in range(1, 11):
        for key in (f"fone{i}", f"telefone{i}", f"tel{i}", f"phone{i}"):
            if key in [c.lower() for c in cols]:
                val = _normalize_phone(row.get(key, ""))
                if val and val != "nan" and val not in phones:
                    phones.append(val)
    return phones


def _update_result(row_id, status, call_id, phone, error=None):
    try:
        update = {"status": status}
        if call_id: update["vapi_call_id"] = call_id
        if phone:   update["phone"]         = phone
        if error:   update["error"]         = error
        supabase.table("campaign_calls").update(update).eq("id", row_id).execute()
    except Exception:
        pass


def _update_job_progress(job_id: str, total: int, processed: int, with_debt: int):
    try:
        supabase.table("import_jobs").update({
            "total":     total,
            "processed": processed,
            "with_debt": with_debt,
        }).eq("id", job_id).execute()
    except Exception:
        pass


def _set_job_error(job_id: str, error_msg: str):
    try:
        supabase.table("import_jobs").update({
            "status": "error",
            "result": {"error": error_msg},
        }).eq("id", job_id).execute()
    except Exception:
        pass
    _delete_import_state(job_id)


# ── TASKS ─────────────────────────────────────────────────────

def _retry_or_fail_call(row_id: str, phone: str, error: str) -> str:
    try:
        row = supabase.table("campaign_calls") \
            .select("watchdog_retries") \
            .eq("id", row_id) \
            .execute().data
        retries = (row[0].get("watchdog_retries") or 0) if row else 0
        if retries >= WATCHDOG_MAX_RETRIES:
            _update_result(row_id, "erro", None, phone, error)
            return "erro"

        supabase.table("campaign_calls").update({
            "status": "pendente",
            "watchdog_retries": retries + 1,
            "error": error,
        }).eq("id", row_id).execute()
        return "pendente"
    except Exception:
        _update_result(row_id, "erro", None, phone, error)
        return "erro"


@celery.task(bind=True, max_retries=0, name="tasks.make_call")
def make_call_task(self, campaign_id: str, contact: dict):
    cpf    = contact.get("cpf", "")
    name   = contact.get("name", "")
    row_id = contact.get("row_id")
    debito = contact.get("debito_data") or {}
    meta = debito.get("_dialer") if isinstance(debito, dict) else {}
    line_token = contact.get("line_token") or (meta or {}).get("line_token") or ""
    line_name = contact.get("line_name") or (meta or {}).get("line_name") or ""
    phone_number_id = contact.get("phone_number_id") or (meta or {}).get("phone_number_id") or ""

    # Monta lista de telefones a tentar em ordem:
    # 1. all_phones do debito_data (FONE1..FONE10 da planilha DDM)
    # 2. phone principal do contato como fallback
    all_phones = debito.get("all_phones") or []
    phone_main = contact.get("phone", "").strip()

    # Garante que phone_main esta na lista sem duplicar
    phones = list(dict.fromkeys(p for p in all_phones + [phone_main] if p and str(p).strip()))

    if not phones:
        _update_result(row_id, "sem_telefone", None, None)
        return

    # Sem linha reservada, mantem compatibilidade com chamadas antigas/manuais.
    if not line_token:
        MAX_WAIT  = 30 * 60
        CHECK_INT = 30
        waited    = 0
        while waited < MAX_WAIT:
            try:
                if get_healthy_lines():
                    break
            except Exception:
                pass
            time.sleep(CHECK_INT)
            waited += CHECK_INT
        else:
            _update_result(row_id, "falha_sem_linha", None, None)
            return

    # Tenta cada telefone em sequencia ate um funcionar
    last_error = None
    for phone in phones:
        phone = str(phone).strip()
        if not phone:
            continue
        try:
            data    = vapi_call(
                phone,
                cpf=cpf,
                name=name,
                debito=debito,
                line_token=line_token,
                phone_number_id=phone_number_id,
                line_name=line_name,
            )
            call_id = data.get("id")
            # Sucesso — registra o telefone que funcionou e encerra
            _update_result(row_id, "em_andamento", call_id, phone)
            return
        except Exception as ex:
            last_error = f"{phone}: {str(ex)}"
            # Pausa curta entre tentativas
            time.sleep(3)
            continue

    # Todos os telefones falharam
    error = f"todos os fones falharam - {last_error}"
    if line_token:
        _cooldown_line(line_token, error)
        status = _retry_or_fail_call(row_id, phones[0], error)
        if status == "pendente":
            fill_campaign_capacity_task.delay(campaign_id)
    else:
        _update_result(row_id, "erro", None, phones[0], error)


def _merge_debito_with_import_meta(row: dict, debito: dict) -> dict:
    merged = dict(debito or {})
    imported = row.get("debito_data") if isinstance(row.get("debito_data"), dict) else {}
    for key in ("all_phones", "email", "imported_id", "iddev", "idcrm", "uf", "cidade", "raw_phone_fallback"):
        if imported.get(key) and not merged.get(key):
            merged[key] = imported.get(key)
    return merged


def _validate_debt_before_call(row: dict) -> dict:
    cpf = row.get("cpf", "")
    if not cpf:
        supabase.table("campaign_calls").update({
            "status": "sem_debito",
            "error": "ddm: cpf vazio",
        }).eq("id", row["id"]).execute()
        return {"ok": False, "reason": "cpf vazio"}

    result = _processar_debito_result(cpf)
    if not result.get("ok"):
        supabase.table("campaign_calls").update({
            "status": "erro",
            "error": f"ddm: {result.get('error', 'erro DDM')}",
        }).eq("id", row["id"]).execute()
        return {"ok": False, "reason": result.get("error", "erro DDM")}

    debito = result.get("debito")
    if not debito:
        supabase.table("campaign_calls").update({
            "status": "sem_debito",
            "error": "ddm: sem debito antes da chamada",
        }).eq("id", row["id"]).execute()
        return {"ok": False, "reason": "sem debito"}

    return {"ok": True, "debito": _merge_debito_with_import_meta(row, debito)}


def fill_campaign_capacity(campaign_id: str) -> dict:
    locked, lock_token = _acquire_scheduler_lock()
    if not locked:
        return {"ok": True, "locked": True, "fired": 0}

    try:
        camp_rows = supabase.table("campaigns") \
            .select("*") \
            .eq("id", campaign_id) \
            .execute().data or []
        if not camp_rows:
            return {"ok": False, "error": "campanha nao encontrada", "fired": 0}

        camp = camp_rows[0]
        if camp.get("status") != "em_andamento":
            return {"ok": True, "status": camp.get("status"), "fired": 0}

        healthy = _filter_campaign_lines(get_healthy_lines(), camp)
        line_tokens = [line.get("token") for line in healthy if line.get("token")]
        counts = _active_line_counts(line_tokens)
        slots = _available_line_slots(healthy, counts)
        capacity = len(healthy) * LINE_MAX_CONCURRENT
        active = sum(counts.values())

        if not slots:
            return {
                "ok": True,
                "fired": 0,
                "capacity": capacity,
                "active": active,
                "healthy_lines": len(healthy),
                "line_max_concurrent": LINE_MAX_CONCURRENT,
                "selected_lines": len(_campaign_line_tokens(camp)) or len(healthy),
            }

        pending_limit = max(len(slots) * 20, 50)
        pending = supabase.table("campaign_calls") \
            .select("id, cpf, phone, name, debito_data, order_idx, watchdog_retries") \
            .eq("campaign_id", campaign_id) \
            .eq("status", "pendente") \
            .order("order_idx") \
            .limit(pending_limit) \
            .execute().data or []

        fired = 0
        skipped = 0
        slot_index = 0
        for row in pending:
            if slot_index >= len(slots):
                break

            validation = _validate_debt_before_call(row)
            if not validation.get("ok"):
                skipped += 1
                continue

            line = slots[slot_index]
            slot_index += 1
            debito = _with_dialer_meta(validation.get("debito") or {}, line)
            meta = debito["_dialer"]
            update = {
                "status": "enfileirado",
                "debito_data": debito,
                "line_token": meta["line_token"],
                "line_name": meta["line_name"],
                "phone_number_id": meta["phone_number_id"],
                "error": f"dialer: reservado {_line_name(line)}",
            }
            try:
                supabase.table("campaign_calls").update(update).eq("id", row["id"]).execute()
            except Exception:
                update.pop("line_token", None)
                update.pop("line_name", None)
                update.pop("phone_number_id", None)
                supabase.table("campaign_calls").update(update).eq("id", row["id"]).execute()

            make_call_task.delay(campaign_id, {
                "row_id":          row["id"],
                "cpf":             row.get("cpf", ""),
                "phone":           row.get("phone", ""),
                "name":            row.get("name", ""),
                "debito_data":     debito,
                "line_token":      meta["line_token"],
                "line_name":       meta["line_name"],
                "phone_number_id": meta["phone_number_id"],
            })
            fired += 1

        if fired:
            try:
                supabase.table("campaigns").update({
                    "fired": (camp.get("fired") or 0) + fired,
                    "updated_at": "now()",
                }).eq("id", campaign_id).execute()
            except Exception:
                supabase.table("campaigns").update({
                    "updated_at": "now()",
                }).eq("id", campaign_id).execute()
        elif skipped and len(pending) >= pending_limit:
            fill_campaign_capacity_task.apply_async(args=[campaign_id], countdown=2)
        elif skipped:
            _check_campaign_completion(campaign_id)
        elif not pending:
            _check_campaign_completion(campaign_id)

        return {
            "ok": True,
            "fired": fired,
            "skipped": skipped,
            "capacity": capacity,
            "active": active + fired,
            "healthy_lines": len(healthy),
            "line_max_concurrent": LINE_MAX_CONCURRENT,
            "selected_lines": len(_campaign_line_tokens(camp)) or len(healthy),
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "fired": 0}
    finally:
        _release_scheduler_lock(lock_token)


@celery.task(name="tasks.fill_campaign_capacity")
def fill_campaign_capacity_task(campaign_id: str):
    return fill_campaign_capacity(campaign_id)


@celery.task(name="tasks.campaign_watchdog")
def campaign_watchdog():
    """
    Roda a cada 2 minutos via Celery Beat.
    Detecta chamadas travadas (em_andamento/enfileirado sem webhook por N minutos)
    e re-dispara o proximo contato para desbloquear a campanha.

    Uma chamada e considerada travada quando:
      - status eh em_andamento ou enfileirado
      - updated_at e mais antigo que WATCHDOG_TIMEOUT_MIN minutos
    """
    from datetime import datetime, timezone, timedelta

    try:
        camps = supabase.table("campaigns") \
            .select("id, name, status, total, finished") \
            .eq("status", "em_andamento") \
            .execute().data

        if not camps:
            return {"checked": 0}

        cutoff  = (datetime.now(timezone.utc) - timedelta(minutes=WATCHDOG_TIMEOUT_MIN)).isoformat()
        rescued = 0
        errors  = 0

        for camp in camps:
            campaign_id = camp["id"]
            try:
                # Chamadas travadas (sem webhook no tempo limite)
                stuck = supabase.table("campaign_calls") \
                    .select("id, cpf, phone, name, debito_data, order_idx, watchdog_retries") \
                    .eq("campaign_id", campaign_id) \
                    .in_("status", ["em_andamento", "enfileirado", "atendido"]) \
                    .lt("updated_at", cutoff) \
                    .order("order_idx") \
                    .execute().data

                if not stuck:
                    # Campanha ativa mas nenhuma chamada em voo —
                    # webhook do ultimo contato pode ter se perdido
                    pending = supabase.table("campaign_calls") \
                        .select("id, cpf, phone, name, debito_data, order_idx, watchdog_retries") \
                        .eq("campaign_id", campaign_id) \
                        .eq("status", "pendente") \
                        .order("order_idx") \
                        .limit(1) \
                        .execute().data

                    if pending:
                        result = fill_campaign_capacity(campaign_id)
                        rescued += result.get("fired", 0)
                    else:
                        _check_campaign_completion(campaign_id)
                    continue

                for row in stuck:
                    retries = row.get("watchdog_retries") or 0
                    if retries >= WATCHDOG_MAX_RETRIES:
                        # Esgotou tentativas — marca erro e avanca
                        supabase.table("campaign_calls").update({
                            "status": "erro",
                            "error":  f"watchdog: {retries} tentativas sem resposta",
                        }).eq("id", row["id"]).execute()
                        errors += 1
                        _advance_campaign(campaign_id, row["order_idx"])
                    else:
                        _watchdog_dispatch(campaign_id, row, retries, reason="timeout_webhook")
                        rescued += 1

            except Exception:
                continue  # erro em uma campanha nao para o watchdog das outras

        return {"checked": len(camps), "rescued": rescued, "errors": errors}

    except Exception as e:
        return {"error": str(e)}


def _watchdog_dispatch(campaign_id: str, row: dict, current_retries: int, reason: str):
    """Libera uma chamada travada e deixa o scheduler reocupar a capacidade."""
    try:
        supabase.table("campaign_calls").update({
            "status":           "pendente",
            "watchdog_retries": current_retries + 1,
            "error":            f"watchdog/{reason} retry #{current_retries + 1}",
        }).eq("id", row["id"]).execute()

        fill_campaign_capacity_task.delay(campaign_id)
    except Exception:
        pass


def _advance_campaign(campaign_id: str, current_order_idx: int):
    """Reocupa slots livres apos erro/timeout."""
    try:
        result = fill_campaign_capacity(campaign_id)
        if not result.get("fired"):
            _check_campaign_completion(campaign_id)
    except Exception:
        pass


def _check_campaign_completion(campaign_id: str):
    """Verifica se todos os contatos foram processados e finaliza a campanha."""
    try:
        remaining = supabase.table("campaign_calls") \
            .select("id", count="exact") \
            .eq("campaign_id", campaign_id) \
            .in_("status", ["pendente", "enfileirado", "em_andamento", "atendido"]) \
            .execute()

        if (remaining.count or 0) == 0:
            supabase.table("campaigns").update({
                "status":     "finalizada",
                "updated_at": "now()",
            }).eq("id", campaign_id).execute()
    except Exception:
        pass



# ── ACORDO FORMALIZADO ────────────────────────────────────────

def _ddm_get_iddev(cpf: str) -> str:
    """Busca o iddev pelo CPF na API DDM."""
    cpf = _norm_cpf(cpf)
    r = requests.get(
        f"{DDM_BASE}/calc/localiza_dev.php",
        params={"tk": DDM_TOKEN, "cpf": cpf},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    # A DDM pode retornar um objeto ou uma lista com o devedor.
    return _ddm_find_first(data, {"idcalc", "iddev", "id"})


def _valid_email(value: str) -> str:
    value = str(value or "").strip()
    if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value):
        return value
    return ""


def _ddm_get_email(cpf: str, iddev: str = "") -> str:
    """Busca email no cadastro DDM por CPF/iddev, sem depender da planilha."""
    cpf = _norm_cpf(cpf)
    iddev = str(iddev or "").strip()
    email_keys = {"email", "emaildev", "emaildevedor", "mail"}

    if not DDM_TOKEN:
        return ""

    try:
        res = requests.get(
            f"{DDM_BASE}/calc/localiza_dev.php",
            params={"tk": DDM_TOKEN, "cpf": cpf},
            timeout=15,
        )
        res.raise_for_status()
        data = res.json()
        email = _valid_email(_ddm_find_first(data, email_keys))
        if email:
            return email
        iddev = iddev or _ddm_find_first(data, {"idcalc", "iddev", "id"})
    except Exception:
        pass

    if not iddev:
        return ""

    try:
        res = requests.get(
            f"{DDM_BASE}/calc/",
            params={"tk": DDM_TOKEN, "idDev": iddev, "cli": "ddm"},
            timeout=15,
        )
        res.raise_for_status()
        return _valid_email(_ddm_find_first(res.json(), email_keys))
    except Exception:
        return ""


def _ddm_find_first(obj, names: set) -> str:
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_norm = str(key).lower().replace("_", "").replace("-", "")
            if key_norm in names:
                if isinstance(value, str) and value.strip():
                    return value.strip()
                if isinstance(value, (int, float)) and value:
                    return str(value)
                if isinstance(value, dict):
                    nested = _ddm_find_first(value, {"link", "url", "href"})
                    if nested:
                        return nested
            nested = _ddm_find_first(value, names)
            if nested:
                return nested
    elif isinstance(obj, list):
        for item in obj:
            nested = _ddm_find_first(item, names)
            if nested:
                return nested
    return ""


def _ddm_get_payment_links(iddev: str) -> dict:
    """Busca links de boleto/pix pelo iddev."""
    r = requests.get(
        f"{DDM_BASE}/calc/",
        params={"tk": DDM_TOKEN, "idDev": iddev, "cli": "ddm"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    data_items = data if isinstance(data, list) else [data]

    link_boleto = ""
    link_pix    = ""
    linha_dig   = ""
    vencimento  = ""

    acordos_raw = []
    for item in data_items:
        if isinstance(item, dict) and item.get("Acordos") is not None:
            acordos_raw = item.get("Acordos") or []
            break
    if isinstance(acordos_raw, dict):
        acordos_raw = [acordos_raw]

    for ac in (acordos_raw or []):
        if isinstance(ac, list):
            for sub in ac:
                pag = sub.get("acordo_pagamento", {}) if isinstance(sub, dict) else {}
                link_boleto = link_boleto or str(pag.get("boleto") or "").strip()
                link_pix    = link_pix    or str(pag.get("pix")    or "").strip()
        elif isinstance(ac, dict):
            pag = ac.get("acordo_pagamento", {})
            link_boleto = link_boleto or str(pag.get("boleto") or "").strip()
            link_pix    = link_pix    or str(pag.get("pix")    or "").strip()

    # Fallback: campos diretos
    first_dict = next((item for item in data_items if isinstance(item, dict)), {})

    if not link_boleto:
        boleto_obj = first_dict.get("boleto", {})
        link_boleto = str(boleto_obj.get("link") or "").strip()
        linha_dig   = str(boleto_obj.get("linhaDigitavel") or "").strip()
        vencimento  = str(boleto_obj.get("vencimento") or "").strip()
    if not link_pix:
        pix_obj = first_dict.get("pix", {})
        link_pix = str(pix_obj.get("link") or "").strip()

    link_boleto = link_boleto or _ddm_find_first(data, {
        "boleto", "linkboleto", "urlboleto", "boletourl"
    })
    link_pix = link_pix or _ddm_find_first(data, {
        "pix", "linkpix", "urlpix", "pixurl", "qrcodepix", "qrcode"
    })
    linha_dig = linha_dig or _ddm_find_first(data, {
        "linhadigitavel", "linhadig", "digitalline", "digitableline"
    })
    vencimento = vencimento or _ddm_find_first(data, {
        "vencimento", "datavencimento", "duedate"
    })

    return {
        "link_boleto": link_boleto,
        "link_pix":    link_pix,
        "linha_dig":   linha_dig,
        "vencimento":  vencimento,
    }


def _ddm_formalizar_acordo(cpf: str) -> dict:
    """Formaliza acordo na DDM"""
    if not DDM_AGREEMENT_TOKEN:
        raise RuntimeError("DDM_AGREEMENT_TOKEN nao configurado")

    cpf = _norm_cpf(cpf)
    url = "https://www.ddmacordos.com/ws_ddm/ws/CalculaDebitos.php"
    r = requests.get(url, params={
        "tk": DDM_AGREEMENT_TOKEN,
        "OpcaoAcordo": "1",
        "TipoAcordo": "1",
        "Doc": cpf,
    }, timeout=30)
    r.raise_for_status()
    data = r.json()

    link_boleto = _ddm_find_first(data, {"linkboleto", "boletourl", "urlboleto"})
    link_pix    = _ddm_find_first(data, {"linkpix", "pixurl", "urlpix", "qrcodepix", "qrcode"})
    linha_dig   = _ddm_find_first(data, {"linhaboleto", "linhadigitavel", "linhadig", "digitalline", "digitableline"})
    vencimento  = _ddm_find_first(data, {"vencimento", "datavencimento", "duedate"})
    nr_acordo   = _ddm_find_first(data, {"nracordo", "numeroacordo", "acordo"})
    idcalc      = _ddm_find_first(data, {"idcalc", "calculoid"})
    nome        = _ddm_find_first(data, {"nomedev", "nomedevedor"})
    documento   = _ddm_find_first(data, {"documento", "cpf"})
    email       = _ddm_find_first(data, {"email"})

    return {
        "raw": data,
        "link_boleto": link_boleto,
        "link_pix": link_pix,
        "linha_dig": linha_dig,
        "vencimento": vencimento,
        "nr_acordo": nr_acordo,
        "idcalc": idcalc,
        "nome": nome,
        "cpf": documento,
        "email": email,
    }


def _enviar_email_acordo(
    destinatario: str,
    nome: str,
    instituicao: str,
    valor: str,
    forma_pagamento: str,
    link_boleto: str,
    link_pix: str,
    linha_dig: str,
    vencimento: str,
):
    """Envia email de formalização de acordo para o devedor."""
    import socket
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    link_principal = link_pix or link_boleto or ""

    # Monta seção de pagamento do email
    secao_pix = f"""
    <tr>
      <td style="padding:8px 0;">
        <b style="color:#101828;">Pix:</b><br>
        <a href="{link_pix}" style="color:#FF5706;">{link_pix}</a>
      </td>
    </tr>""" if link_pix else ""

    boleto_html = (
        f'<a href="{link_boleto}" style="color:#FF5706;">{link_boleto}</a>'
        if link_boleto else
        '<span style="color:#667085;">Link de boleto indisponivel.</span>'
    )
    secao_boleto = f"""
    <tr>
      <td style="padding:8px 0;">
        <b style="color:#101828;">Boleto:</b><br>
        {boleto_html}
        {f'<br><span style="color:#667085;font-size:13px;">Linha digitável: {linha_dig}</span>' if linha_dig else ""}
        {f'<br><span style="color:#667085;font-size:13px;">Vencimento: {vencimento}</span>' if vencimento else ""}
      </td>
    </tr>""" if (link_boleto or linha_dig) else ""

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<body style="margin:0;padding:0;background:#f4f6f8;font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:40px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 8px 24px rgba(0,0,0,0.08);">

<!-- Header -->
<tr><td style="background:#FF5706;padding:28px 36px;">
  <img src="https://assets.zyrosite.com/cdn-cgi/image/format=auto,w=375,h=152,fit=crop/m6L4ppGnqncBWn2J/ativo-16-YZ9a5WVxR2fx79Vd.png"
       alt="DDM" height="40" style="display:block;" />
</td></tr>

<!-- Body -->
<tr><td style="padding:32px 36px;">
  <p style="color:#101828;font-size:18px;font-weight:bold;margin:0 0 8px;">Acordo formalizado com sucesso!</p>
  <p style="color:#475467;font-size:15px;margin:0 0 24px;">Prezado(a) {nome},</p>
  <p style="color:#475467;font-size:14px;line-height:1.6;margin:0 0 24px;">
    Confirmamos a formalização do acordo referente à pendência financeira vinculada à
    <strong>{instituicao}</strong>, conforme tratado em nossa ligação.
  </p>

  <!-- Detalhes -->
  <table width="100%" cellpadding="0" cellspacing="0"
         style="background:#f9fafb;border-radius:10px;padding:20px;margin-bottom:24px;">
    <tr><td style="padding:6px 0;">
      <span style="color:#667085;font-size:13px;">Instituição</span><br>
      <strong style="color:#101828;">{instituicao}</strong>
    </td></tr>
    <tr><td style="padding:6px 0;">
      <span style="color:#667085;font-size:13px;">Forma de pagamento</span><br>
      <strong style="color:#101828;">{forma_pagamento}</strong>
    </td></tr>
    <tr><td style="padding:6px 0;">
      <span style="color:#667085;font-size:13px;">Valor acordado</span><br>
      <strong style="color:#101828;font-size:18px;">R$ {valor}</strong>
    </td></tr>
  </table>

  <!-- Links de pagamento -->
  {"<p style='color:#101828;font-weight:bold;margin:0 0 12px;'>Links para pagamento:</p><table width='100%' cellpadding='0' cellspacing='0'>" + secao_pix + secao_boleto + "</table>" if (link_pix or link_boleto or linha_dig) else ""}

  <p style="color:#475467;font-size:13px;margin:24px 0 0;line-height:1.6;">
    Qualquer dúvida, nossa equipe está à disposição.<br>
    <strong>Equipe de Atendimento – DDM</strong>
  </p>
</td></tr>

<tr><td style="background:#f9fafb;padding:20px 36px;text-align:center;">
  <p style="color:#98a2b3;font-size:12px;margin:0;">
    © DDM Assessoria | Este é um e-mail automático, não responda diretamente.
  </p>
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Acordo formalizado — {instituicao}"
    msg["From"]    = SMTP_FROM
    msg["To"]      = destinatario
    msg.attach(MIMEText(html, "html"))

    if SMTP_FORCE_IPV4 and SMTP_SECURITY != "ssl":
        class SMTPIPv4(smtplib.SMTP):
            def _get_socket(self, host, port, timeout):
                last_error = None
                for family, socktype, proto, _, sockaddr in socket.getaddrinfo(
                    host, port, socket.AF_INET, socket.SOCK_STREAM
                ):
                    sock = socket.socket(family, socktype, proto)
                    try:
                        sock.settimeout(timeout)
                        sock.connect(sockaddr)
                        return sock
                    except OSError as exc:
                        last_error = exc
                        sock.close()
                if last_error:
                    raise last_error
                raise OSError(f"Nenhum endereco IPv4 encontrado para {host}")

        srv = SMTPIPv4(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT)
    elif SMTP_SECURITY == "ssl":
        srv = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT)
    else:
        srv = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT)

    with srv:
        srv.ehlo()
        if SMTP_SECURITY == "starttls":
            srv.starttls()
            srv.ehlo()
        srv.login(SMTP_USER, SMTP_PASSWORD)
        srv.sendmail(SMTP_FROM, [destinatario], msg.as_string())


@celery.task(bind=True, max_retries=2, soft_time_limit=45, time_limit=60, name="tasks.formalizar_acordo")
def formalizar_acordo(self, dados: dict):
    """
    Disparada quando a Júlia formaliza um acordo.
    1. Busca iddev pelo CPF na API DDM
    2. Busca links de boleto/pix
    3. Envia email para o devedor
    4. Registra no Supabase
    """
    try:
        cpf             = dados.get("cpf", "")
        nome            = dados.get("nome", "Cliente")
        email           = dados.get("email", "")
        instituicao     = dados.get("instituicao", "")
        valor           = dados.get("valor", "")
        forma_pagamento = dados.get("forma_pagamento", "À vista")
        vapi_call_id    = dados.get("vapi_call_id", "")
        campaign_call_id = dados.get("campaign_call_id", "")
        debito          = dados.get("debito") or {}

        link_boleto = ""
        link_pix    = ""
        linha_dig   = ""
        vencimento  = ""
        nr_acordo   = ""
        idcalc       = str(
            dados.get("idcalc") or
            dados.get("iddev") or
            debito.get("idcalc") or
            debito.get("iddev") or
            ""
        ).strip()

        if cpf:
            try:
                acordo = _ddm_formalizar_acordo(cpf)
                link_boleto = acordo.get("link_boleto") or ""
                link_pix    = acordo.get("link_pix") or ""
                linha_dig   = acordo.get("linha_dig") or ""
                vencimento  = acordo.get("vencimento") or ""
                nr_acordo   = acordo.get("nr_acordo") or ""
                idcalc      = acordo.get("idcalc") or idcalc
                if acordo.get("email") and not email:
                    email = acordo["email"]
                if acordo.get("nome") and nome == "Cliente":
                    nome = acordo["nome"]
            except Exception as e:
                import logging
                logging.warning("[DDM] erro ao formalizar acordo cpf_final=%s erro=%s", _norm_cpf(cpf)[-4:], e)

        # 1. Busca iddev e links de pagamento na API DDM
        if cpf and not (link_boleto or link_pix):
            try:
                iddev = _ddm_get_iddev(cpf)
                if iddev:
                    idcalc = iddev
                    pagamentos = _ddm_get_payment_links(iddev)
                    link_boleto = pagamentos["link_boleto"]
                    link_pix    = pagamentos["link_pix"]
                    linha_dig   = pagamentos["linha_dig"]
                    vencimento  = pagamentos["vencimento"]
            except Exception as e:
                # Não bloqueia o envio do email se a API DDM falhar
                pass

        if cpf and not (link_boleto or link_pix):
            try:
                iddev = idcalc
                if not iddev:
                    debito_recalculado = _processar_debito(_norm_cpf(cpf))
                    if debito_recalculado:
                        iddev = str(debito_recalculado.get("idcalc") or "").strip()
                if iddev:
                    idcalc = iddev
                    pagamentos = _ddm_get_payment_links(iddev)
                    link_boleto = pagamentos["link_boleto"]
                    link_pix    = pagamentos["link_pix"]
                    linha_dig   = pagamentos["linha_dig"]
                    vencimento  = pagamentos["vencimento"]
                if not (link_boleto or link_pix):
                    import logging
                    logging.warning("[DDM] sem link de pagamento cpf_final=%s id=%s", _norm_cpf(cpf)[-4:], iddev)
            except Exception as e:
                import logging
                logging.warning("[DDM] erro no fallback de boleto cpf_final=%s erro=%s", _norm_cpf(cpf)[-4:], e)

        if cpf and not email:
            email = _ddm_get_email(cpf, idcalc)

        # 2. Envia email para o devedor
        email_enviado = False
        if email:
            try:
                _enviar_email_acordo(
                    destinatario    = email,
                    nome            = nome,
                    instituicao     = instituicao,
                    valor           = valor,
                    forma_pagamento = forma_pagamento,
                    link_boleto     = link_boleto,
                    link_pix        = link_pix,
                    linha_dig       = linha_dig,
                    vencimento      = vencimento,
                )
                email_enviado = True
            except Exception as e:
                import logging
                logging.error(f"[FORMALIZAR] erro SMTP: {e}")  # ADD ISSO

        # 3. Registra acordo no Supabase
        try:
            supabase.table("acordos_formalizados").insert({
                "cpf":             cpf,
                "nome":            nome,
                "email":           email,
                "instituicao":     instituicao,
                "valor":           valor,
                "forma_pagamento": forma_pagamento,
                "link_boleto":     link_boleto,
                "link_pix":        link_pix,
                "email_enviado":   email_enviado,
                "vapi_call_id":    vapi_call_id,
                "campaign_call_id": campaign_call_id,
            }).execute()
        except Exception:
            pass  # tabela pode não existir ainda

        return {
            "ok":            True,
            "email_enviado": email_enviado,
            "link_pix":      link_pix,
            "link_boleto":   link_boleto,
            "linha_boleto":  linha_dig,
            "nr_acordo":     nr_acordo,
        }

    except Exception as exc:
        raise self.retry(exc=exc, countdown=30)

@celery.task(bind=True, name="tasks.process_import")
def process_import(self, job_id: str, rows: list):
    processed = 0
    with_debt = 0
    result    = []

    for row in rows:
        cpf   = str(row.get("cpf", "")).strip()
        name  = str(row.get("name", "")).strip()
        phone = str(row.get("phone", "")).strip()
        debito = _processar_debito(cpf) if cpf else None

        result.append({
            "cpf":      cpf,
            "name":     name,
            "phone":    phone,
            "has_debt": debito is not None,
            "debito":   debito,
        })

        processed += 1
        if debito:
            with_debt += 1

        if processed % 10 == 0:
            try:
                supabase.table("import_jobs").update({
                    "processed": processed,
                    "with_debt": with_debt,
                }).eq("id", job_id).execute()
            except Exception:
                pass

    try:
        supabase.table("import_jobs").update({
            "status":    "done",
            "processed": processed,
            "with_debt": with_debt,
            "result":    result,
        }).eq("id", job_id).execute()
    except Exception:
        pass


@celery.task(bind=True, name="tasks.process_file")
def process_file(self, job_id: str, file_id: str, fname: str):
    """Task legada — lê do Redis, usada para arquivos pequenos."""
    import pandas as pd
    import redis as redis_lib

    try:
        r    = redis_lib.from_url(os.getenv("REDIS_URL"))
        data = r.get(f"import:{file_id}")
        if not data:
            _set_job_error(job_id, "Arquivo não encontrado no Redis (expirou?)")
            return

        buf = io.BytesIO(data)
        if fname.endswith(".csv"):
            df = pd.read_csv(buf, dtype=str, sep=None, engine="python")
        else:
            df = pd.read_excel(buf, dtype=str)

        r.delete(f"import:{file_id}")

        if len(df) > 20000:
            _set_job_error(job_id, f"Arquivo com {len(df)} linhas excede o limite. Use o upload via Storage para arquivos grandes.")
            return

        _process_dataframe(job_id, df, fname)

    except Exception as exc:
        _set_job_error(job_id, str(exc))


@celery.task(bind=True, name="tasks.process_import_from_storage", max_retries=3)
def process_import_from_storage(self, job_id: str, storage_path: str, fname: str = ""):
    """
    Task principal para arquivos grandes.
    Baixa do Supabase Storage e processa sem limite de tamanho.
    Sem timeout HTTP — roda inteiramente no container Celery.
    """
    import pandas as pd

    try:
        # Atualiza status para processing
        supabase.table("import_jobs").update({
            "status": "processing"
        }).eq("id", job_id).execute()

        # Baixa direto com service role. Evita erro 400 no endpoint de assinatura
        # quando a Storage ainda nao enxerga o objeto logo apos o upload.
        from urllib.parse import quote
        storage_host = SUPABASE_URL.rstrip("/")
        object_path = quote(storage_path.lstrip("/"), safe="/")
        download_urls = [
            f"{storage_host}/storage/v1/object/{SUPABASE_BUCKET}/{object_path}",
            f"{storage_host}/storage/v1/object/authenticated/{SUPABASE_BUCKET}/{object_path}",
        ]
        resp = None
        last_error = ""
        headers = {
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            "apikey": SUPABASE_SERVICE_KEY,
        }
        for attempt in range(5):
            for download_url in download_urls:
                resp = requests.get(download_url, headers=headers, timeout=300)
                if resp.ok:
                    break
                last_error = f"{resp.status_code} {resp.text[:300]}"
            if resp is not None and resp.ok:
                break
            if attempt < 4:
                time.sleep(2)
                continue
            import logging
            logging.warning(
                "[STORAGE_DOWNLOAD] path=%s bucket=%s key_len=%s service_key_len=%s service_starts_eyJ=%s erro=%s",
                storage_path,
                SUPABASE_BUCKET,
                len(SUPABASE_KEY or ""),
                len(SUPABASE_SERVICE_KEY or ""),
                str(SUPABASE_SERVICE_KEY or "").startswith("eyJ"),
                last_error,
            )
            resp.raise_for_status()

        if resp is None:
            raise Exception("Falha ao baixar arquivo do Supabase Storage")

        raw_bytes = resp.content
        if not raw_bytes:
            raise Exception("Arquivo baixado do Supabase Storage veio vazio")

        # Detecta encoding — planilha DDM geralmente vem em latin-1
        try:
            raw_bytes.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            encoding = "latin-1"

        buf = io.BytesIO(raw_bytes)
        fname_lower = (fname or storage_path).lower()

        if fname_lower.endswith(".xlsx") or fname_lower.endswith(".xls"):
            df = pd.read_excel(buf, dtype=str)
        else:
            df = pd.read_csv(buf, dtype=str, sep=None, engine="python", encoding=encoding)

        # Processa o dataframe — delete só acontece após sucesso completo
        _process_dataframe(job_id, df, fname_lower)

        # Remove arquivo do bucket APENAS após processamento bem-sucedido
        # (se deletar antes e o retry precisar do arquivo, vai quebrar)
        try:
            supabase_admin.storage.from_(SUPABASE_BUCKET).remove([storage_path])
        except Exception:
            pass  # não crítico

    except Exception as exc:
        if self.request.retries >= self.max_retries:
            _set_job_error(job_id, str(exc))
            raise
        raise self.retry(exc=exc, countdown=30)


def _process_dataframe(job_id: str, df, fname: str):
    """
    Padroniza a planilha para CPF + NOME + identificador + telefone,
    independentemente do layout original.
    """
    import pandas as pd

    df.columns = [str(c).strip() for c in df.columns]
    cols_lower = [c.lower() for c in df.columns]

    def first_col(candidates):
        for cand in candidates:
            for c, cl in zip(df.columns, cols_lower):
                if cand in cl:
                    return c
        return None

    cpf_col = first_col(["cpf", "cpfcgc"])
    nome_col = first_col(["nome", "name"])
    id_col = first_col(["matricula", "id", "externo", "cliente id", "cliente_id"])
    phone_col = first_col(
        ["fone", "fone1", "fone2", "fone3", "tel", "telefone", "celular", "phone", "contato", "whatsapp"]
    )
    val_col = first_col(["val_atualizado", "val_nominal"])
    is_ddm = val_col is not None

    if not cpf_col or not nome_col:
        _set_job_error(job_id, f"Colunas obrigatórias não encontradas. Colunas detectadas: {list(df.columns)}")
        return

    pending_rows = []
    for idx, r in df.iterrows():
        row = {str(k): ("" if pd.isna(v) else str(v).strip()) for k, v in r.items()}
        cpf_norm = _norm_cpf(row.get(cpf_col, ""))
        if not cpf_norm:
            continue

        nome = row.get(nome_col, "").strip() or ""
        if is_ddm:
            phones = _all_phones_ddm(row, list(df.columns))
            phone = phones[0] if phones else ""
            meta = {
                "planilha_carteira": row.get("carteira", "").strip(),
                "iddev": row.get("iddev", "").strip(),
                "idcrm": row.get("idcrm", "").strip(),
                "uf": row.get("uf", "").strip(),
                "cidade": row.get("cidade", "").strip(),
                "all_phones": phones,
                "email": row.get("email", "").strip(),
            }
            if phone_col and phone_col in row:
                meta["raw_phone_fallback"] = row.get(phone_col, "").strip()
        else:
            phone = _first_phone_hires(row, list(df.columns)) if phone_col else ""
            meta = {}

        if not phone:
            continue

        meta_id = ""
        if id_col and id_col in row:
            meta_id = row.get(id_col, "").strip()

        pending_rows.append({
            "idx": idx,
            "cpf": cpf_norm,
            "name": nome,
            "phone": phone,
            "has_debt": False,
            "debito": None,
            "ddm_error": "",
            "validation_status": "pending",
            "meta": {
                "imported_id": meta_id,
                **(meta if isinstance(meta, dict) else {}),
            },
        })

    _set_import_rows(job_id, pending_rows)
    _set_import_state(job_id, {
        "status": "done",
        "total": len(pending_rows),
        "processed": len(pending_rows),
        "with_debt": 0,
        "sample": pending_rows[:200],
    })

    try:
        supabase.table("import_jobs").update({
            "status": "done",
            "total": len(pending_rows),
            "processed": len(pending_rows),
            "with_debt": 0,
            "result": {
                "sample": pending_rows[:200],
                "source": "redis",
                "validation_mode": "before_call",
            },
        }).eq("id", job_id).execute()
    except Exception:
        pass


def _validate_import_rows(job_id: str):
    state = _get_import_state(job_id)
    pending_rows = _get_import_rows(job_id)
    job = supabase.table("import_jobs").select("*").eq("id", job_id).execute().data
    if not job:
        return
    job = job[0]
    result = job.get("result") or {}
    if not pending_rows and isinstance(state, dict):
        pending_rows = state.get("rows") or []
    if not pending_rows:
        pending_rows = result.get("rows") if isinstance(result, dict) else []
    if not pending_rows:
        supabase.table("import_jobs").update({
            "status":    "done",
            "total":     0,
            "processed": 0,
            "with_debt": 0,
            "result":    [],
        }).eq("id", job_id).execute()
        return

    total = len(pending_rows)
    processed = 0
    with_debt = 0
    results = []
    rate_lock = threading.Lock()
    next_call = {"at": 0.0}
    min_gap   = 1.0 / DDM_IMPORT_RATE_PER_SEC

    def validate_contact(item: dict) -> dict:
        with rate_lock:
            now = time.monotonic()
            wait = next_call["at"] - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            next_call["at"] = now + min_gap

        result = _processar_debito_result(item["cpf"])

        return {
            "idx":      item["idx"],
            "cpf":      item["cpf"],
            "name":     item["name"],
            "phone":    item["phone"],
            "debito":   result.get("debito"),
            "ddm_error": "" if result.get("ok") else result.get("error", "erro DDM"),
        }

    def merge_row_debito(item: dict, base_debito: dict) -> dict:
        if not base_debito:
            return None
        debito = dict(base_debito)
        meta = item.get("meta") or {}
        if meta:
            iddev_planilha = meta.get("iddev") or ""
            debito.update(meta)
            if iddev_planilha:
                debito["iddev"] = iddev_planilha
        return debito

    first_by_cpf = {}
    rows_by_cpf = {}
    for item in pending_rows:
        first_by_cpf.setdefault(item["cpf"], item)
        rows_by_cpf.setdefault(item["cpf"], []).append(item)

    unique_items = list(first_by_cpf.values())
    worker_count = min(DDM_IMPORT_CONCURRENCY, len(unique_items))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(validate_contact, item): item["cpf"] for item in unique_items}
        for future in as_completed(futures):
            row = future.result()
            validation = {
                "debito": row.get("debito"),
                "ddm_error": row.get("ddm_error", ""),
            }
            for item in rows_by_cpf.get(row["cpf"], []):
                debito = merge_row_debito(item, validation.get("debito"))
                result_row = {
                    "idx":      item["idx"],
                    "cpf":      item["cpf"],
                    "name":     item["name"],
                    "phone":    item["phone"],
                    "has_debt": debito is not None,
                    "debito":   debito,
                    "ddm_error": validation.get("ddm_error", ""),
                    "validation_status": "done" if validation.get("ddm_error", "") == "" else "error",
                }
                results.append(result_row)
                processed += 1
                if result_row["has_debt"]:
                    with_debt += 1

            if processed % DDM_IMPORT_PROGRESS_EVERY == 0 or processed == total:
                preview = sorted(results, key=lambda r: r["idx"])[:200]
                _set_import_state(job_id, {
                    "status": "validating",
                    "total": total,
                    "processed": processed,
                    "with_debt": with_debt,
                    "sample": preview,
                })

            if processed % DDM_IMPORT_DB_PROGRESS_EVERY == 0 or processed == total:
                preview = sorted(results, key=lambda r: r["idx"])[:200]
                try:
                    supabase.table("import_jobs").update({
                        "total":     total,
                        "processed": processed,
                        "with_debt": with_debt,
                        "result":    {
                            "sample": preview,
                            "source": "redis",
                        },
                    }).eq("id", job_id).execute()
                except Exception:
                    _update_job_progress(job_id, total, processed, with_debt)

    rows = sorted(results, key=lambda r: r["idx"])
    for row in rows:
        row.pop("idx", None)
        row.pop("meta", None)

    try:
        supabase.table("import_jobs").update({
            "status":    "done",
            "total":     total,
            "processed": total,
            "with_debt": with_debt,
            "result":    rows,
        }).eq("id", job_id).execute()
    except Exception:
        pass
    _delete_import_state(job_id)

    if DDM_ERROR_RECHECK_ROUNDS and any(row.get("ddm_error") for row in rows):
        recheck_import_errors_task.apply_async(
            args=[job_id, DDM_ERROR_RECHECK_ROUNDS],
            countdown=DDM_ERROR_RECHECK_DELAY_SECONDS,
            queue="imports",
        )


@celery.task(name="tasks.validate_import_ddm")
def validate_import_ddm_task(job_id: str):
    try:
        return _validate_import_rows(job_id)
    except Exception as exc:
        _set_job_error(job_id, f"Erro validando DDM: {exc}")
        raise


@celery.task(name="tasks.recheck_import_errors")
def recheck_import_errors_task(job_id: str, rounds_left: int = 1):
    if rounds_left <= 0:
        return {"ok": True, "rechecked": 0, "remaining_rounds": 0}

    job = supabase.table("import_jobs").select("*").eq("id", job_id).execute().data
    if not job:
        return {"ok": False, "error": "job nao encontrado"}

    rows = job[0].get("result") or []
    if not isinstance(rows, list):
        return {"ok": False, "error": "resultado do job nao e lista"}

    error_rows = [row for row in rows if row.get("ddm_error") and row.get("cpf")]
    if not error_rows:
        return {"ok": True, "rechecked": 0, "errors": 0}

    def recheck(row: dict) -> dict:
        result = {"ok": False, "error": "DDM nao consultada", "debito": None}
        for attempt in range(DDM_IMPORT_RETRIES + 1):
            result = _processar_debito_result(row["cpf"])
            if result.get("ok"):
                break
            if attempt < DDM_IMPORT_RETRIES:
                time.sleep(0.5 * (attempt + 1))
        return {
            "cpf": row["cpf"],
            "debito": result.get("debito"),
            "ddm_error": "" if result.get("ok") else result.get("error", "erro DDM"),
        }

    checked = {}
    worker_count = min(DDM_ERROR_RECHECK_CONCURRENCY, len(error_rows))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(recheck, row) for row in error_rows]
        for future in as_completed(futures):
            result = future.result()
            checked[result["cpf"]] = result

    updated = []
    for row in rows:
        result = checked.get(row.get("cpf"))
        if not result:
            updated.append(row)
            continue

        if result.get("ddm_error"):
            updated.append(row)
            continue

        debito = result.get("debito")
        updated.append({
            **row,
            "has_debt": debito is not None,
            "debito": debito,
            "ddm_error": "",
            "validation_status": "done",
        })

    with_debt = sum(1 for row in updated if row.get("has_debt"))
    remaining_errors = sum(1 for row in updated if row.get("ddm_error"))
    supabase.table("import_jobs").update({
        "with_debt": with_debt,
        "result": updated,
    }).eq("id", job_id).execute()

    if remaining_errors and rounds_left > 1:
        recheck_import_errors_task.apply_async(
            args=[job_id, rounds_left - 1],
            countdown=DDM_ERROR_RECHECK_DELAY_SECONDS,
            queue="imports",
        )

    return {"ok": True, "rechecked": len(error_rows), "errors": remaining_errors}
