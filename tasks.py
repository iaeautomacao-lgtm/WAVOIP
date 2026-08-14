# WAVOIP Async Tasks Module (Updated 23/07/2026 16:08)
import os
import re
import io
import time
import uuid
import random
import json
import requests
import threading
import logging
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    from celery import Celery
except ImportError:
    class Celery:
        def __init__(self, *args, **kwargs):
            self.conf = type("Conf", (), {"update": lambda self, **kw: None})()
        def task(self, *args, **kwargs):
            def decorator(f):
                return f
            return decorator

from mysql_adapter import create_client
from typing import Optional, Dict, Any
from ddm import processar_debito as _processar_debito, processar_debito_result as _processar_debito_result


logger = logging.getLogger(__name__)


def env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip().strip('"').strip("'") if isinstance(value, str) else value


_dispatched_threads = []
_dispatched_threads_lock = threading.Lock()

def wait_for_dispatched_tasks(timeout: float = 60.0):
    """Aguarda a conclusão de todas as threads locais disparadas nesta instância do runner."""
    import time
    start_time = time.time()
    while time.time() - start_time < timeout:
        with _dispatched_threads_lock:
            active = [t for t in _dispatched_threads if t.is_alive()]
        if not active:
            break
        time.sleep(0.5)

def _dispatch_task(task_obj, args=None, kwargs=None, countdown=0):
    """
    Executa tarefas de forma resiliente: tenta via Celery (se disponível)
    ou chaveia para threading.Thread / threading.Timer local se o Celery/Redis TCP não responder.
    """
    args = args or []
    kwargs = kwargs or {}
    countdown = max(0, int(countdown))

    # Tenta Celery apenas se ativado explicitamente
    use_celery = env("USE_CELERY", "false").lower() in ("true", "1", "sim")
    if use_celery:
        try:
            if hasattr(task_obj, "apply_async"):
                if countdown > 0:
                    return task_obj.apply_async(args=args, kwargs=kwargs, countdown=countdown)
                else:
                    return task_obj.apply_async(args=args, kwargs=kwargs)
        except Exception as e:
            logger.warning(f"[dispatch_task] Celery/Redis indisponível ({e}), chaveando para Thread local.")

    def runner():
        try:
            if hasattr(task_obj, "run"):
                import inspect
                try:
                    sig = inspect.signature(task_obj.run)
                    params = list(sig.parameters.keys())
                except Exception:
                    params = []
                if params and params[0] == "self" and len(args) < len(params):
                    task_obj.run(None, *args, **kwargs)
                else:
                    task_obj.run(*args, **kwargs)
            elif callable(task_obj):
                task_obj(*args, **kwargs)
        except Exception as ex:
            task_name = getattr(task_obj, "__name__", str(task_obj))
            logger.error(f"[dispatch_task] Erro ao executar {task_name} via Thread local: {ex}", exc_info=True)

    if countdown > 0:
        t = threading.Timer(float(countdown), runner)
        t.daemon = True
        t.start()
    else:
        t = threading.Thread(target=runner)
        t.daemon = True
        t.start()

    with _dispatched_threads_lock:
        _dispatched_threads.append(t)


#credenciais
REDIS_URL            = env("REDIS_URL", "redis://localhost:6379/0")
VAPI_API_KEY         = env("VAPI_API_KEY")
VAPI_ASSISTANT_ID    = env("VAPI_ASSISTANT_ID")
VAPI_ASSISTANT_ID_CRUZEIRO = env("VAPI_ASSISTANT_ID_CRUZEIRO", VAPI_ASSISTANT_ID)
VAPI_ASSISTANT_ID_DDM = env("VAPI_ASSISTANT_ID_DDM", VAPI_ASSISTANT_ID)
VAPI_BASE            = "https://api.vapi.ai"


def _get_assistant_id(debito: dict) -> str:
    if not isinstance(debito, dict):
        return VAPI_ASSISTANT_ID
    inst = str(debito.get("instituicao", "")).lower()
    if "cruzeiro" in inst:
        return VAPI_ASSISTANT_ID_CRUZEIRO
    # Se não for Cruzeiro, usa o do Veiga / DDM
    return VAPI_ASSISTANT_ID_DDM
WAVOIP_EMAIL         = env("WAVOIP_EMAIL")
WAVOIP_PASSWORD      = env("WAVOIP_PASSWORD")
WAVOIP_BASE          = "https://api.wavoip.com"

# ── Email (SMTP) ──────────────────────────────────────────────
SMTP_HOST     = env("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(env("SMTP_PORT", "587"))
SMTP_USER     = env("SMTP_USER")
SMTP_PASSWORD = env("SMTP_PASSWORD")
SMTP_FROM     = env("SMTP_FROM", "atendimento@ddm.adv.br")
SMTP_TIMEOUT  = max(30, int(env("SMTP_TIMEOUT", "30")))
SMTP_SECURITY = env("SMTP_SECURITY", "starttls").lower()
SMTP_FORCE_IPV4 = env("SMTP_FORCE_IPV4", "true").lower() in ("1", "true", "yes", "sim")
DEBUG_EMAIL_RECIPIENT = env("DEBUG_EMAIL_RECIPIENT", "")
DEBUG_PHONE_RECIPIENT = env("DEBUG_PHONE_RECIPIENT", "")
ADMIN_NOTIFY_EMAIL    = env("ADMIN_NOTIFY_EMAIL", DEBUG_EMAIL_RECIPIENT)
ADMIN_NOTIFY_PHONE    = env("ADMIN_NOTIFY_PHONE", DEBUG_PHONE_RECIPIENT)
N8N_WEBHOOK_URL       = env("N8N_WEBHOOK_URL", "")

# ── DDM Acordos ───────────────────────────────────────────────
DDM_TOKEN    = env("DDM_TOKEN")
DDM_TOKEN_BUSCA = env("DDM_TOKEN_BUSCA", "2e30b68c0feda298f9d6d40ab36c1a09")
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
BOLETO_RETRY_DELAY_SECONDS = max(60, int(env("BOLETO_RETRY_DELAY_SECONDS", "600")))
BOLETO_RETRY_MAX = max(1, int(env("BOLETO_RETRY_MAX", "12")))

supabase       = create_client()
supabase_admin = supabase

WATCHDOG_TIMEOUT_MIN = int(os.getenv("WATCHDOG_TIMEOUT_MIN", "8"))   # minutos sem webhook → re-dispara
WATCHDOG_MAX_RETRIES = int(os.getenv("WATCHDOG_MAX_RETRIES", "3"))    # tentativas antes de marcar erro
LINE_MAX_CONCURRENT = int(env("LINE_MAX_CONCURRENT", env("SIP_MAX_CONCURRENT", "1")))
LINE_COOLDOWN_SECONDS = int(env("LINE_COOLDOWN_SECONDS", "120"))
CALL_DELAY_MIN = float(env("CALL_DELAY_MIN", "10.0"))
CALL_DELAY_MAX = float(env("CALL_DELAY_MAX", "30.0"))
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
        rest_url = env("UPSTASH_REDIS_REST_URL", "")
        rest_token = env("UPSTASH_REDIS_REST_TOKEN", "")
        if rest_url or rest_token or env("REDIS_URL", "").startswith("https://") or "upstash.io" in env("REDIS_URL", ""):
            try:
                from redis_rest import UpstashRedisREST
                _redis_client = UpstashRedisREST(rest_url, rest_token)
                return _redis_client
            except Exception as e:
                logger.warning(f"[redis_client] Erro ao usar UpstashRedisREST: {e}")
        try:
            import redis as redis_lib
            _redis_client = redis_lib.from_url(REDIS_URL)
        except Exception:
            from redis_rest import UpstashRedisREST
            _redis_client = UpstashRedisREST()
    return _redis_client



def _import_state_key(job_id: str) -> str:
    return f"import_job:{job_id}"


def _import_rows_key(job_id: str) -> str:
    return f"import_job:{job_id}:rows"


def _import_stop_key(job_id: str) -> str:
    return f"import_job:{job_id}:stop"


def _is_import_stopped(job_id: str) -> bool:
    try:
        if redis_client().exists(_import_stop_key(job_id)):
            return True
    except Exception:
        pass
    try:
        job = supabase.table("import_jobs").select("status").eq("id", job_id).execute().data
        return bool(job and job[0].get("status") == "stopped")
    except Exception:
        return False


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
        upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, f"import_{job_id}_rows.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"[IMPORT_ROWS] Erro ao salvar arquivo local: {e}")

    try:
        redis_client().setex(
            _import_rows_key(job_id),
            IMPORT_REDIS_TTL_SECONDS,
            json.dumps(rows[:200], ensure_ascii=False),
        )
    except Exception:
        pass


def _get_import_rows(job_id: str) -> list:
    try:
        upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
        file_path = os.path.join(upload_dir, f"import_{job_id}_rows.json")
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"[IMPORT_ROWS] Erro ao ler arquivo local: {e}")

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
        redis_client().delete(_import_state_key(job_id), _import_rows_key(job_id), _import_stop_key(job_id))
    except Exception:
        pass
    try:
        upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
        file_path = os.path.join(upload_dir, f"import_{job_id}_rows.json")
        if os.path.exists(file_path):
            os.remove(file_path)
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


def _acquire_scheduler_lock(campaign_id: str = "") -> tuple:
    token = str(uuid.uuid4())
    lock_key = f"dialer:scheduler_lock:{campaign_id}" if campaign_id else "dialer:scheduler_lock"
    try:
        ok = redis_client().set(lock_key, token, nx=True, ex=5)
        return bool(ok), token
    except Exception:
        return True, token


def _release_scheduler_lock(token: str, campaign_id: str = ""):
    lock_key = f"dialer:scheduler_lock:{campaign_id}" if campaign_id else "dialer:scheduler_lock"
    try:
        r = redis_client()
        val = r.get(lock_key)
        if isinstance(val, bytes):
            val = val.decode("utf-8", errors="ignore")
        if str(val or "").strip() == str(token).strip():
            r.delete(lock_key)
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
    campaign_assistant_id: str = "",
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

        campaign_assistant_id_resolved = campaign_assistant_id or ""
        assistant_id = campaign_assistant_id_resolved if campaign_assistant_id_resolved else _get_assistant_id(debito)

        payload: Dict[str, Any] = {
            "phoneNumberId": phone_id,
            "assistantId":   assistant_id,
            "customer":      {"number": phone_e164, "name": name},
        }

        cpf_raw = str(cpf or (debito or {}).get("cpf") or (debito or {}).get("CPF") or (debito or {}).get("Valorcpf") or "").strip()
        cpf_clean = re.sub(r"\D", "", cpf_raw)
        cpf_formatted = f"{cpf_clean[:3]}.{cpf_clean[3:6]}.{cpf_clean[6:9]}-{cpf_clean[9:]}" if len(cpf_clean) == 11 else cpf_raw
        cpf_prefixo3 = cpf_clean[:3] if len(cpf_clean) >= 3 else ""

        inst = (debito or {}).get("instituicao", "")
        valor_final_avista = (debito or {}).get("PgtoAvista", {}).get("ValorFinal") or (debito or {}).get("ValorFinalAVista") or "0,00"
        
        pgto_cartao = (debito or {}).get("PgtoParceladoCartao")
        if not isinstance(pgto_cartao, dict):
            pgto_cartao = {"Parcelas": "1", "ValorParcela": valor_final_avista}
        else:
            pgto_cartao = dict(pgto_cartao)
            if "Parcelas" not in pgto_cartao:
                pgto_cartao["Parcelas"] = str(pgto_cartao.get("parcelas") or "1")
            if "ValorParcela" not in pgto_cartao:
                pgto_cartao["ValorParcela"] = str(pgto_cartao.get("valor_parcela") or pgto_cartao.get("ValorFinal") or valor_final_avista)

        first_name = (name or "").strip().split()[0].title() if name else ""
        first_msg = f"Oi, {first_name or 'tudo bem'}. Aqui é a Júlia, da assessoria financeira da {inst or 'nossa instituição'}. Por segurança, pode me confirmar apenas os três primeiros números do seu CPF?"

        overrides: Dict[str, Any] = {
            "firstMessage": first_msg,
            "silenceTimeoutSeconds": 12,
            "maxDurationSeconds": 600,
        }

        server_url = env("VAPI_SERVER_URL", "").strip() or env("PUBLIC_WEBHOOK_URL", "").strip()
        if server_url:
            overrides["serverUrl"] = server_url


        overrides["variableValues"] = {
            "cpf":                 cpf_clean,
            "CPF":                 cpf_clean,
            "Valorcpf":            cpf_prefixo3,
            "valorcpf":            cpf_prefixo3,
            "Valorcpf_full":       cpf_clean,
            "cpf_formatado":       cpf_formatted,
            "cpf_prefixo3":        cpf_prefixo3,
            "cpf_esperado":        cpf_prefixo3,
            "Valorcpf_prefixo3":   cpf_prefixo3,
            "Valorcpf_3digitos":   cpf_prefixo3,
            "instituicao":         inst,
            "NominalPrinc":        (debito or {}).get("PgtoAvista", {}).get("ValorTotal", "0,00") if debito else "0,00",
            "PgtoAvista":          (debito or {}).get("PgtoAvista", {}) if debito else {},
            "CalculoBoleto":       (debito or {}).get("CalculoBoleto", {}) if debito else {},
            "ParcelasBoleto":      (debito or {}).get("ParcelasBoleto", "0") if debito else "0",
            "PgtoParceladoCartao": pgto_cartao,
            "PrimeiroVencto":      (debito or {}).get("PrimeiroVencto", "em dois dias") if debito else "em dois dias",
            "QuantidadeMensalidades": (debito or {}).get("numero_debitos", "1") if debito else "1",
            "ValorFinalAVista":    valor_final_avista,
        }


        payload["assistantOverrides"] = overrides




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


def _get_wacalls_url() -> str:
    url = env("WACALLS_BASE_URL", "https://wacalls-c-production-cb9b.up.railway.app").strip().strip('"').strip("'")
    url = url.rstrip('\\').rstrip('"').rstrip("'")
    if not url.startswith("http://") and not url.startswith("https://"):
        url = f"https://{url}"
    return url.rstrip("/")

WACALLS_VAPI_PHONE_NUMBER_ID = env("WACALLS_VAPI_PHONE_NUMBER_ID", "75f8ecfc-ce17-416b-8bb4-397c7cdb1c36")

def wacalls_call(phone: str, name: str = "", cpf: str = "", debito: dict = None, campaign_assistant_id: str = "") -> Dict:
    import requests
    phone_e164 = _phone_e164(phone)
    digits = re.sub(r"\D", "", phone_e164)
    if len(digits) < 12:
        raise Exception(f"telefone invalido para WaCalls: {phone}")

    # 1. Inicia sessão da Julia na Vapi API
    vapi_call_id = None
    try:
        vapi_data = vapi_call(
            phone,
            cpf=cpf,
            name=name,
            debito=debito,
            phone_number_id=WACALLS_VAPI_PHONE_NUMBER_ID,
            campaign_assistant_id=campaign_assistant_id,
        )
        vapi_call_id = vapi_data.get("id")
    except Exception as ex:
        logging.warning(f"Aviso reserva Vapi SIP WaCalls: {ex}")

    # 2. Dispara a chamada telefônica via WhatsApp no WaCalls
    wacalls_url = _get_wacalls_url()
    payload = {
        "phone": phone_e164,
        "name": name or cpf or "Devedor",
        "cpf": cpf,
    }
    session_id = "default"
    try:
        sess_resp = requests.get(f"{wacalls_url}/api/sessions", timeout=4)
        if sess_resp.status_code == 200:
            sessions = sess_resp.json()
            if isinstance(sessions, dict) and "sessions" in sessions:
                sessions = sessions["sessions"]
            if isinstance(sessions, list):
                for s in sessions:
                    if s.get("status") in ("connected", "paired", "active", "online") or s.get("paired") or s.get("state") == "open":
                        session_id = s.get("id")
                        break
                if session_id == "default" and sessions:
                    session_id = sessions[0].get("id", "default")
    except Exception:
        pass

    url = f"{wacalls_url}/api/sessions/{session_id}/calls"
    resp = requests.post(url, json=payload, timeout=8)
    if resp.status_code not in (200, 201):
        raise Exception(f"WaCalls API erro [{resp.status_code}]: {resp.text}")
    res_json = resp.json()
    call_id = res_json.get("id") or res_json.get("call_id") or f"wacalls-{int(time.time())}"
    return {"id": call_id, "vapi_call_id": vapi_call_id or call_id, "provider": "wacalls"}


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
    cols_lower = {c.lower(): c for c in cols}
    for i in range(1, 11):
        for key_cand in (f"fone{i}", f"telefone{i}", f"tel{i}", f"phone{i}"):
            orig_key = cols_lower.get(key_cand)
            if orig_key:
                val = _normalize_phone(row.get(orig_key, ""))
                if val and val != "nan" and val not in phones:
                    phones.append(val)
    return phones


def _update_result(row_id, status, call_id, phone, error=None):
    try:
        update = {"status": status}
        if call_id: update["vapi_call_id"] = call_id
        if phone:   update["phone"]         = phone
        if error:   update["error"]         = error
        if status in ("sem_debito", "sem_telefone", "falha_sem_linha", "erro"):
            update["tabulation"] = status
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
    if _is_import_stopped(job_id):
        return
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

    # Consulta o provedor configurado na campanha (wavoip, wacalls ou hybrid)
    dialer_provider = "wavoip"
    try:
        camp_res = supabase.table("campaigns").select("dialer_provider").eq("id", campaign_id).execute().data
        if camp_res and camp_res[0].get("dialer_provider"):
            dialer_provider = camp_res[0].get("dialer_provider")
    except Exception:
        pass

    # Sem linha reservada (e provedor wavoip), aguarda linha disponivel
    if dialer_provider in ("wavoip", "hybrid") and not line_token:
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
            if dialer_provider != "hybrid":
                _update_result(row_id, "falha_sem_linha", None, None)
                return

    # Tenta cada telefone em sequencia ate um funcionar
    last_error = None
    for phone in phones:
        phone = str(phone).strip()
        if not phone:
            continue
        try:
            if dialer_provider == "wacalls":
                data = wacalls_call(phone, name=name, cpf=cpf, debito=debito, campaign_assistant_id=contact.get("campaign_assistant_id") or "")
            elif dialer_provider == "hybrid":
                try:
                    data = vapi_call(
                        phone,
                        cpf=cpf,
                        name=name,
                        debito=debito,
                        line_token=line_token,
                        phone_number_id=phone_number_id,
                        line_name=line_name,
                        campaign_assistant_id=contact.get("campaign_assistant_id") or "",
                    )
                except Exception as ex_wavoip:
                    # Failover automatico para WaCalls caso a Wavoip falhe
                    data = wacalls_call(phone, name=name, cpf=cpf, debito=debito, campaign_assistant_id=contact.get("campaign_assistant_id") or "")
            else:
                data = vapi_call(
                    phone,
                    cpf=cpf,
                    name=name,
                    debito=debito,
                    line_token=line_token,
                    phone_number_id=phone_number_id,
                    line_name=line_name,
                    campaign_assistant_id=contact.get("campaign_assistant_id") or "",
                )
            call_id = data.get("id")
            if dialer_provider == "wacalls":
                _update_result(row_id, "atendida", call_id, phone)
                try:
                    _check_campaign_completion(campaign_id)
                except Exception:
                    pass
            else:
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
            _dispatch_task(fill_campaign_capacity_task, args=[campaign_id])

    else:
        _update_result(row_id, "erro", None, phones[0], error)
        try:
            _check_campaign_completion(campaign_id)
        except Exception:
            pass


def _check_campaign_completion(campaign_id: str) -> bool:
    """
    Verifica se ainda existem chamadas em status 'pendente', 'enfileirado' ou 'em_andamento'
    para esta campanha. Se não houver mais chamadas ativas/pendentes, marca a campanha como 'finalizada'.
    """
    if not campaign_id:
        return False
    try:
        active_res = supabase.table("campaign_calls") \
            .select("id", count="exact") \
            .eq("campaign_id", campaign_id) \
            .in_("status", ["pendente", "enfileirado", "em_andamento"]) \
            .execute()
        active_count = active_res.count or 0

        finished_res = supabase.table("campaign_calls") \
            .select("id", count="exact") \
            .eq("campaign_id", campaign_id) \
            .in_("status", ["finalizado", "erro", "atendido", "sem_debito", "sem_telefone", "falha_sem_linha"]) \
            .execute()
        finished_count = finished_res.count or 0

        if active_count == 0:
            supabase.table("campaigns").update({
                "status": "finalizada",
                "finished": finished_count,
                "updated_at": "now()"
            }).eq("id", campaign_id).execute()
            logger.info(f"[CAMPAIGN] Campanha {campaign_id} finalizada automaticamente! (finished={finished_count})")
            return True
        else:
            supabase.table("campaigns").update({
                "finished": finished_count,
                "updated_at": "now()"
            }).eq("id", campaign_id).execute()
            return False
    except Exception as e:
        logger.error(f"[CAMPAIGN] Erro ao verificar finalização da campanha {campaign_id}: {e}")
        return False


def _merge_debito_with_import_meta(row: dict, debito: dict) -> dict:

    merged = dict(debito or {})
    imported = row.get("debito_data") if isinstance(row.get("debito_data"), dict) else {}
    for key in ("all_phones", "email", "imported_id", "iddev", "idcrm", "uf", "cidade", "raw_phone_fallback"):
        if imported.get(key) and not merged.get(key):
            merged[key] = imported.get(key)
    return merged


def _imported_debito_is_usable(row: dict) -> bool:
    debito = row.get("debito_data") if isinstance(row.get("debito_data"), dict) else {}
    if not debito:
        return False
    if debito.get("PgtoAvista") or debito.get("CalculoBoleto"):
        return True
    return bool(debito.get("idcalc") or debito.get("iddev") or debito.get("instituicao"))


def _previous_usable_debito(cpf: str) -> dict:
    cpf = str(cpf or "").strip()
    if not cpf:
        return {}
    try:
        rows = supabase.table("campaign_calls") \
            .select("debito_data") \
            .eq("cpf", cpf) \
            .order("updated_at", desc=True) \
            .limit(20) \
            .execute().data or []
    except Exception:
        return {}

    for row in rows:
        if _imported_debito_is_usable(row):
            return row.get("debito_data") or {}
    return {}


def _build_fallback_debito_from_meta(meta: dict) -> dict:
    nominal = meta.get("nominal") or meta.get("nominal_princ") or meta.get("nominal_val") or "0,00"
    instituicao = meta.get("instituicao") or "Faculdade"
    
    return {
        "instituicao":    instituicao,
        "nome_devedor":   meta.get("name") or "Aluno",
        "numero_debitos": "1",
        "idcalc":         meta.get("iddev") or meta.get("imported_id") or "",
        "iddev":          meta.get("iddev") or "",
        "PrimeiroVencto": "em dois dias",
        "PgtoAvista": {
            "ValorTotal":    nominal,
            "PercDesconto":  "0",
            "ValorDesconto": "0,00",
            "ValorFinal":    nominal,
        },
        "CalculoBoleto": {
            "SubtotalBoleto":    nominal,
            "HonorarioBoleto":   "0,00",
            "ValorCobrarBoleto": nominal,
        },
        "ParcelasBoleto": "1",
        "PgtoParceladoCartao": {
            "Parcelas":     "1",
            "ValorParcela": nominal,
            "ValorFinal":   nominal,
        },
    }


def _validate_debt_before_call(row: dict) -> dict:
    cpf = row.get("cpf", "")
    if not cpf:
        supabase.table("campaign_calls").update({
            "status": "sem_debito",
            "error": "ddm: cpf vazio",
            "tabulation": "sem_debito",
        }).eq("id", row["id"]).execute()
        return {"ok": False, "reason": "cpf vazio"}

    # MOCK MODE / MODO HOMOLOGAÇÃO:
    # Se o CPF começar com '119' ou o nome contiver 'test' (caso de teste/homologação),
    # geramos um débito simulado de R$ 1.500,00 da Cruzeiro do Sul para a chamada prosseguir e ser testada na hora!
    is_test_mode = "test" in str(row.get("name", "")).lower()
    if is_test_mode:
        debito_data = {
            "nominal": "1.500,00",
            "instituicao": "UNIVERSIDADE VEIGA DE ALMEIDA (Teste Homologação)",
        }
        fallback = _build_fallback_debito_from_meta({**debito_data, "name": row.get("name", "Aluno Teste")})
        return {"ok": True, "debito": _merge_debito_with_import_meta(row, fallback), "stale_ddm": True}

    result = _processar_debito_result(cpf)
    if not result.get("ok"):
        # Fallback 1: se temos metadados válidos da planilha na linha importada, usa para construir o débito
        debito_data = row.get("debito_data") or {}
        if isinstance(debito_data, dict) and (debito_data.get("instituicao") or debito_data.get("nominal") or debito_data.get("iddev")):
            fallback = _build_fallback_debito_from_meta({**debito_data, "name": row.get("name", "Aluno")})
            return {"ok": True, "debito": _merge_debito_with_import_meta(row, fallback), "stale_ddm": True}

        # Fallback 2: se o debito_data importado for usável diretamente
        if _imported_debito_is_usable(row):
            return {"ok": True, "debito": row.get("debito_data") or {}, "stale_ddm": True}

        # Fallback 3: tenta histórico de débitos utilizáveis passados
        previous_debito = _previous_usable_debito(cpf)
        if previous_debito:
            return {"ok": True, "debito": _merge_debito_with_import_meta(row, previous_debito), "stale_ddm": True}

        supabase.table("campaign_calls").update({
            "status": "erro",
            "error": f"ddm: {result.get('error', 'erro DDM')}",
            "tabulation": "erro",
        }).eq("id", row["id"]).execute()
        return {"ok": False, "reason": result.get("error", "erro DDM")}

    debito = result.get("debito")
    if not debito:
        # Fallback 4: Se a DDM respondeu com sucesso que o devedor não tem débitos ativos (R$ 0,00),
        # mas o importador trouxe um valor nominal real maior que zero na planilha, usamos os dados da planilha!
        debito_data = row.get("debito_data") or {}
        nominal = debito_data.get("nominal") or "0,00" if isinstance(debito_data, dict) else "0,00"
        if isinstance(debito_data, dict) and nominal not in ("0,00", "0", ""):
            fallback = _build_fallback_debito_from_meta({**debito_data, "name": row.get("name", "Aluno")})
            return {"ok": True, "debito": _merge_debito_with_import_meta(row, fallback), "stale_ddm": True}

        supabase.table("campaign_calls").update({
            "status": "sem_debito",
            "error": "ddm: sem debito antes da chamada",
            "tabulation": "sem_debito",
        }).eq("id", row["id"]).execute()
        return {"ok": False, "reason": "sem debito"}

    return {"ok": True, "debito": _merge_debito_with_import_meta(row, debito)}


def _get_call_delay_wait(delay_seconds: float = 7.0) -> float:
    """
    Calculates the wait time for the next call to enforce a global delay.
    Uses Redis to store the next allowed timestamp and coordinates via a lock.
    """
    try:
        r = redis_client()
        lock_key = "dialer:rate_limit_lock"
        acquired = False
        token = str(uuid.uuid4())
        
        # Try to acquire lock for up to 2 seconds
        for _ in range(20):
            if r.set(lock_key, token, nx=True, px=1000):
                acquired = True
                break
            time.sleep(0.1)
            
        if not acquired:
            logger.warning("Could not acquire dialer rate limit lock, returning 0 delay")
            return 0.0
            
        try:
            now = time.time()
            redis_key = "dialer:next_call_allowed_at"
            next_allowed_raw = r.get(redis_key)
            next_allowed = float(next_allowed_raw) if next_allowed_raw else 0.0
            
            if next_allowed < now:
                next_allowed = now
                wait_time = 0.0
            else:
                wait_time = next_allowed - now
                
            r.set(redis_key, str(next_allowed + delay_seconds))
            return wait_time
        finally:
            try:
                val = r.get(lock_key)
                if isinstance(val, bytes):
                    val = val.decode("utf-8", errors="ignore")
                if str(val or "").strip() == str(token).strip():
                    r.delete(lock_key)
            except Exception:
                pass

    except Exception as e:
        logger.error(f"Error calculating call delay wait: {e}")
        return 0.0


def fill_campaign_capacity(campaign_id: str) -> dict:
    locked, lock_token = _acquire_scheduler_lock(campaign_id)
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
        campaign_assistant_id = camp.get("assistant_id") or ""
        if camp.get("status") != "em_andamento":
            return {"ok": True, "status": camp.get("status"), "fired": 0}

        healthy = _filter_campaign_lines(get_healthy_lines(), camp)
        line_tokens = [line.get("token") for line in healthy if line.get("token")]
        counts = _active_line_counts(line_tokens)
        slots = _available_line_slots(healthy, counts)
        capacity = len(healthy) * LINE_MAX_CONCURRENT
        active = sum(counts.values())

        if not slots:
            # Se não temos slots porque todas as linhas estão offline ou em cooldown,
            # e ainda temos contatos pendentes na campanha, agendamos uma nova tentativa em 30s.
            if len(healthy) == 0:
                try:
                    pending_check = supabase.table("campaign_calls") \
                        .select("id") \
                        .eq("campaign_id", campaign_id) \
                        .eq("status", "pendente") \
                        .limit(1) \
                        .execute().data or []
                    if pending_check:
                        logger.warning(f"[DIALER] Nenhuma linha saudavel no momento para campanha {campaign_id}. Re-agendando em 30s...")
                        _dispatch_task(fill_campaign_capacity_task, args=[campaign_id], countdown=30)
                except Exception as e:
                    logger.error(f"[DIALER] Erro ao verificar pendencias para re-agendamento: {e}")

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

            # Randomize call delay to avoid carrier detection
            delay = random.uniform(CALL_DELAY_MIN, CALL_DELAY_MAX)
            wait_time = _get_call_delay_wait(delay)
            _dispatch_task(
                make_call_task,
                args=[campaign_id, {
                    "row_id":          row["id"],
                    "cpf":             row.get("cpf", ""),
                    "phone":           row.get("phone", ""),
                    "name":            row.get("name", ""),
                    "debito_data":     debito,
                    "line_token":      meta["line_token"],
                    "line_name":       meta["line_name"],
                    "phone_number_id": meta["phone_number_id"],
                    "campaign_assistant_id": campaign_assistant_id,
                }],
                countdown=int(round(wait_time))
            )
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
            _dispatch_task(fill_campaign_capacity_task, args=[campaign_id], countdown=2)
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
        logger.error(f"[DIALER] Erro em fill_campaign_capacity para {campaign_id}: {e}", exc_info=True)
        try:
            _dispatch_task(fill_campaign_capacity_task, args=[campaign_id], countdown=30)
        except Exception:
            pass
        return {"ok": False, "error": str(e), "fired": 0}

    finally:
        _release_scheduler_lock(lock_token, campaign_id)



@celery.task(name="tasks.fill_campaign_capacity")
def fill_campaign_capacity_task(campaign_id: str):
    return fill_campaign_capacity(campaign_id)


def _is_vapi_call_active(call_id: str) -> bool:
    if not call_id:
        return False
    try:
        import requests
        url = f"https://api.vapi.ai/call/{call_id}"
        v_key = os.getenv("VAPI_API_KEY", "332987f4-f832-4542-9fd0-76de02bde971")
        headers = {"Authorization": f"Bearer {v_key}"}
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            call_data = r.json()
            status = call_data.get("status")
            if status in ("queued", "ringing", "in-progress"):
                return True
        return False
    except Exception as e:
        logger.error(f"[VAPI_ACTIVE_CHECK] Erro ao verificar status da chamada {call_id}: {e}")
        return False


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

        cutoff  = (datetime.now() - timedelta(minutes=WATCHDOG_TIMEOUT_MIN)).strftime("%Y-%m-%d %H:%M:%S")
        rescued = 0
        errors  = 0

        for camp in camps:
            campaign_id = camp["id"]
            try:
                # Chamadas travadas (sem webhook no tempo limite)
                stuck = supabase.table("campaign_calls") \
                    .select("id, cpf, phone, name, debito_data, order_idx, watchdog_retries, vapi_call_id") \
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
                    vapi_id = row.get("vapi_call_id")
                    if vapi_id and _is_vapi_call_active(vapi_id):
                        logger.info(f"[WATCHDOG] Chamada {vapi_id} (row_id={row['id']}) ainda está ativa na Vapi. Postergando watchdog.")
                        try:
                            supabase.table("campaign_calls").update({
                                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }).eq("id", row["id"]).execute()
                        except Exception:
                            pass
                        continue

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

        _dispatch_task(fill_campaign_capacity_task, args=[campaign_id])

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
    """Verifica o progresso das chamadas, atualiza o contador 'finished' e finaliza a campanha se concluida."""
    try:
        remaining_res = supabase.table("campaign_calls") \
            .select("id", count="exact") \
            .eq("campaign_id", campaign_id) \
            .in_("status", ["pendente", "enfileirado", "em_andamento", "atendido"]) \
            .execute()
        remaining = remaining_res.count or 0

        finished_res = supabase.table("campaign_calls") \
            .select("id", count="exact") \
            .eq("campaign_id", campaign_id) \
            .in_("status", ["finalizado", "erro", "sem_debito", "sem_telefone", "falha_sem_linha", "atendida"]) \
            .execute()
        finished = finished_res.count or 0

        camp_res = supabase.table("campaigns").select("total").eq("id", campaign_id).execute()
        total = (camp_res.data[0].get("total") or 0) if camp_res.data else 0

        update_payload = {"finished": finished, "updated_at": "now()"}
        if remaining == 0 or (total > 0 and finished >= total):
            update_payload["status"] = "finalizada"

        supabase.table("campaigns").update(update_payload).eq("id", campaign_id).execute()
    except Exception as e:
        logger.error(f"[CAMPAIGN] Erro em _check_campaign_completion para {campaign_id}: {e}")




# ── ACORDO FORMALIZADO ────────────────────────────────────────

def _ddm_get_iddev(cpf: str) -> str:
    """Busca o iddev pelo CPF na API DDM."""
    cpf = _norm_cpf(cpf)
    tk = DDM_TOKEN_BUSCA or DDM_TOKEN
    r = requests.get(
        f"{DDM_BASE}/calc/localiza_dev.php",
        params={"tk": tk, "cpf": cpf},
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

    tk = DDM_TOKEN_BUSCA or DDM_TOKEN
    if not tk:
        return ""

    try:
        res = requests.get(
            f"{DDM_BASE}/calc/localiza_dev.php",
            params={"tk": tk, "cpf": cpf},
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
    names_norm = {str(n).lower().replace("_", "").replace("-", "") for n in names}
    return _ddm_find_first_recursive(obj, names_norm)

def _ddm_find_first_recursive(obj, names_norm: set) -> str:
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_norm = str(key).lower().replace("_", "").replace("-", "")
            if key_norm in names_norm:
                if isinstance(value, str) and value.strip() and value.strip().lower() != "none":
                    return value.strip()
                if isinstance(value, (int, float)) and value:
                    return str(value)
                if isinstance(value, dict):
                    nested = _ddm_find_first_recursive(value, {"link", "url", "href"})
                    if nested:
                        return nested
            nested = _ddm_find_first_recursive(value, names_norm)
            if nested:
                return nested
    elif isinstance(obj, list):
        for item in obj:
            nested = _ddm_find_first_recursive(item, names_norm)
            if nested:
                return nested
    return ""


def _valid_linha_digitavel(value: str) -> str:
    value = str(value or "").strip()
    digits = re.sub(r"\D", "", value)
    return value if len(digits) >= 30 else ""


def _valid_payment_url(value: str) -> str:
    value = str(value or "").strip()
    if not value or value.lower() == "none":
        return ""
    if re.match(r"^https?://", value, re.IGNORECASE):
        return value
    if value.lower().startswith("www."):
        return "https://" + value
    if "." in value and "/" in value and not " " in value:
        return "https://" + value
    return ""


def _has_payment_info(link_boleto: str = "", link_pix: str = "", linha_dig: str = "") -> bool:
    return bool(_valid_payment_url(link_boleto) or _valid_payment_url(link_pix) or _valid_linha_digitavel(linha_dig))


def _map_sistema_to_cli(sistema: str) -> str:
    s = str(sistema or "").strip().lower()
    if s in ("cruzeirodosul", "cruzeiro_do_sul", "cruzeiro"):
        return "cruzeiro"
    return s


def _ddm_get_payment_links(iddev: str, cli: str = "ddm") -> dict:
    """Busca links de boleto/pix pelo iddev."""
    r = requests.get(
        f"{DDM_BASE}/calc/",
        params={"tk": DDM_TOKEN, "idDev": iddev, "cli": cli},
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
    linha_dig = _valid_linha_digitavel(linha_dig or _ddm_find_first(data, {
        "linhadigitavel", "linhadig", "digitalline", "digitableline"
    }))
    vencimento = vencimento or _ddm_find_first(data, {
        "vencimento", "datavencimento", "duedate"
    })

    return {
        "link_boleto": _valid_payment_url(link_boleto),
        "link_pix":    _valid_payment_url(link_pix),
        "linha_dig":   linha_dig,
        "vencimento":  vencimento,
    }


def _get_discount_percentage(debito: dict, parc: int) -> str:
    if not isinstance(debito, dict):
        return "0"
        
    def clean_pct(pct_val) -> str:
        if pct_val is None:
            return "0"
        s = str(pct_val).strip()
        if not s:
            return "0"
        s = s.replace("%", "").replace(",", ".").strip()
        try:
            val = float(s)
            if 0 < val < 1:
                val = val * 100
            return str(int(round(val)))
        except Exception:
            return "0"

    try:
        if parc <= 1:
            pct = debito.get("PgtoAvista", {}).get("PercDesconto")
            return clean_pct(pct)
        
        lista_parcelas = debito.get("ListaParcelas", {}) or {}
        parcelas = lista_parcelas.get("Parcelas") or []
        for p in parcelas:
            if isinstance(p, dict) and int(p.get("Parcelas", 0)) == parc:
                pct = p.get("PercDescontoParcela") or p.get("PercDesconto")
                return clean_pct(pct)
    except Exception:
        pass
    return "0"


def _ddm_formalizar_acordo(cpf: str, parc: int = 1, debito: dict = None) -> dict:
    """Formaliza acordo na DDM usando o novo fluxo do calc/efetiva_acordo.php"""
    token_busca = DDM_TOKEN_BUSCA or DDM_TOKEN
    token_calcula = DDM_TOKEN or DDM_AGREEMENT_TOKEN
    if not token_busca or not token_calcula:
        raise RuntimeError("DDM_TOKEN_BUSCA ou DDM_TOKEN nao configurado")

    cpf = _norm_cpf(cpf)
    
    # 1. Localiza o iddev e o sistema pelo CPF usando o token_busca
    url_loc = "https://ddmacordos.com/calc/localiza_dev.php"
    r1 = requests.get(url_loc, params={"tk": token_busca, "cpf": cpf}, timeout=15)
    r1.raise_for_status()
    data1 = r1.json()
    
    if not isinstance(data1, list) or len(data1) == 0:
        raise RuntimeError(f"Nenhum devedor encontrado na DDM para o CPF {cpf}")
        
    dev = data1[0]
    iddev = dev.get("iddev")
    
    # Mapeia dinamicamente o cli com base no sistema retornado pela DDM
    sistema_raw = dev.get("sistema", "").strip().lower()
    sistema = "ddm"
    if sistema_raw == "cruzeirodosul":
        sistema = "cruzeiro"
        
    email = dev.get("email") or dev.get("email_aluno") or dev.get("email_responsavel") or ""
    nome = dev.get("nome") or ""
    
    if not iddev:
        raise RuntimeError(f"Devedor encontrado mas sem iddev para o CPF {cpf}")

    data2 = {}
    desconto = _get_discount_percentage(debito, int(parc or 1))
    url_efe = "https://ddmacordos.com/calc/efetiva_acordo.php"
    params = {
        "tk": token_calcula,
        "idDev": iddev,
        "cli": sistema,
        "Parc": str(int(parc or 1))
    }
    if desconto and desconto != "0":
        params["Desconto"] = desconto

    logger.warning("[DDM_FORMALIZA] CPF_FINAL=%s formalizando acordo (parc=%s, desc=%s) no efetiva_acordo.php", cpf[-4:], parc, desconto)
    r2 = requests.get(url_efe, params=params, timeout=30)
    r2.raise_for_status()
    if r2.text.strip():
        try:
            data2 = r2.json()
        except Exception:
            pass
        
    link_boleto = _ddm_find_first(data2, {"linkboleto", "boletourl", "urlboleto", "boleto", "link_boleto", "url_boleto"})
    link_pix    = _ddm_find_first(data2, {"linkpix", "pixurl", "urlpix", "qrcodepix", "qrcode", "pix", "link_pix", "url_pix"})
    linha_dig   = _valid_linha_digitavel(_ddm_find_first(data2, {"linhaboleto", "linhadigitavel", "linhadig", "digitalline", "digitableline", "linha_digitavel", "linha"}))
    vencimento  = _ddm_find_first(data2, {"vencimento", "datavencto", "vencto", "due_date", "venc", "data_vencimento"})
    nr_acordo   = _ddm_find_first(data2, {"nracordo", "nr_acordo", "acordo", "agreement_number", "numero_acordo", "idacordo"})
    valor_ret   = _ddm_find_first(data2, {"valor", "valortotal", "valor_total", "valoracordo", "valor_acordo", "amount", "valorfinal", "valordocumento", "valor_documento", "val_total", "total", "valor_final"})
    
    return {
        "raw": data2,
        "link_boleto": _valid_payment_url(link_boleto),
        "link_pix": _valid_payment_url(link_pix),
        "linha_dig": linha_dig,
        "vencimento": vencimento,
        "nr_acordo": nr_acordo,
        "valor": valor_ret,
        "idcalc": iddev,
        "nome": nome,
        "cpf": cpf,
        "email": email,
        "sistema": sistema,
    }


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    # Remove eventuais aspas ou caracteres estranhos que possam vir da API
    url = url.replace('"', '').replace("'", "")
    if not url.lower().startswith(("http://", "https://")):
        return f"https://{url}"
    return url


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
    link_pix = _normalize_url(link_pix)
    link_boleto = _normalize_url(link_boleto)
    import socket
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    original_destinatario = destinatario
    recip = DEBUG_EMAIL_RECIPIENT
    if recip == "caiovicenteti@gmail.com":
        recip = "caiovicenterj@gmail.com"
    if recip:
        logger.info("[EMAIL_DEBUG] Redirecionando e-mail de %s para %s", destinatario, recip)
        destinatario = recip

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

    from email.utils import make_msgid
    msg = MIMEMultipart("alternative")
    subject = f"Acordo formalizado — {instituicao}"
    if DEBUG_EMAIL_RECIPIENT:
        subject = f"[DEBUG] (Para: {original_destinatario}) " + subject
    msg["Subject"] = subject
    msg["From"]    = SMTP_FROM
    msg["To"]      = destinatario
    msg["Message-ID"] = make_msgid(domain=SMTP_FROM.split("@")[-1])
    msg.attach(MIMEText(html, "html"))

    target_hosts = []
    for h in [SMTP_HOST, "mail.grupoddm.ia.br", "localhost", "127.0.0.1"]:
        if h and h not in target_hosts:
            target_hosts.append(h)

    errors = []
    sent = False
    for host in target_hosts:
        for port, sec in [(int(SMTP_PORT), SMTP_SECURITY), (587, "starttls"), (465, "ssl"), (25, "none")]:
            try:
                if sec == "ssl":
                    srv = smtplib.SMTP_SSL(host, port, timeout=5)
                else:
                    srv = smtplib.SMTP(host, port, timeout=5)
                with srv:
                    srv.ehlo()
                    if sec == "starttls":
                        srv.starttls()
                        srv.ehlo()
                    if SMTP_USER and SMTP_PASSWORD:
                        try:
                            srv.login(SMTP_USER, SMTP_PASSWORD)
                        except Exception:
                            pass
                    srv.sendmail(SMTP_FROM, [destinatario], msg.as_string())
                logger.info(f"[EMAIL_ACORDO] E-mail de acordo para {destinatario} enviado com sucesso via {host}:{port}!")
                sent = True
                break
            except Exception as err:
                errors.append(f"{host}:{port} ({err})")
        if sent:
            break

    if not sent:
        err_msg = " | ".join(errors[:2])
        logger.error(f"[EMAIL_ACORDO] Erro ao enviar e-mail para {destinatario}: {err_msg}")
        raise Exception(f"Falha de SMTP: {err_msg}")



def _enviar_whatsapp_acordo(
    destinatario: str,
    nome: str,
    instituicao: str,
    valor: str,
    forma_pagamento: str,
    link_boleto: str,
    link_pix: str,
    linha_dig: str,
    vencimento: str,
) -> bool:
    """Envia mensagem de formalização de acordo para o WhatsApp do devedor via Wavoip API."""
    link_pix = _normalize_url(link_pix)
    link_boleto = _normalize_url(link_boleto)
    if not destinatario:
        logger.info("[WAVOIP_WPP] Telefone do destinatário não informado.")
        return False

    original_destinatario = destinatario
    if DEBUG_PHONE_RECIPIENT:
        recip_digits = re.sub(r"\D", "", DEBUG_PHONE_RECIPIENT)
        if recip_digits:
            logger.info("[WPP_DEBUG] Redirecionando WhatsApp de %s para %s (Modo Teste)", destinatario, recip_digits)
            destinatario = recip_digits

    try:
        phone_norm = _phone_e164(destinatario)
        digits = re.sub(r"\D", "", phone_norm)
        if len(digits) < 10:
            logger.warning(f"[WAVOIP_WPP] Telefone inválido para envio via WhatsApp: {destinatario}")
            return False

        header_title = "*[TESTE DE ACORDO] DDM Assessoria — Acordo Formalizado*" if DEBUG_PHONE_RECIPIENT else "*DDM Assessoria — Acordo Formalizado*"
        msg_lines = [
            header_title,
        ]
        if DEBUG_PHONE_RECIPIENT:
            msg_lines.append(f"_(Destinado originalmente a: {original_destinatario})_")

        msg_lines.extend([
            "",
            f"Prezado(a) *{nome}*,",
            "",
            f"Confirmamos a formalização do acordo referente à pendência vinculada à *{instituicao}*.",
            "",
            f"📌 *Forma de pagamento:* {forma_pagamento}",
            f"💰 *Valor acordado:* R$ {valor}",
        ])
        if vencimento:
            msg_lines.append(f"📅 *Vencimento:* {vencimento}")

        msg_lines.append("")
        if link_pix or link_boleto or linha_dig:
            msg_lines.append("*Dados para Pagamento:*")
            if link_pix:
                msg_lines.append(f"⚡ *PIX:* {link_pix}")
            if link_boleto:
                msg_lines.append(f"📄 *Boleto:* {link_boleto}")
            if linha_dig:
                msg_lines.append(f"🔢 *Linha Digitável:* {linha_dig}")
            msg_lines.append("")

        msg_lines.append("Qualquer dúvida, estamos à disposição.")
        msg_lines.append("*Equipe de Atendimento – DDM*")

        mensagem_texto = "\n".join(msg_lines)

        token = wavoip_login()
        healthy = get_healthy_lines()
        if not healthy:
            logger.warning("[WAVOIP_WPP] Nenhuma linha saudável disponível para envio de WhatsApp")
            return False

        line_token = healthy[0].get("token")
        payload = {
            "number": digits,
            "message": mensagem_texto,
            "token": line_token
        }

        url = f"{WAVOIP_BASE}/v2/messages/send"
        res = requests.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=15
        )
        if not res.ok:
            url_alt = f"{WAVOIP_BASE}/v2/send-text"
            res = requests.post(
                url_alt,
                json=payload,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                timeout=15
            )

        if res.ok:
            logger.info(f"[WAVOIP_WPP] Mensagem de acordo enviada com sucesso via WhatsApp para {digits}")
            return True
        else:
            logger.warning(f"[WAVOIP_WPP] Falha ao enviar WhatsApp para {digits}: HTTP {res.status_code} - {res.text}")
            return False
    except Exception as e:
        logger.error(f"[WAVOIP_WPP] Erro ao enviar mensagem via WhatsApp: {e}")
        return False


def _buscar_pagamento_acordo(dados: dict) -> dict:
    debito = dados.get("debito") if isinstance(dados.get("debito"), dict) else {}
    cli = "ddm"
    candidatos = [
        dados.get("idcalc"),
        dados.get("iddev"),
        debito.get("idcalc"),
        debito.get("iddev"),
        dados.get("nr_acordo"),
        dados.get("cpf"),
    ]
    vistos = set()
    for candidato in candidatos:
        iddev = str(candidato or "").strip()
        if not iddev or iddev in vistos:
            continue
        vistos.add(iddev)
        try:
            pagamentos = _ddm_get_payment_links(iddev, cli=cli)
            if _has_payment_info(
                pagamentos.get("link_boleto", ""),
                pagamentos.get("link_pix", ""),
                pagamentos.get("linha_dig", ""),
            ):
                return pagamentos
        except Exception as e:
            logger.warning("[DDM] boleto ainda indisponivel id=%s cli=%s erro=%s", iddev, cli, e)
    return {"link_boleto": "", "link_pix": "", "linha_dig": "", "vencimento": ""}


def _salvar_acordo_formalizado(payload: dict):
    return supabase.table("acordos_formalizados").insert(payload).execute()


def _atualizar_acordo_formalizado(dados: dict, update: dict):
    campaign_call_id = dados.get("campaign_call_id", "")
    nr_acordo = dados.get("nr_acordo", "")
    cpf = dados.get("cpf", "")
    query = supabase.table("acordos_formalizados").update(update)
    if campaign_call_id:
        return query.eq("campaign_call_id", campaign_call_id).execute()
    if nr_acordo:
        query = query.eq("nr_acordo", nr_acordo)
        if cpf:
            query = query.eq("cpf", cpf)
        return query.execute()
    if cpf:
        return query.eq("cpf", cpf).eq("email_enviado", False).execute()
    return None


def _agendar_verificacao_boleto(dados: dict, tentativa: int = 1):
    _dispatch_task(
        verificar_boleto_acordo,
        args=[dados, tentativa],
        countdown=BOLETO_RETRY_DELAY_SECONDS,
    )



def _obter_tokens_wavoip() -> tuple:
    """Retorna (wavoip_bearer_token, line_device_token) para inclusão no payload do n8n."""
    w_token = ""
    l_token = ""
    try:
        w_token = wavoip_login()
    except Exception as e:
        logger.warning(f"[WAVOIP_TOKEN] Falha ao obter token Wavoip: {e}")

    try:
        healthy = get_healthy_lines()
        if healthy:
            l_token = healthy[0].get("token", "")
    except Exception as e:
        logger.warning(f"[WAVOIP_TOKEN] Falha ao obter linha saudável Wavoip: {e}")

    return w_token, l_token


def _disparar_notificacoes_acordo(dados: dict) -> dict:
    """
    Centraliza o envio do acordo para o n8n Webhook.
    Se N8N_WEBHOOK_URL estiver configurado e o n8n responder com sucesso (HTTP 2xx),
    evita envios nativos duplicados. Caso contrário, aciona o fallback nativo (SMTP / Wavoip Direct).
    """
    cpf             = dados.get("cpf", "")
    nome            = dados.get("nome", "Cliente")
    email           = dados.get("email", "")
    phone           = dados.get("phone") or dados.get("celular") or dados.get("telefone") or ""
    instituicao     = dados.get("instituicao", "")
    valor = dados.get("valor") or ""
    import re
    if not str(valor).strip() or str(valor).lower() == "none" or not re.sub(r"[^\d]", "", str(valor)).strip():
        valor = _ddm_find_first(dados, {"valor", "valortotal", "valoracordo", "amount", "valorfinal", "total"})
    forma_pagamento = dados.get("forma_pagamento", "À vista")
    link_boleto     = dados.get("link_boleto", "")
    link_pix        = dados.get("link_pix", "")
    linha_dig       = dados.get("linha_dig", "")
    vencimento      = dados.get("vencimento", "")
    nr_acordo       = dados.get("nr_acordo", "")
    vapi_call_id    = dados.get("vapi_call_id", "")

    pagamento_pronto = _has_payment_info(link_boleto, link_pix, linha_dig)

    n8n_sucesso = False
    email_enviado = False
    wpp_enviado = False

    # 1. Envia para o Webhook do n8n se configurado
    if N8N_WEBHOOK_URL:
        wavoip_token, line_token = _obter_tokens_wavoip()
        n8n_payload = {
            "cpf":              cpf,
            "nome":             nome,
            "email":            DEBUG_EMAIL_RECIPIENT or email,
            "phone":            DEBUG_PHONE_RECIPIENT or phone,
            "original_email":   email,
            "original_phone":   phone,
            "instituicao":      instituicao,
            "valor":            valor,
            "forma_pagamento":  forma_pagamento,
            "link_boleto":     link_boleto,
            "link_pix":        link_pix,
            "linha_dig":       linha_dig,
            "vencimento":      vencimento,
            "nr_acordo":       nr_acordo,
            "vapi_call_id":     vapi_call_id,
            "pagamento_pronto": pagamento_pronto,
            "wavoip_token":     wavoip_token,
            "line_token":       line_token,
        }
        try:
            logger.info(f"[FORMALIZAR] Enviando dados do acordo para o n8n: {N8N_WEBHOOK_URL}")
            resp = requests.post(N8N_WEBHOOK_URL, json=n8n_payload, timeout=10)
            if resp.ok:
                n8n_sucesso = True
                email_enviado = True
                wpp_enviado = True
                logger.info(f"[FORMALIZAR] Sucesso no envio para o n8n (HTTP {resp.status_code})")
            else:
                logger.warning(f"[FORMALIZAR] n8n respondeu com HTTP {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"[FORMALIZAR] Erro ao enviar Webhook para n8n: {e}")

    # 2. Fallback direto pelo Python se n8n não estiver ativo ou se o envio para o n8n falhar
    if not n8n_sucesso:
        if N8N_WEBHOOK_URL:
            logger.warning("[FORMALIZAR] Webhook n8n falhou. Executando fallback nativo (SMTP / Wavoip Direct).")

        if email and pagamento_pronto:
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
                logger.error(f"[FORMALIZAR] erro SMTP no fallback: {e}")
        elif email and not pagamento_pronto:
            logger.warning(
                "[FORMALIZAR] acordo pendente de boleto cpf_final=%s nr_acordo=%s",
                _norm_cpf(cpf)[-4:],
                nr_acordo,
            )

        if phone and pagamento_pronto:
            try:
                wpp_enviado = _enviar_whatsapp_acordo(
                    destinatario    = phone,
                    nome            = nome,
                    instituicao     = instituicao,
                    valor           = valor,
                    forma_pagamento = forma_pagamento,
                    link_boleto     = link_boleto,
                    link_pix        = link_pix,
                    linha_dig       = linha_dig,
                    vencimento      = vencimento,
                )
            except Exception as e:
                logger.error(f"[FORMALIZAR] erro WhatsApp no fallback: {e}")

    # 3. Notificações Admin (opcional)
    if ADMIN_NOTIFY_EMAIL and ADMIN_NOTIFY_EMAIL != email and pagamento_pronto:
        try:
            _enviar_email_acordo(
                destinatario    = ADMIN_NOTIFY_EMAIL,
                nome            = f"[NOTIFICAÇÃO ADMIN] {nome}",
                instituicao     = instituicao,
                valor           = valor,
                forma_pagamento = forma_pagamento,
                link_boleto     = link_boleto,
                link_pix        = link_pix,
                linha_dig       = linha_dig,
                vencimento      = vencimento,
            )
        except Exception as e:
            logger.error(f"[FORMALIZAR] erro ao enviar copia email admin: {e}")

    if ADMIN_NOTIFY_PHONE and ADMIN_NOTIFY_PHONE != phone and pagamento_pronto:
        try:
            _enviar_whatsapp_acordo(
                destinatario    = ADMIN_NOTIFY_PHONE,
                nome            = f"[NOTIFICAÇÃO ADMIN] {nome}",
                instituicao     = instituicao,
                valor           = valor,
                forma_pagamento = forma_pagamento,
                link_boleto     = link_boleto,
                link_pix        = link_pix,
                linha_dig       = linha_dig,
                vencimento      = vencimento,
            )
        except Exception as e:
            logger.error(f"[FORMALIZAR] erro ao enviar copia whatsapp admin: {e}")

    return {
        "n8n_sucesso": n8n_sucesso,
        "email_enviado": email_enviado,
        "wpp_enviado": wpp_enviado,
    }


@celery.task(name="tasks.verificar_boleto_acordo")
def verificar_boleto_acordo(dados: dict, tentativa: int = 1):
    link_boleto = dados.get("link_boleto", "")
    link_pix    = dados.get("link_pix", "")
    linha_dig   = dados.get("linha_dig", "")
    vencimento  = dados.get("vencimento", "")

    if not _has_payment_info(link_boleto, link_pix, linha_dig):
        pagamentos = _buscar_pagamento_acordo(dados)
        link_boleto = pagamentos.get("link_boleto", "")
        link_pix    = pagamentos.get("link_pix", "")
        linha_dig   = pagamentos.get("linha_dig", "")
        vencimento  = pagamentos.get("vencimento", "") or vencimento

    if not _has_payment_info(link_boleto, link_pix, linha_dig):
        if tentativa < BOLETO_RETRY_MAX:
            _agendar_verificacao_boleto(dados, tentativa + 1)
            return {"ok": True, "pending": True, "tentativa": tentativa}
        logger.warning(
            "[FORMALIZAR] boleto nao gerado apos %s tentativas cpf_final=%s nr_acordo=%s",
            tentativa,
            _norm_cpf(dados.get("cpf", ""))[-4:],
            dados.get("nr_acordo", ""),
        )
        return {"ok": False, "pending": True, "tentativa": tentativa}

    dados_atualizados = dict(dados)
    dados_atualizados.update({
        "link_boleto": link_boleto,
        "link_pix": link_pix,
        "linha_dig": linha_dig,
        "vencimento": vencimento,
    })

    notif = _disparar_notificacoes_acordo(dados_atualizados)
    email_enviado = notif["email_enviado"]
    wpp_enviado   = notif["wpp_enviado"]

    _atualizar_acordo_formalizado(dados_atualizados, {
        "link_boleto":   link_boleto,
        "link_pix":      link_pix,
        "linha_dig":     linha_dig,
        "vencimento":    vencimento,
        "email_enviado": email_enviado,
        "wpp_enviado":   wpp_enviado,
    })

    return {
        "ok": True,
        "pending": False,
        "email_enviado": email_enviado,
        "wpp_enviado": wpp_enviado,
        "link_boleto": link_boleto,
        "link_pix": link_pix,
        "linha_boleto": linha_dig,
    }


_FORMALIZANDO_CALLS_SET = set()
_FORMALIZANDO_LOCK = threading.Lock()

@celery.task(bind=True, max_retries=2, soft_time_limit=45, time_limit=60, name="tasks.formalizar_acordo")
def formalizar_acordo(self, dados: dict):
    """
    Disparada quando a Júlia formaliza um acordo.
    1. Busca iddev pelo CPF na API DDM
    2. Busca links de boleto/pix
    3. Envia para o n8n (com fallback direto via SMTP/Wavoip)
    4. Registra no Supabase
    """
    try:
        cpf             = dados.get("cpf", "")
        nome            = dados.get("nome", "Cliente")
        email           = dados.get("email", "")
        instituicao     = dados.get("instituicao", "")
        valor           = dados.get("valor", "")
        forma_pagamento = dados.get("forma_pagamento", "À vista")
        phone           = dados.get("phone") or dados.get("celular") or dados.get("telefone") or ""
        vapi_call_id    = dados.get("vapi_call_id", "")
        campaign_call_id = dados.get("campaign_call_id", "")
        debito          = dados.get("debito") or {}

        # Trava instantânea em memória para prevenir execuções concorrentes idênticas
        lock_key = vapi_call_id or f"{cpf}_{valor}_{forma_pagamento}"
        if lock_key:
            with _FORMALIZANDO_LOCK:
                if lock_key in _FORMALIZANDO_CALLS_SET:
                    logger.warning(f"[FORMALIZAR] Trava em memória acionada para key={lock_key}. Abortando execução duplicada.")
                    return {"ok": True, "duplicate": True}
                _FORMALIZANDO_CALLS_SET.add(lock_key)

        link_boleto = ""
        link_pix    = ""
        linha_dig   = ""
        vencimento  = ""
        nr_acordo   = ""

        # Prevenção de duplicidade: se este call_id já gravou um acordo, encerra imediatamente
        if vapi_call_id:
            existing = supabase.table("acordos_formalizados").select("id").eq("vapi_call_id", vapi_call_id).execute()
            if existing and existing.data:
                logger.warning(f"[FORMALIZAR] Acordo já formalizado anteriormente para vapi_call_id={vapi_call_id}. Abortando execução duplicada.")
                return {"ok": True, "duplicate": True}
        idcalc       = str(
            dados.get("idcalc") or
            dados.get("iddev") or
            debito.get("idcalc") or
            debito.get("iddev") or
            ""
        ).strip()

        if cpf:
            try:
                forma_pagamento = dados.get("forma_pagamento", "À vista")
                import re
                parc = dados.get("parc")
                if parc is None:
                    import re
                    parc = 1
                    digits = re.findall(r"\d+", forma_pagamento)
                    if digits:
                        parc = int(digits[0])
                acordo = _ddm_formalizar_acordo(cpf, parc=parc, debito=debito)
                link_boleto = acordo.get("link_boleto") or ""
                link_pix    = acordo.get("link_pix") or ""
                linha_dig   = acordo.get("linha_dig") or ""
                vencimento  = acordo.get("vencimento") or ""
                nr_acordo   = acordo.get("nr_acordo") or ""
                idcalc      = acordo.get("idcalc") or idcalc
                if acordo.get("valor"):
                    valor = acordo["valor"]
                if acordo.get("email") and not email:
                    email = acordo["email"]
                if acordo.get("nome") and nome == "Cliente":
                    nome = acordo["nome"]
            except Exception as e:
                import logging
                logging.warning("[DDM] erro ao formalizar acordo cpf_final=%s erro=%s", _norm_cpf(cpf)[-4:], e)

        # 1. Busca iddev e links de pagamento na API DDM
        if cpf and not _has_payment_info(link_boleto, link_pix, linha_dig):
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
                pass

        if cpf and not _has_payment_info(link_boleto, link_pix, linha_dig):
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
                if not _has_payment_info(link_boleto, link_pix, linha_dig):
                    import logging
                    logging.warning("[DDM] sem link de pagamento cpf_final=%s id=%s", _norm_cpf(cpf)[-4:], iddev)
            except Exception as e:
                import logging
                logging.warning("[DDM] erro no fallback de boleto cpf_final=%s erro=%s", _norm_cpf(cpf)[-4:], e)

        if cpf and not email:
            email = _ddm_get_email(cpf, idcalc)

        pagamento_pronto = _has_payment_info(link_boleto, link_pix, linha_dig)

        # 2. Dispara notificações (n8n Webhook com fallback para envio nativo)
        notif = _disparar_notificacoes_acordo({
            "cpf":             cpf,
            "nome":            nome,
            "email":           email,
            "phone":           phone,
            "instituicao":     instituicao,
            "valor":           valor,
            "forma_pagamento": forma_pagamento,
            "link_boleto":     link_boleto,
            "link_pix":        link_pix,
            "linha_dig":       linha_dig,
            "vencimento":      vencimento,
            "nr_acordo":       nr_acordo,
            "vapi_call_id":    vapi_call_id,
        })
        email_enviado = notif["email_enviado"]
        wpp_enviado   = notif["wpp_enviado"]

        # 3. Registra acordo no Supabase
        acordo_payload = {
            "cpf":             cpf,
            "nome":            nome,
            "email":           email,
            "phone":           phone,
            "instituicao":     instituicao,
            "valor":           valor,
            "forma_pagamento": forma_pagamento,
            "link_boleto":     link_boleto,
            "link_pix":        link_pix,
            "linha_dig":       linha_dig,
            "vencimento":      vencimento,
            "nr_acordo":       nr_acordo,
            "email_enviado":   email_enviado,
            "wpp_enviado":     wpp_enviado,
            "vapi_call_id":    vapi_call_id or None,
            "campaign_call_id": campaign_call_id or None,
        }
        try:
            _salvar_acordo_formalizado(acordo_payload)
        except Exception as e:
            logger.error(f"[FORMALIZAR] erro ao salvar no Supabase: {e}")

        if (email or phone) and not pagamento_pronto:
            _agendar_verificacao_boleto({
                "cpf":             cpf,
                "nome":            nome,
                "email":           email,
                "phone":           phone,
                "instituicao":     instituicao,
                "valor":           valor,
                "forma_pagamento": forma_pagamento,
                "vencimento":      vencimento,
                "nr_acordo":       nr_acordo,
                "idcalc":          idcalc,
                "iddev":           idcalc,
                "debito":          debito,
                "vapi_call_id":    vapi_call_id,
                "campaign_call_id": campaign_call_id,
            })

        return {
            "ok":            True,
            "email_enviado": email_enviado,
            "wpp_enviado":   wpp_enviado,
            "link_pix":      link_pix,
            "link_boleto":   link_boleto,
            "linha_boleto":  linha_dig,
            "nr_acordo":     nr_acordo,
            "boleto_pendente": bool((email or phone) and not pagamento_pronto),
        }

    except Exception as exc:
        raise self.retry(exc=exc, countdown=30)

@celery.task(bind=True, name="tasks.process_import")
def process_import(self, job_id: str, rows: list):
    processed = 0
    with_debt = 0
    result    = []

    for row in rows:
        if _is_import_stopped(job_id):
            return
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
        if _is_import_stopped(job_id):
            return
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

    try:
        if _is_import_stopped(job_id):
            return
        r = redis_client()
        val = r.get(f"import:{file_id}")
        data = None

        if isinstance(val, (bytes, bytearray)):
            data = bytes(val)
        elif isinstance(val, str):
            if os.path.exists(val):
                with open(val, "rb") as f:
                    data = f.read()
            else:
                data = val.encode("utf-8")
        elif os.path.exists(file_id):
            with open(file_id, "rb") as f:
                data = f.read()

        if not data:
            _set_job_error(job_id, "Arquivo não encontrado ou expirado.")
            return

        try:
            data.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            encoding = "latin-1"

        buf = io.BytesIO(data)
        if fname.endswith(".csv") or fname.endswith(".txt"):
            df = pd.read_csv(buf, dtype=str, sep=None, engine="python", encoding=encoding)
        else:
            df = pd.read_excel(buf, dtype=str)

        try:
            r.delete(f"import:{file_id}")
        except Exception:
            pass


        if _is_import_stopped(job_id):
            return

        # Limpeza preventiva: remove linhas que não têm CPF válido
        # (evita contar milhares de linhas em branco vazias do Excel como ativas)
        df = df.dropna(how="all")
        orig_len = len(df)
        cols_lower = [str(c).strip().lower() for c in df.columns]
        cpf_col = None
        for cand in ["cpf", "cpfcgc"]:
            for c, cl in zip(df.columns, cols_lower):
                if cand == cl:
                    cpf_col = c
                    break
            if cpf_col:
                break
        if not cpf_col:
            for cand in ["cpf", "cpfcgc"]:
                for c, cl in zip(df.columns, cols_lower):
                    if cand in cl:
                        cpf_col = c
                        break
                if cpf_col:
                    break
        
        len_after_cpf = -1
        sample_rows = []
        if cpf_col:
            sample_rows = df[cpf_col].head(10).tolist()
            df = df[df[cpf_col].notna()]
            df = df[df[cpf_col].astype(str).str.strip().str.replace(r'\D', '', regex=True).str.len() > 0]
            len_after_cpf = len(df)

        import json as json_lib
        try:
            r.setex("import_debug_info", 3600, json_lib.dumps({
                "orig_len": orig_len,
                "cols": list(df.columns),
                "cpf_col": cpf_col,
                "len_after_cpf": len_after_cpf,
                "sample_cpfs": [str(x) for x in sample_rows]
            }))
        except Exception:
            pass

        if len(df) > 20000:
            _set_job_error(job_id, f"Arquivo com {len(df)} linhas excede o limite atual de 20000 linhas.")
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
    _set_job_error(job_id, "Importacao via Supabase Storage foi desativada. Use /api/import/upload.")
    return {"ok": False, "error": "storage_disabled"}

    import pandas as pd

    try:
        if _is_import_stopped(job_id):
            return {"ok": False, "stopped": True}
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

        if _is_import_stopped(job_id):
            return {"ok": False, "stopped": True}

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
        if _is_import_stopped(job_id):
            return {"ok": False, "stopped": True}

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

    if _is_import_stopped(job_id):
        return

    df.columns = [str(c).strip() for c in df.columns]
    cols_lower = [c.lower() for c in df.columns]

    def first_col(candidates):
        for cand in candidates:
            for c, cl in zip(df.columns, cols_lower):
                if cand == cl:
                    return c
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
    val_col = first_col(["val_atualizado", "val_nominal", "val_atua", "val_nom"])
    is_ddm = val_col is not None

    # Detecta colunas de valor e instituição para fallback
    valor_col = first_col(["val_atualizado", "val_nominal", "nominal", "valor", "nominal_princ", "val_atua", "val_nom"])
    inst_col = first_col(["instituicao", "cliente", "carteira", "ies"])

    if not cpf_col or not nome_col:
        _set_job_error(job_id, f"Colunas obrigatórias não encontradas. Colunas detectadas: {list(df.columns)}")
        return

    pending_rows = []
    for loop_i, (idx, r) in enumerate(df.iterrows()):
        if loop_i % 500 == 0:
            if _is_import_stopped(job_id):
                return
        row = {str(k): ("" if pd.isna(v) else str(v).strip()) for k, v in r.items()}
        cpf_norm = _norm_cpf(row.get(cpf_col, ""))
        if not cpf_norm:
            continue

        nome = row.get(nome_col, "").strip() or ""
        
        # Pega valores da planilha para fallback caso a DDM falhe em tempo real
        nominal_val = row.get(val_col or valor_col, "").strip() if (val_col or valor_col) else ""
        inst_val = row.get(inst_col, "").strip() if inst_col else ""

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
                "nominal": nominal_val or "0,00",
                "nominal_princ": nominal_val or "0,00",
                "instituicao": inst_val or "Faculdade",
            }
            if phone_col and phone_col in row:
                meta["raw_phone_fallback"] = row.get(phone_col, "").strip()
        else:
            phone = _first_phone_hires(row, list(df.columns)) if phone_col else ""
            meta = {
                "nominal": nominal_val or "0,00",
                "nominal_princ": nominal_val or "0,00",
                "instituicao": inst_val or "Faculdade",
            }

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

    if _is_import_stopped(job_id):
        return

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
    if _is_import_stopped(job_id):
        return
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
        for future_i, future in enumerate(as_completed(futures)):
            if future_i % 100 == 0:
                if _is_import_stopped(job_id):
                    return
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
        if _is_import_stopped(job_id):
            return
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
        _dispatch_task(
            recheck_import_errors_task,
            args=[job_id, DDM_ERROR_RECHECK_ROUNDS],
            countdown=DDM_ERROR_RECHECK_DELAY_SECONDS,
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
        _dispatch_task(
            recheck_import_errors_task,
            args=[job_id, rounds_left - 1],
            countdown=DDM_ERROR_RECHECK_DELAY_SECONDS,
        )

    return {"ok": True, "rechecked": len(error_rows), "errors": remaining_errors}

