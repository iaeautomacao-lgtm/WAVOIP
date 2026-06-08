import os
import time
import requests
from celery import Celery
from supabase import create_client

REDIS_URL         = os.getenv("REDIS_URL", "redis://localhost:6379/0")
VAPI_API_KEY      = os.getenv("VAPI_API_KEY")
VAPI_ASSISTANT_ID = os.getenv("VAPI_ASSISTANT_ID")
VAPI_BASE         = "https://api.vapi.ai"
WAVOIP_EMAIL      = os.getenv("WAVOIP_EMAIL")
WAVOIP_PASSWORD   = os.getenv("WAVOIP_PASSWORD")
WAVOIP_BASE       = "https://api.wavoip.com"
SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

celery = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)
celery.conf.update(
    task_serializer     = "json",
    result_serializer   = "json",
    accept_content      = ["json"],
    task_acks_late      = True,        # reprocessa se worker cair
    task_reject_on_worker_lost = True,
    worker_prefetch_multiplier = 1,    # 1 job por vez por worker
)

WAVOIP_VAPI_MAP = {
    "88b232ad-5c1d-404f-8652-f5399e6a6f51": os.getenv("VAPI_PHONE_NUMBER_ID_1"),
    "ed49616d-fb19-46df-96b8-decab4cde3cf": os.getenv("VAPI_PHONE_NUMBER_ID_2"),
    "c8af8686-ca83-4eef-b757-f53940426011": os.getenv("VAPI_PHONE_NUMBER_ID_3"),
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43": os.getenv("VAPI_PHONE_NUMBER_ID_4"),
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb": os.getenv("VAPI_PHONE_NUMBER_ID_5"),
}

DEVICE_PRIORITY = [
    "88b232ad-5c1d-404f-8652-f5399e6a6f51",
    "ed49616d-fb19-46df-96b8-decab4cde3cf",
    "c8af8686-ca83-4eef-b757-f53940426011",
    "49d7328f-13ab-469f-b8a9-b7eb2b713f43",
    "8d739b23-77ed-4eb7-b807-e81270eb4ddb",
]

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
        if not WAVOIP_VAPI_MAP.get(t):  # 🆕 pula se não tem phoneNumberId mapeado
            continue
        healthy.append(d)

    return healthy
_rr_idx = 0

def vapi_call(phone: str) -> dict:
    global _rr_idx
    digits = ''.join(filter(str.isdigit, phone))
    if not digits.startswith("55"):
        digits = "55" + digits
    phone_e164 = "+" + digits

    healthy = get_healthy_lines()
    if not healthy:
        raise Exception("Nenhuma linha disponível")

    start     = _rr_idx % len(healthy)
    _rr_idx  += 1
    last_err  = None

    for i in range(len(healthy)):
        line     = healthy[(start + i) % len(healthy)]
        phone_id = WAVOIP_VAPI_MAP.get(line.get("token"))
        if not phone_id:
            continue
        try:
            r = requests.post(f"{VAPI_BASE}/call/phone", json={
                "phoneNumberId": phone_id,
                "assistantId":   VAPI_ASSISTANT_ID,
                "customer":      {"number": phone_e164}
            }, headers={
                "Authorization": f"Bearer {VAPI_API_KEY}",
                "Content-Type":  "application/json"
            }, timeout=15)
            if r.ok:
                return r.json()
            try:    detail = r.json()
            except: detail = r.text
            last_err = f"{line.get('name')} — {r.status_code}: {detail}"
        except Exception as ex:
            last_err = f"{line.get('name')} — {str(ex)}"

    raise Exception(f"Todas as linhas falharam: {last_err}")


@celery.task(bind=True, max_retries=0, name="tasks.make_call")
def make_call_task(self, campaign_id: str, contact: dict):
    """
    Tarefa de uma ligação.
    - Verifica se há linhas disponíveis
    - Se não houver, aguarda até 30min checando a cada 30s (pausa automática)
    - Se voltar linha, retoma
    - Se não voltar em 30min, marca como falha
    """
    phone = contact.get("phone", "").strip()
    cpf   = contact.get("cpf", "")

    if not phone:
        _save_result(campaign_id, cpf, "sem_telefone", None, None)
        return

    # ── Pausa automática se todas as linhas caírem ──
    MAX_WAIT   = 30 * 60   # 30 minutos máximo esperando
    CHECK_INT  = 30        # verifica a cada 30 segundos
    waited     = 0

    while waited < MAX_WAIT:
        try:
            healthy = get_healthy_lines()
            if healthy:
                break
        except Exception:
            pass
        time.sleep(CHECK_INT)
        waited += CHECK_INT
    else:
        _save_result(campaign_id, cpf, "falha_sem_linha", None, None)
        return

    # ── Dispara a ligação ──
    try:
        data = vapi_call(phone)
        _save_result(campaign_id, cpf, "disparado", data.get("id"), phone)
    except Exception as ex:
        _save_result(campaign_id, cpf, "erro", None, phone, str(ex))


def _save_result(campaign_id, cpf, status, call_id, phone, error=None):
    try:
        supabase.table("campaign_calls").insert({
            "campaign_id": campaign_id,
            "cpf":         cpf,
            "phone":       phone,
            "status":      status,
            "vapi_call_id":call_id,
            "error":       error,
        }).execute()
    except Exception:
        pass