import os
import re
import time
import requests
from celery import Celery
from supabase import create_client
from typing import Optional, Dict, Any

REDIS_URL         = os.getenv("REDIS_URL", "redis://localhost:6379/0")
VAPI_API_KEY      = os.getenv("VAPI_API_KEY")
VAPI_ASSISTANT_ID = os.getenv("VAPI_ASSISTANT_ID")
VAPI_BASE         = "https://api.vapi.ai"
WAVOIP_EMAIL      = os.getenv("WAVOIP_EMAIL")
WAVOIP_PASSWORD   = os.getenv("WAVOIP_PASSWORD")
WAVOIP_BASE       = "https://api.wavoip.com"
SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_KEY")

DDM_TOKEN    = os.getenv("DDM_TOKEN", "2e30b68c0feda298f9d6d40ab36c1a09")
DDM_BASE_URL = "https://www.ddmacordos.com"
DDM_CALCULA  = f"{DDM_BASE_URL}/ws_ddm/ws/CalculaDebitos.php"
DDM_CALC_ID  = f"{DDM_BASE_URL}/calc/"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

celery = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)
celery.conf.update(
    task_serializer            = "json",
    result_serializer          = "json",
    accept_content             = ["json"],
    task_acks_late             = True,
    task_reject_on_worker_lost = True,
    worker_prefetch_multiplier = 1,
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


def wavoip_get_devices(token: str) -> list:
    res = requests.get(f"{WAVOIP_BASE}/v2/devices/me",
        headers={"Authorization": f"Bearer {token}"}, timeout=10)
    res.raise_for_status()
    return res.json().get("data", [])


def get_healthy_lines() -> list:
    token      = wavoip_login()
    devices    = wavoip_get_devices(token)
    device_map = {d.get("token"): d for d in devices}
    healthy    = []
    for t in DEVICE_PRIORITY:
        if t not in device_map:
            continue
        d = device_map[t]
        if d.get("status") != "open":   continue
        if d.get("disabled") != 0:      continue
        if d.get("phone") is None:      continue
        if not WAVOIP_VAPI_MAP.get(t):  continue
        healthy.append(d)
    return healthy


_rr_idx = 0

def _get_phone_number_id() -> Optional[str]:
    global _rr_idx
    healthy = get_healthy_lines()
    if not healthy:
        return None
    line     = healthy[_rr_idx % len(healthy)]
    _rr_idx += 1
    return WAVOIP_VAPI_MAP.get(line.get("token"))


# ── DDM ────────────────────────────────────────────────────────

def consultar_debitos_cpf(cpf: str) -> Dict:
    url = f"{DDM_CALCULA}?tk={DDM_TOKEN}&Doc={cpf}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"ERRO": str(e)}


def consultar_detalhes_iddev(idcalc: str) -> Dict:
    url = f"{DDM_CALC_ID}?tk={DDM_TOKEN}&idDev={idcalc}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"ERRO": str(e)}


def processar_debito(cpf: str) -> Optional[Dict]:
    """
    Retorna dict com dados do débito ou None se sem débito/erro.
    """
    dados_cpf = consultar_debitos_cpf(cpf)
    if dados_cpf.get("ERRO"):
        return None

    idcalc = dados_cpf.get("idcalc")
    if not idcalc:
        return None

    detalhes = consultar_detalhes_iddev(idcalc)
    if detalhes.get("ERRO"):
        return None

    # Verifica se tem débito real
    lista_parcelas = detalhes.get("ListaParcelas", {}).get("Parcelas", [])
    if isinstance(lista_parcelas, dict):
        lista_parcelas = [lista_parcelas]
    if not lista_parcelas:
        return None
    primeira = lista_parcelas[0] if lista_parcelas else {}
    if primeira.get("ValorParcela", "0,00") == "0,00":
        return None

    # Extrai dados
    def _last(obj):
        return obj[-1] if isinstance(obj, list) and obj else (obj if isinstance(obj, dict) else {})

    dados          = detalhes.get("Dados", {})
    pgto_avista    = _last(detalhes.get("PgtoAvista", {}))
    pgto_boleto    = _last(detalhes.get("PgtoParceladoBoleto", {}))
    pgto_cartao    = _last(detalhes.get("PgtoParceladoCartao", {}))

    nome_cliente = dados.get("Cliente", "")
    nome_cliente = re.sub(r'\bNOVO\b', '', nome_cliente, flags=re.IGNORECASE).strip()

    lista_debitos = dados_cpf.get("ListaDebitos", {}).get("Debito", [])
    if isinstance(lista_debitos, dict):
        lista_debitos = [lista_debitos]

    return {
        "instituicao":            nome_cliente,
        "nome_devedor":           dados.get("NomeDevedor", ""),
        "numero_debitos":         str(len(lista_debitos)),
        "idcalc":                 idcalc,
        # à vista
        "PgtoAvista": {
            "ValorTotal":     pgto_avista.get("ValorTotal", "0,00"),
            "PercDesconto":   pgto_avista.get("PercDesconto", "0"),
            "ValorDesconto":  pgto_avista.get("ValorDesconto", "0,00"),
            "ValorFinal":     pgto_avista.get("ValorFinal", "0,00"),
        },
        # boleto parcelado
        "CalculoBoleto": {
            "SubtotalBoleto":    pgto_boleto.get("Valor", "0,00"),
            "HonorarioBoleto":   pgto_boleto.get("ValorDesconto", "0,00"),
            "ValorCobrarBoleto": pgto_boleto.get("ValorParcela", "0,00"),
        },
        "ParcelasBoleto": pgto_boleto.get("Parcelas", "0"),
        # cartão parcelado
        "PgtoParceladoCartao": {
            "Parcelas":     pgto_cartao.get("Parcelas", "0"),
            "ValorParcela": pgto_cartao.get("ValorParcela", "0,00"),
            "ValorFinal":   pgto_cartao.get("ValorFinal", "0,00"),
        },
    }


# ── VAPI ───────────────────────────────────────────────────────

def vapi_call(phone: str, cpf: str = "", name: str = "") -> Dict:
    digits = ''.join(filter(str.isdigit, phone))
    if not digits.startswith("55"):
        digits = "55" + digits
    phone_e164 = "+" + digits

    healthy = get_healthy_lines()
    if not healthy:
        raise Exception("Nenhuma linha disponível")

    global _rr_idx
    start    = _rr_idx % len(healthy)
    _rr_idx += 1
    last_err = None

    # Consulta débito DDM
    debito = processar_debito(cpf) if cpf else None

    for i in range(len(healthy)):
        line     = healthy[(start + i) % len(healthy)]
        phone_id = WAVOIP_VAPI_MAP.get(line.get("token"))
        if not phone_id:
            continue

        # Monta payload base
        payload: Dict[str, Any] = {
            "phoneNumberId": phone_id,
            "assistantId":   VAPI_ASSISTANT_ID,
            "customer":      {"number": phone_e164, "name": name},
        }

        # Se tem débito, injeta variableValues
        if debito:
            primeiro_nome = name.split()[0] if name else ""
            payload["assistantOverrides"] = {
                "variableValues": {
                    "instituicao":          debito["instituicao"],
                    "Valorcpf":             cpf,
                    "NominalPrinc":         debito["PgtoAvista"]["ValorTotal"],
                    "PgtoAvista":           debito["PgtoAvista"],
                    "CalculoBoleto":        debito["CalculoBoleto"],
                    "ParcelasBoleto":       debito["ParcelasBoleto"],
                    "PgtoParceladoCartao":  debito["PgtoParceladoCartao"],
                }
            }

        try:
            r = requests.post(f"{VAPI_BASE}/call/phone", json=payload,
                headers={
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


# ── TASK ───────────────────────────────────────────────────────

@celery.task(bind=True, max_retries=0, name="tasks.make_call")
def make_call_task(self, campaign_id: str, contact: dict):
    phone  = contact.get("phone", "").strip()
    cpf    = contact.get("cpf", "")
    name   = contact.get("name", "")
    row_id = contact.get("row_id")

    if not phone:
        _update_result(row_id, "sem_telefone", None, None)
        return

    # Pausa automática se não há linhas
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

    # Consulta débito antes de ligar
    debito = processar_debito(cpf) if cpf else None
    if debito is None and cpf:
        # Sem débito confirmado pela API DDM
        _update_result(row_id, "sem_debito", None, phone)
        return

    try:
        data    = vapi_call(phone, cpf=cpf, name=name)
        call_id = data.get("id")
        _update_result(row_id, "em_andamento", call_id, phone)
    except Exception as ex:
        _update_result(row_id, "erro", None, phone, str(ex))


def _update_result(row_id, status, call_id, phone, error=None):
    try:
        update = {"status": status}
        if call_id: update["vapi_call_id"] = call_id
        if phone:   update["phone"]         = phone
        if error:   update["error"]         = error
        supabase.table("campaign_calls").update(update).eq("id", row_id).execute()
    except Exception:
        pass


def _save_result(campaign_id, cpf, status, call_id, phone, error=None):
    try:
        supabase.table("campaign_calls").insert({
            "campaign_id":  campaign_id,
            "cpf":          cpf,
            "phone":        phone,
            "status":       status,
            "vapi_call_id": call_id,
            "error":        error,
        }).execute()
    except Exception:
        pass