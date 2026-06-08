import os
import re
import time
import requests
from celery import Celery
from supabase import create_client
from typing import Optional, Dict, Any
from ddm import processar_debito as _processar_debito

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
    task_serializer                    = "json",
    result_serializer                  = "json",
    accept_content                     = ["json"],
    task_acks_late                     = True,
    task_reject_on_worker_lost         = True,
    worker_prefetch_multiplier         = 1,
    broker_connection_retry_on_startup = True,
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


def get_healthy_lines() -> list:
    token      = wavoip_login()
    devices    = wavoip_get_devices(token)
    device_map = {d.get("token"): d for d in devices}
    healthy    = []
    for t in DEVICE_PRIORITY:
        if t not in device_map:         continue
        d = device_map[t]
        if d.get("status") != "open":  continue
        if d.get("disabled") != 0:     continue
        if d.get("phone") is None:     continue
        if not WAVOIP_VAPI_MAP.get(t): continue
        healthy.append(d)
    return healthy


def vapi_call(phone: str, cpf: str = "", name: str = "", debito: dict = None) -> Dict:
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

    for i in range(len(healthy)):
        line     = healthy[(start + i) % len(healthy)]
        phone_id = WAVOIP_VAPI_MAP.get(line.get("token"))
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
                return r.json()
            try:    detail = r.json()
            except: detail = r.text
            last_err = f"{line.get('name')} — {r.status_code}: {detail}"
        except Exception as ex:
            last_err = f"{line.get('name')} — {str(ex)}"

    raise Exception(f"Todas as linhas falharam: {last_err}")


@celery.task(bind=True, max_retries=0, name="tasks.make_call")
def make_call_task(self, campaign_id: str, contact: dict):
    phone  = contact.get("phone", "").strip()
    cpf    = contact.get("cpf", "")
    name   = contact.get("name", "")
    row_id = contact.get("row_id")
    debito = contact.get("debito_data")

    if not phone:
        _update_result(row_id, "sem_telefone", None, None)
        return

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

    try:
        data    = vapi_call(phone, cpf=cpf, name=name, debito=debito)
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
def process_file(self, job_id: str, file_path: str):
    import pandas as pd

    def _norm_cpf(cpf):
        return re.sub(r'\D', '', str(cpf)).zfill(11) if cpf else ""

    def _get_phone(r, cols):
        for col in ["fone1", "tel", "telefone", "celular", "phone"]:
            if col in cols:
                val = str(r.get(col, "") or "").strip()
                if val and val != "nan":
                    return val
        return ""

    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path, dtype=str, sep=None, engine="python")
        else:
            df = pd.read_excel(file_path, dtype=str)

        df.columns = [c.strip().lower() for c in df.columns]
        cols     = list(df.columns)
        cpf_col  = next((c for c in cols if "cpf" in c), None)
        nome_col = next((c for c in cols if "nome" in c or "name" in c), None)
        val_col  = next((c for c in cols if "val_atualizado" in c or "val_nominal" in c), None)
        is_ddm   = val_col is not None

        if not cpf_col or not nome_col:
            supabase.table("import_jobs").update({"status": "error"}).eq("id", job_id).execute()
            return

        total     = 0
        with_debt = 0
        rows      = []

        for _, r in df.iterrows():
            cpf_norm = _norm_cpf(str(r.get(cpf_col, "") or ""))
            if not cpf_norm:
                continue

            phone = _get_phone(r, cols)
            total += 1

            if is_ddm:
                val_str = str(r.get(val_col, "0") or "0").strip().replace(",", ".")
                try:
                    tem_debito = float(val_str) > 0
                except Exception:
                    tem_debito = False

                if tem_debito and phone:
                    with_debt += 1
                    debito = {
                        "instituicao":    str(r.get("carteira", "") or "").strip(),
                        "nome_devedor":   str(r.get(nome_col, "") or "").strip(),
                        "numero_debitos": str(r.get("parcelas", "1") or "1").strip(),
                        "PgtoAvista": {
                            "ValorTotal":    str(r.get("val_nominal", "0,00") or "0,00").strip(),
                            "PercDesconto":  "0",
                            "ValorDesconto": "0,00",
                            "ValorFinal":    val_str.replace(".", ","),
                        },
                        "CalculoBoleto": {
                            "SubtotalBoleto":    val_str.replace(".", ","),
                            "HonorarioBoleto":   "0,00",
                            "ValorCobrarBoleto": "0,00",
                        },
                        "ParcelasBoleto": str(r.get("parcelas", "1") or "1").strip(),
                        "PgtoParceladoCartao": {
                            "Parcelas":     str(r.get("parcelas", "1") or "1").strip(),
                            "ValorParcela": "0,00",
                            "ValorFinal":   val_str.replace(".", ","),
                        },
                    }
                    rows.append({
                        "cpf":      cpf_norm,
                        "name":     str(r.get(nome_col, "") or "").strip(),
                        "phone":    phone,
                        "has_debt": True,
                        "debito":   debito,
                    })
            else:
                debito = _processar_debito(cpf_norm)
                if debito:
                    with_debt += 1
                if phone:
                    rows.append({
                        "cpf":      cpf_norm,
                        "name":     str(r.get(nome_col, "") or "").strip(),
                        "phone":    phone,
                        "has_debt": debito is not None,
                        "debito":   debito,
                    })

            if total % 1000 == 0:
                try:
                    supabase.table("import_jobs").update({
                        "total":     total,
                        "processed": total,
                        "with_debt": with_debt,
                    }).eq("id", job_id).execute()
                except Exception:
                    pass

        supabase.table("import_jobs").update({
            "status":    "done",
            "total":     total,
            "processed": total,
            "with_debt": with_debt,
            "result":    rows,
        }).eq("id", job_id).execute()

    except Exception:
        supabase.table("import_jobs").update({"status": "error"}).eq("id", job_id).execute()
    finally:
        try:
            os.remove(file_path)
        except Exception:
            pass