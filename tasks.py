import os
import re
import io
import time
import requests
from celery import Celery
from supabase import create_client
from typing import Optional, Dict, Any
from ddm import processar_debito as _processar_debito

REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379/0")
VAPI_API_KEY         = os.getenv("VAPI_API_KEY")
VAPI_ASSISTANT_ID    = os.getenv("VAPI_ASSISTANT_ID")
VAPI_BASE            = "https://api.vapi.ai"
WAVOIP_EMAIL         = os.getenv("WAVOIP_EMAIL")
WAVOIP_PASSWORD      = os.getenv("WAVOIP_PASSWORD")
WAVOIP_BASE          = "https://api.wavoip.com"
SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_KEY         = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY"))
SUPABASE_BUCKET      = os.getenv("SUPABASE_BUCKET", "imports")

# ── Email (SMTP) ──────────────────────────────────────────────
SMTP_HOST     = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM     = os.getenv("SMTP_FROM", "atendimento@ddm.adv.br")
SMTP_TIMEOUT  = int(os.getenv("SMTP_TIMEOUT", "10"))
SMTP_SECURITY = os.getenv("SMTP_SECURITY", "starttls").lower()

# ── DDM Acordos ───────────────────────────────────────────────
DDM_TOKEN    = os.getenv("DDM_TOKEN", "2e30b68c0feda298f9d6d40ab36c1a09")
DDM_BASE     = "https://ddmacordos.com"

supabase       = create_client(SUPABASE_URL, SUPABASE_KEY)
supabase_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

WATCHDOG_TIMEOUT_MIN = int(os.getenv("WATCHDOG_TIMEOUT_MIN", "8"))   # minutos sem webhook → re-dispara
WATCHDOG_MAX_RETRIES = int(os.getenv("WATCHDOG_MAX_RETRIES", "3"))    # tentativas antes de marcar erro

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


# ── HELPERS ───────────────────────────────────────────────────

def _norm_cpf(cpf) -> str:
    return re.sub(r'\D', '', str(cpf)).zfill(11) if cpf else ""


def _get_phone(row, cols: list) -> str:
    """Busca o primeiro telefone válido nas colunas da planilha."""
    for col in ["fone1", "tel", "telefone", "celular", "phone"]:
        if col in cols:
            val = str(row.get(col, "") or "").strip()
            if val and val != "nan":
                return val
    return ""


def _get_phones_ddm(row) -> list:
    """Extrai todos os telefones FONE1..FONE10 de planilha DDM."""
    phones = []
    for i in range(1, 11):
        key = f"fone{i}"
        val = str(row.get(key, "") or "").strip()
        if val and val != "nan":
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


# ── TASKS ─────────────────────────────────────────────────────

@celery.task(bind=True, max_retries=0, name="tasks.make_call")
def make_call_task(self, campaign_id: str, contact: dict):
    cpf    = contact.get("cpf", "")
    name   = contact.get("name", "")
    row_id = contact.get("row_id")
    debito = contact.get("debito_data") or {}

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

    # Aguarda linha SIP disponivel (max 30min)
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
            data    = vapi_call(phone, cpf=cpf, name=name, debito=debito)
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
    _update_result(row_id, "erro", None, phones[0], f"todos os fones falharam — {last_error}")




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
                    .in_("status", ["em_andamento", "enfileirado"]) \
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
                        _watchdog_dispatch(campaign_id, pending[0],
                                           pending[0].get("watchdog_retries") or 0,
                                           reason="sem_ativa")
                        rescued += 1
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
    """Re-enfileira uma chamada travada incrementando o contador de retries."""
    try:
        supabase.table("campaign_calls").update({
            "status":           "enfileirado",
            "watchdog_retries": current_retries + 1,
            "error":            f"watchdog/{reason} retry #{current_retries + 1}",
        }).eq("id", row["id"]).execute()

        make_call_task.delay(campaign_id, {
            "row_id":      row["id"],
            "cpf":         row.get("cpf", ""),
            "phone":       row.get("phone", ""),
            "name":        row.get("name", ""),
            "debito_data": row.get("debito_data"),
        })
    except Exception:
        pass


def _advance_campaign(campaign_id: str, current_order_idx: int):
    """Pula para o proximo contato pendente apos esgotar retries."""
    try:
        next_row = supabase.table("campaign_calls") \
            .select("id, cpf, phone, name, debito_data, order_idx, watchdog_retries") \
            .eq("campaign_id", campaign_id) \
            .eq("status", "pendente") \
            .gt("order_idx", current_order_idx) \
            .order("order_idx") \
            .limit(1) \
            .execute().data

        if next_row:
            row = next_row[0]
            supabase.table("campaign_calls").update({"status": "enfileirado"}) \
                .eq("id", row["id"]).execute()
            make_call_task.delay(campaign_id, {
                "row_id":      row["id"],
                "cpf":         row.get("cpf", ""),
                "phone":       row.get("phone", ""),
                "name":        row.get("name", ""),
                "debito_data": row.get("debito_data"),
            })
        else:
            _check_campaign_completion(campaign_id)
    except Exception:
        pass


def _check_campaign_completion(campaign_id: str):
    """Verifica se todos os contatos foram processados e finaliza a campanha."""
    try:
        remaining = supabase.table("campaign_calls") \
            .select("id", count="exact") \
            .eq("campaign_id", campaign_id) \
            .in_("status", ["pendente", "enfileirado", "em_andamento"]) \
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
    url = f"{DDM_BASE}/calc/localiza_dev.php?tk={DDM_TOKEN}&cpf={cpf}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
    # Retorna o iddev/idcalc
    return str(data.get("idcalc") or data.get("iddev") or data.get("id") or "")


def _ddm_get_payment_links(iddev: str) -> dict:
    """Busca links de boleto/pix pelo iddev."""
    url = f"{DDM_BASE}/calc/?tk={DDM_TOKEN}&idDev={iddev}&cli=ddm"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()

    # Navega pela estrutura de Acordos para encontrar acordo_pagamento
    link_boleto = ""
    link_pix    = ""
    linha_dig   = ""
    vencimento  = ""

    acordos_raw = data.get("Acordos", [])
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
    if not link_boleto:
        boleto_obj = data.get("boleto", {})
        link_boleto = str(boleto_obj.get("link") or "").strip()
        linha_dig   = str(boleto_obj.get("linhaDigitavel") or "").strip()
        vencimento  = str(boleto_obj.get("vencimento") or "").strip()
    if not link_pix:
        pix_obj = data.get("pix", {})
        link_pix = str(pix_obj.get("link") or "").strip()

    return {
        "link_boleto": link_boleto,
        "link_pix":    link_pix,
        "linha_dig":   linha_dig,
        "vencimento":  vencimento,
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

    secao_boleto = f"""
    <tr>
      <td style="padding:8px 0;">
        <b style="color:#101828;">Boleto:</b><br>
        <a href="{link_boleto}" style="color:#FF5706;">{link_boleto}</a>
        {f'<br><span style="color:#667085;font-size:13px;">Linha digitável: {linha_dig}</span>' if linha_dig else ""}
        {f'<br><span style="color:#667085;font-size:13px;">Vencimento: {vencimento}</span>' if vencimento else ""}
      </td>
    </tr>""" if link_boleto else ""

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
  {"<p style='color:#101828;font-weight:bold;margin:0 0 12px;'>Links para pagamento:</p><table width='100%' cellpadding='0' cellspacing='0'>" + secao_pix + secao_boleto + "</table>" if (link_pix or link_boleto) else ""}

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

    if SMTP_SECURITY == "ssl":
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

        link_boleto = ""
        link_pix    = ""
        linha_dig   = ""
        vencimento  = ""

        # 1. Busca iddev e links de pagamento na API DDM
        if cpf:
            try:
                iddev = _ddm_get_iddev(cpf)
                if iddev:
                    pagamentos = _ddm_get_payment_links(iddev)
                    link_boleto = pagamentos["link_boleto"]
                    link_pix    = pagamentos["link_pix"]
                    linha_dig   = pagamentos["linha_dig"]
                    vencimento  = pagamentos["vencimento"]
            except Exception as e:
                # Não bloqueia o envio do email se a API DDM falhar
                pass

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

        # Gera URL assinada para download (1h de validade)
        # Usa requests direto na API REST do Supabase Storage — bypassa o SDK
        # (o SDK storage3 retorna formato inconsistente entre versoes)
        import json as _json
        storage_host = SUPABASE_URL.rstrip("/")
        sign_resp = requests.post(
            f"{storage_host}/storage/v1/object/sign/{SUPABASE_BUCKET}/{storage_path}",
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Content-Type": "application/json",
            },
            json={"expiresIn": 3600},
            timeout=15,
        )
        sign_resp.raise_for_status()
        sign_data = sign_resp.json()
        # API REST retorna: {"signedURL": "/storage/v1/object/sign/...?token=..."}
        signed_path = sign_data.get("signedURL") or sign_data.get("signedUrl") or sign_data.get("signed_url", "")
        if not signed_path:
            raise Exception(f"Storage API nao retornou URL. Resposta: {sign_data}")
        # Monta URL completa — garante que /storage/v1 está presente
        if signed_path.startswith("http"):
            download_url = signed_path
        elif signed_path.startswith("/storage/v1"):
            download_url = f"{storage_host}{signed_path}"
        elif signed_path.startswith("/object"):
            download_url = f"{storage_host}/storage/v1{signed_path}"
        else:
            download_url = f"{storage_host}/storage/v1/object/sign/{signed_path}"

        # Download com stream para não estourar memória no header
        resp = requests.get(download_url, timeout=300)
        resp.raise_for_status()

        # Detecta encoding — planilha DDM geralmente vem em latin-1
        raw_bytes = resp.content
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
        _set_job_error(job_id, str(exc))
        raise self.retry(exc=exc, countdown=30)


def _process_dataframe(job_id: str, df, fname: str):
    """
    Núcleo de processamento — compartilhado entre process_file e process_import_from_storage.
    Detecta automaticamente se é planilha DDM (tem VAL_ATUALIZADO_AVISTA) ou formato genérico.
    """
    import pandas as pd

    df.columns = [c.strip().lower() for c in df.columns]
    cols = list(df.columns)

    cpf_col  = next((c for c in cols if "cpf" in c or "cpfcgc" in c), None)
    nome_col = next((c for c in cols if "nome" in c or "name" in c), None)
    val_col  = next((c for c in cols if "val_atualizado" in c or "val_nominal" in c), None)
    is_ddm   = val_col is not None

    if not cpf_col or not nome_col:
        _set_job_error(job_id, f"Colunas obrigatórias não encontradas. Colunas detectadas: {cols}")
        return

    total     = 0
    with_debt = 0
    rows      = []

    for _, r in df.iterrows():
        # Converte row para dict simples garantindo que todos os valores sao strings
        r = {k: (str(v) if not isinstance(v, (list, dict)) else "") for k, v in r.items()}
        cpf_raw  = str(r.get(cpf_col, "") or "").strip()
        cpf_norm = _norm_cpf(cpf_raw)
        if not cpf_norm:
            continue

        nome = str(r.get(nome_col, "") or "").strip()
        total += 1

        if is_ddm:
            # ── Planilha DDM: dados de débito já estão na planilha ──────────────
            val_str = str(r.get(val_col, "0") or "0").strip().replace(",", ".")
            try:
                tem_debito = float(val_str) > 0
            except Exception:
                tem_debito = False

            phones = _get_phones_ddm(r)
            phone  = phones[0] if phones else ""

            if tem_debito and phone:
                with_debt += 1
                debito = {
                    "instituicao":    str(r.get("carteira", "") or "").strip(),
                    "nome_devedor":   nome,
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
                    # Metadados adicionais para referência
                    "idcrm":   str(r.get("idcrm", "") or "").strip(),
                    "uf":      str(r.get("uf", "") or "").strip(),
                    "cidade":  str(r.get("cidade", "") or "").strip(),
                    "all_phones": phones,
                    "email":      str(r.get("email", "") or "").strip(),
                }
                rows.append({
                    "cpf":      cpf_norm,
                    "name":     nome,
                    "phone":    phone,
                    "has_debt": True,
                    "debito":   debito,
                })

        else:
            # ── Planilha genérica: consulta API DDM por CPF ─────────────────────
            phone  = _get_phone(r, cols)
            debito = _processar_debito(cpf_norm)

            if debito:
                with_debt += 1
            if phone:
                rows.append({
                    "cpf":      cpf_norm,
                    "name":     nome,
                    "phone":    phone,
                    "has_debt": debito is not None,
                    "debito":   debito,
                })

        # Progresso a cada 1000 linhas
        if total % 1000 == 0:
            _update_job_progress(job_id, total, total, with_debt)

    # Finaliza job
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
