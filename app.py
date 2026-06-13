from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import requests
import os
import re
import time
import uuid
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from tasks import celery, make_call_task, process_import, process_file, process_import_from_storage, formalizar_acordo

app = Flask(__name__)
CORS(app)

def env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip().strip('"').strip("'") if isinstance(value, str) else value


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
    healthy    = []
    for t in DEVICE_PRIORITY:
        if t not in device_map:          continue
        d = device_map[t]
        if d.get("status") != "open":   continue
        if d.get("disabled") != 0:      continue
        if d.get("phone") is None:      continue
        if not WAVOIP_VAPI_MAP.get(t):  continue
        healthy.append(d)
    return healthy


def vapi_call(phone: str) -> dict:
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

        rows = [{
            "id":         r["id"],
            "cpf":        r["cpf"],
            "phone":      r["phone"],
            "status":     r["status"],
            "campaign":   camp_map.get(r["campaign_id"], "—"),
            "created_at": r["created_at"],
        } for r in result.data]

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


@app.route("/api/campaigns", methods=["POST"])
def create_campaign():
    try:
        body       = request.json
        name       = body.get("name", "").strip()
        session_id = body.get("session_id")
        contacts   = body.get("contacts", [])

        if not name:
            return jsonify({"ok": False, "error": "Nome obrigatório"}), 400

        if session_id:
            job = supabase.table("import_jobs")\
                .select("result, with_debt").eq("id", session_id).execute().data
            if not job:
                return jsonify({"ok": False, "error": "Sessão não encontrada"}), 404
            contacts = [r for r in job[0]["result"] if r.get("has_debt") and r.get("phone")]

        if not contacts:
            return jsonify({"ok": False, "error": "Nenhum contato com débito e telefone"}), 400

        camp = supabase.table("campaigns").insert({
            "name":   name,
            "status": "rascunho",
            "total":  len(contacts),
        }).execute().data[0]

        campaign_id = camp["id"]

        rows = [{
            "campaign_id": campaign_id,
            "cpf":         c.get("cpf", ""),
            "phone":       c.get("phone", "").strip(),
            "name":        c.get("name", ""),
            "status":      "pendente",
            "order_idx":   i,
            "debito_data": c.get("debito"),
        } for i, c in enumerate(contacts)]

        for i in range(0, len(rows), 500):
            supabase.table("campaign_calls").insert(rows[i:i+500]).execute()

        return jsonify({"ok": True, "campaign": camp})
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

        if camp["status"] == "em_andamento":
            return jsonify({"ok": False, "error": "Campanha já em andamento"}), 400

        supabase.table("campaigns").update({
            "status": "em_andamento", "updated_at": "now()"
        }).eq("id", campaign_id).execute()

        pending = supabase.table("campaign_calls")\
            .select("*")\
            .eq("campaign_id", campaign_id)\
            .in_("status", ["pendente", "enfileirado"])\
            .order("order_idx")\
            .execute().data

        if not pending:
            supabase.table("campaigns").update({"status": "finalizada"}).eq("id", campaign_id).execute()
            return jsonify({"ok": True, "fired": 0, "message": "Nenhum pendente"})

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
                "row_id":      row["id"],
                "cpf":         row["cpf"],
                "phone":       row["phone"],
                "name":        row.get("name", ""),
                "debito_data": row.get("debito_data"),
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
                supabase.table("campaigns").update({
                    "finished": finished, "updated_at": "now()"
                }).eq("id", campaign_id).execute()

                if finished >= (camp.get("total") or 0):
                    supabase.table("campaigns").update({
                        "status": "finalizada", "updated_at": "now()"
                    }).eq("id", campaign_id).execute()
                else:
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

        import redis as redis_lib

        file_bytes = file.read()
        file_id    = str(uuid.uuid4())

        r = redis_lib.from_url(os.getenv("REDIS_URL"))
        r.setex(f"import:{file_id}", 3600, file_bytes)

        job = supabase.table("import_jobs").insert({
            "status":    "processing",
            "total":     0,
            "with_debt": 0,
            "processed": 0,
        }).execute().data[0]

        process_file.delay(job["id"], file_id, fname)

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

        process_import_from_storage.delay(job["id"], storage_path, fname)

        return jsonify({"ok": True, "job_id": job["id"], "status": "queued"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/import/status/<job_id>", methods=["GET"])
def import_status(job_id):
    try:
        job = supabase.table("import_jobs").select("*").eq("id", job_id).execute().data
        if not job:
            return jsonify({"ok": False, "error": "Job não encontrado"}), 404
        job = job[0]

        result_preview = None
        if job["status"] == "done" and job["result"]:
            if isinstance(job["result"], list):
                result_preview = job["result"][:200]
            elif isinstance(job["result"], dict):
                result_preview = job["result"]

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
