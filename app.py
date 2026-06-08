from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import requests
import os
import time
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from tasks import celery, make_call_task, process_import, process_import_ddm
from ddm import processar_debito
load_dotenv()
from tasks import celery, make_call_task, process_import, process_file

app = Flask(__name__)
CORS(app)

WAVOIP_EMAIL      = os.getenv("WAVOIP_EMAIL")
WAVOIP_PASSWORD   = os.getenv("WAVOIP_PASSWORD")
WAVOIP_BASE       = "https://api.wavoip.com"
VAPI_API_KEY      = os.getenv("VAPI_API_KEY")
VAPI_ASSISTANT_ID = os.getenv("VAPI_ASSISTANT_ID")
VAPI_BASE         = "https://api.vapi.ai"
SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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
        body     = request.json
        call_id  = body.get("message", {}).get("call", {}).get("id") or body.get("call", {}).get("id") or body.get("id")
        msg_type = body.get("message", {}).get("type") or body.get("type")

        if msg_type == "status-update":
            status_call = body.get("message", {}).get("status")
            call_id_log = body.get("message", {}).get("call", {}).get("id")
            if status_call == "in-progress" and call_id_log:
                try:
                    supabase.table("campaign_calls").update({"status": "atendido"})\
                        .eq("vapi_call_id", call_id_log).execute()
                except Exception:
                    pass
            return jsonify({"ok": True}), 200

        if msg_type not in ("end-of-call-report", "call-ended"):
            return jsonify({"ok": True}), 200

        if not call_id:
            return jsonify({"ok": True}), 200

        res = supabase.table("campaign_calls")\
            .select("*").eq("vapi_call_id", call_id).execute()
        if not res.data:
            return jsonify({"ok": True}), 200

        row          = res.data[0]
        campaign_id  = row["campaign_id"]
        ended_reason = body.get("message", {}).get("endedReason") or body.get("endedReason", "")
        duration     = body.get("message", {}).get("durationSeconds") or body.get("durationSeconds", 0)

        supabase.table("campaign_calls").update({
            "status":   "finalizado",
            "error":    ended_reason,
            "duration": duration,
        }).eq("id", row["id"]).execute()

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
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/import/upload", methods=["POST"])
def import_upload():
    try:
        if "file" not in request.files:
            return jsonify({"ok": False, "error": "Nenhum arquivo enviado"}), 400

        file  = request.files["file"]
        fname = file.filename.lower()

        if not (fname.endswith(".csv") or fname.endswith(".xlsx") or fname.endswith(".xls")):
            return jsonify({"ok": False, "error": "Formato inválido. Use .csv ou .xlsx"}), 400

        # Salva em /tmp e enfileira no Celery
        import uuid
        file_id   = str(uuid.uuid4())
        file_path = f"/tmp/{file_id}_{file.filename}"
        file.save(file_path)

        job = supabase.table("import_jobs").insert({
            "status":    "processing",
            "total":     0,
            "with_debt": 0,
            "processed": 0,
        }).execute().data[0]

        process_file.delay(job["id"], file_path)

        return jsonify({
            "ok":     True,
            "mode":   "async",
            "job_id": job["id"],
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/import/status/<job_id>", methods=["GET"])
def import_status(job_id):
    try:
        job = supabase.table("import_jobs").select("*").eq("id", job_id).execute().data
        if not job:
            return jsonify({"ok": False, "error": "Job não encontrado"}), 404
        job = job[0]
        return jsonify({
            "ok":        True,
            "status":    job["status"],
            "total":     job["total"],
            "processed": job["processed"],
            "with_debt": job["with_debt"],
            "session_id": job["id"] if job["status"] == "done" else None,
            "result":    job["result"][:200] if job["status"] == "done" and job["result"] else None,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)