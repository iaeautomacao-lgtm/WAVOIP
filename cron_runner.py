import os
import sys
import time
import logging
import argparse
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [CRON] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("cron_runner")

from mysql_adapter import create_client
from redis_rest import UpstashRedisREST

def acquire_cron_lock(job_name: str, ttl_seconds: int = 50) -> bool:
    """Evita que duas instâncias da mesma tarefa no Cron se sobreponham."""
    try:
        r = UpstashRedisREST()
        lock_key = f"cron_lock:{job_name}"
        res = r.set(lock_key, "locked", ex=ttl_seconds, nx=True)
        if res:
            return True
        logger.info(f"Lock ativado para {job_name}. Outra instância do cron está rodando. Ignorando.")
        return False
    except Exception:
        lock_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"cron_{job_name}.lock")
        if os.path.exists(lock_file):
            try:
                mtime = os.path.getmtime(lock_file)
                if time.time() - mtime < ttl_seconds:
                    logger.info(f"Lock de arquivo ativado para {job_name}. Ignorando.")
                    return False
            except Exception:
                pass
        try:
            with open(lock_file, "w") as f:
                f.write(str(time.time()))
        except Exception:
            pass
        return True

def release_cron_lock(job_name: str):
    try:
        r = UpstashRedisREST()
        r.delete(f"cron_lock:{job_name}")
    except Exception:
        pass
    lock_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"cron_{job_name}.lock")
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
        except Exception:
            pass

def run_campaign_jobs():
    if not acquire_cron_lock("campaign"):
        return
    try:
        from tasks import fill_campaign_capacity, campaign_watchdog, _check_campaign_completion
        logger.info("Iniciando verificação de campanhas ativas e watchdog...")
        supabase = create_client()
        res = supabase.table("campaigns").select("id, name, status").in_("status", ["ativa", "em_andamento"]).execute()
        campaigns = res.data or []
        logger.info(f"Encontradas {len(campaigns)} campanhas ativas/em_andamento.")
        for camp in campaigns:
            camp_id = camp["id"]
            camp_name = camp.get("name", camp_id)
            logger.info(f"Preenchendo capacidade para campanha '{camp_name}' ({camp_id})...")
            try:
                res_fill = fill_campaign_capacity(camp_id)
                logger.info(f"Resultado fill_campaign_capacity ({camp_id}): {res_fill}")
            except Exception as e:
                logger.error(f"Erro em fill_campaign_capacity para {camp_id}: {e}", exc_info=True)

            try:
                _check_campaign_completion(camp_id)
            except Exception:
                pass

        logger.info("Executando campaign_watchdog...")
        try:
            campaign_watchdog()
        except Exception as e:
            logger.error(f"Erro em campaign_watchdog: {e}", exc_info=True)
    finally:

        release_cron_lock("campaign")

def run_import_jobs():
    if not acquire_cron_lock("import"):
        return
    try:
        from tasks import process_import_from_storage, _is_import_stopped
        logger.info("Verificando jobs de importação pendentes...")
        supabase = create_client()
        res = supabase.table("import_jobs").select("*").in_("status", ["pending", "processing"]).execute()
        jobs = res.data or []
        for job in jobs:
            job_id = job["id"]
            if _is_import_stopped(job_id):
                logger.info(f"Import_job {job_id} está marcado como parado.")
                continue
            storage_path = job.get("storage_path") or job.get("file_path")
            fname = job.get("filename", "import.xlsx")
            if storage_path and os.path.exists(storage_path):
                logger.info(f"Iniciando process_import_from_storage para job {job_id} ({fname})...")
                try:
                    process_import_from_storage(job_id, storage_path, fname)
                except Exception as e:
                    logger.error(f"Erro ao processar import_job {job_id}: {e}", exc_info=True)
    finally:
        release_cron_lock("import")

def main():
    parser = argparse.ArgumentParser(description="Runner de Cron Jobs para o Wavoip (cPanel)")
    parser.add_argument("--job", choices=["campaign", "import", "all"], default="all", help="Qual job rodar")
    args = parser.parse_args()

    if args.job in ("campaign", "all"):
        run_campaign_jobs()
    if args.job in ("import", "all"):
        run_import_jobs()

    # Aguarda todas as threads disparadas em background (como chamadas com delay) finalizarem
    try:
        from tasks import wait_for_dispatched_tasks
        logger.info("Aguardando tarefas em background finalizarem...")
        wait_for_dispatched_tasks(timeout=120.0)
        logger.info("Todas as tarefas concluídas.")
    except Exception as e:
        logger.error(f"Erro ao aguardar tarefas em background: {e}")

if __name__ == "__main__":
    main()
