import re
import requests
import os
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

DDM_TOKEN    = os.getenv("DDM_TOKEN", "").strip()
DDM_TIMEOUT_SECONDS = float(os.getenv("DDM_TIMEOUT_SECONDS", "30"))
DDM_BASE_URL = "https://www.ddmacordos.com"
DDM_CALCULA  = f"{DDM_BASE_URL}/ws_ddm/ws/CalculaDebitos.php"


class DDMSoftError(Exception):
    """Erro transitório: timeout, 429, 5xx."""
    pass


class DDMHardError(Exception):
    """Erro definitivo: 401/403, resposta inválida."""
    pass


def _safe_dict(obj) -> dict:
    if isinstance(obj, list):
        return obj[0] if obj and isinstance(obj[0], dict) else {}
    return obj if isinstance(obj, dict) else {}


def _safe_list(obj) -> list:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        return [obj]
    return []


def consultar_debitos_cpf(cpf: str) -> Dict:
    if not DDM_TOKEN:
        raise DDMHardError("DDM_TOKEN nao configurado")

    try:
        url = f"{DDM_CALCULA}?tk={DDM_TOKEN}&Doc={cpf}"
        logger.warning("[DDM_DEBUG] CPF=%s URL=%s", cpf, url)
        r = requests.get(url, timeout=DDM_TIMEOUT_SECONDS)
        logger.warning("[DDM_DEBUG] CPF=%s STATUS=%s BODY=%s", cpf, r.status_code, r.text[:500])
        if r.status_code == 429 or r.status_code >= 500:
            raise DDMSoftError(f"DDM indisponivel ({r.status_code})")
        if r.status_code in (401, 403):
            raise DDMHardError(f"Token invalido ({r.status_code})")
        r.raise_for_status()
    except requests.exceptions.Timeout:
        logger.warning("[DDM_DEBUG] CPF=%s TIMEOUT", cpf)
        raise DDMSoftError("timeout")
    except requests.exceptions.ConnectionError:
        logger.warning("[DDM_DEBUG] CPF=%s CONEXAO_CAIU", cpf)
        raise DDMSoftError("conexao caiu")
    except requests.exceptions.HTTPError as e:
        code = getattr(getattr(e, "response", None), "status_code", 0)
        if code in (429, 500, 502, 503, 504):
            raise DDMSoftError(f"DDM indisponivel ({code})")
        raise DDMHardError(str(e))
    except DDMSoftError:
        raise
    except DDMHardError:
        raise
    except Exception as e:
        raise DDMSoftError(str(e))

    try:
        raw = r.json()
    except Exception as e:
        raise DDMHardError(f"Resposta invalida: {e}")

    return _safe_dict(raw)


def _montar_debito(dados: Dict[str, Any]) -> Optional[Dict]:
    idcalc = dados.get("idcalc")
    if not idcalc:
        return None

    parcelas_raw = _safe_list(_safe_dict(dados.get("ListaParcelas", {})).get("Parcelas", []))
    parcelas = [p for p in parcelas_raw if isinstance(p, dict) and p.get("ValorParcela") and p.get("ValorParcela") != "0,00"]

    if not parcelas:
        return None

    avista = parcelas[0]
    valor_avista = avista.get("ValorParcela", "0,00")

    if valor_avista == "0,00":
        return None

    lista_debitos = _safe_list(_safe_dict(dados.get("ListaDebitos", {})).get("Debito", []))
    nome_cliente = re.sub(r'\bNOVO\b', '', dados.get("Cliente", ""), flags=re.IGNORECASE).strip()

    return {
        "instituicao":    nome_cliente,
        "nome_devedor":   dados.get("NomeDev", ""),
        "numero_debitos": str(len(lista_debitos)),
        "idcalc":         idcalc,
        "PgtoAvista": {
            "ValorTotal":    dados.get("TotalNominal", "0,00"),
            "PercDesconto":  "0",
            "ValorDesconto": "0,00",
            "ValorFinal":    valor_avista,
        },
        "CalculoBoleto": {
            "SubtotalBoleto":    valor_avista,
            "HonorarioBoleto":   "0,00",
            "ValorCobrarBoleto": valor_avista,
        },
        "ParcelasBoleto": str(len(parcelas)),
        "PgtoParceladoCartao": {
            "Parcelas":     str(len(parcelas)),
            "ValorParcela": valor_avista,
            "ValorFinal":   valor_avista,
        },
    }


def processar_debito_result(cpf: str) -> Dict[str, Any]:
    try:
        dados = consultar_debitos_cpf(cpf)
        debito = _montar_debito(dados)
        if debito is None:
            logger.warning("[DDM_DEBUG] CPF=%s SEM_DEBITO_JSON_VALIDO", cpf)
            return {"ok": True, "error": "", "debito": None, "_no_debt": True}
        return {"ok": True, "error": "", "debito": debito}
    except DDMSoftError as e:
        logger.warning("[DDM_DEBUG] CPF=%s SOFT_ERROR=%s", cpf, e)
        return {"ok": False, "error": str(e), "debito": None, "_soft": True}
    except Exception as e:
        logger.warning("[DDM_DEBUG] CPF=%s ERRO=%s", cpf, e)
        return {"ok": False, "error": str(e), "debito": None, "_soft": False}


def processar_debito(cpf: str) -> Optional[Dict]:
    result = processar_debito_result(cpf)
    return result.get("debito") if result.get("ok") else None
