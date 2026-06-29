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


def _norm_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(key).lower())


def _get_any(obj, *names, default=None):
    if not isinstance(obj, dict):
        return default
    wanted = {_norm_key(name) for name in names}
    for key, value in obj.items():
        if _norm_key(key) in wanted:
            return value
    return default


def _find_first(obj, *names) -> str:
    wanted = {_norm_key(name) for name in names}
    if isinstance(obj, dict):
        for key, value in obj.items():
            if _norm_key(key) in wanted and value not in (None, ""):
                return str(value).strip()
            nested = _find_first(value, *names)
            if nested:
                return nested
    elif isinstance(obj, list):
        for item in obj:
            nested = _find_first(item, *names)
            if nested:
                return nested
    return ""


def consultar_debitos_cpf(cpf: str) -> Dict:
    if not DDM_TOKEN:
        raise DDMHardError("DDM_TOKEN nao configurado")

    try:
        cpf_tail = re.sub(r"\D", "", str(cpf))[-4:]
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s consultando DDM", cpf_tail)
        r = requests.get(
            DDM_CALCULA,
            params={"tk": DDM_TOKEN, "Doc": cpf},
            timeout=DDM_TIMEOUT_SECONDS,
        )
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s STATUS=%s BODY_LEN=%s", cpf_tail, r.status_code, len(r.text or ""))

        if r.status_code == 429 or r.status_code >= 500:
            raise DDMSoftError(f"DDM indisponivel ({r.status_code})")
        if r.status_code in (401, 403):
            raise DDMHardError(f"Token invalido ({r.status_code})")
        r.raise_for_status()
    except requests.exceptions.Timeout:
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s TIMEOUT", re.sub(r"\D", "", str(cpf))[-4:])
        raise DDMSoftError("timeout")
    except requests.exceptions.ConnectionError:
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s CONEXAO_CAIU", re.sub(r"\D", "", str(cpf))[-4:])
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

    erro_ddm = _get_any(raw, "ERRO", "erro") if isinstance(raw, dict) else ""
    if erro_ddm:
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s ERRO_GENERICO_RECEBIDO", re.sub(r"\D", "", str(cpf))[-4:])
        raise DDMHardError("ERRO_GENERICO_DDM")

    return _safe_dict(raw)


def _montar_debito(dados: Dict[str, Any]) -> Optional[Dict]:
    idcalc = _find_first(dados, "idcalc", "id_calc", "idCalculo", "calculoId")

    lista_parcelas = _safe_dict(_get_any(dados, "ListaParcelas", "lista_parcelas", "parcelas", default={}))
    parcelas_raw = _safe_list(_get_any(lista_parcelas, "Parcelas", "Parcela", "parcelas", default=[]))
    if not parcelas_raw:
        parcelas_raw = _safe_list(_get_any(dados, "Parcelas", "Parcela", "parcelas", default=[]))

    def parcela_valor(parcela: dict) -> str:
        return str(_get_any(
            parcela,
            "ValorParcela", "valor_parcela", "valor", "ValorFinal", "valorFinal",
            default=""
        ) or "").strip()

    parcelas = [
        p for p in parcelas_raw
        if isinstance(p, dict) and parcela_valor(p) and parcela_valor(p) != "0,00"
    ]

    if not parcelas:
        return None

    avista = parcelas[0]
    valor_avista = parcela_valor(avista) or "0,00"

    if valor_avista == "0,00":
        return None

    lista_debitos_obj = _safe_dict(_get_any(dados, "ListaDebitos", "lista_debitos", "debitos", default={}))
    lista_debitos = _safe_list(_get_any(lista_debitos_obj, "Debito", "Debitos", "debito", "debitos", default=[]))
    nome_cliente = re.sub(
        r'\bNOVO\b',
        '',
        _find_first(dados, "Cliente", "cliente", "Instituicao", "instituicao"),
        flags=re.IGNORECASE,
    ).strip()
    nome_devedor = _find_first(dados, "NomeDev", "nome_dev", "NomeDevedor", "nome_devedor")
    total_nominal = _find_first(dados, "TotalNominal", "total_nominal", "ValorTotal", "valor_total") or "0,00"

    max_parcelas = len(parcelas)
    max_parcela_val = parcela_valor(parcelas[-1]) if parcelas else "0,00"
    primeiro_venc = _find_first(dados, "PrimeiroVencto", "PrimeiroVencimento", "DtVenc", "Vencimento", "VencimentoAcordo")
    if not primeiro_venc and parcelas:
        primeiro_venc = _find_first(parcelas[0], "Vencimento", "DataVencimento", "Venc", "DtVenc")
    if not primeiro_venc:
        primeiro_venc = "em dois dias"

    return {
        "instituicao":    nome_cliente,
        "nome_devedor":   nome_devedor,
        "numero_debitos": str(len(lista_debitos)),
        "idcalc":         idcalc,
        "PrimeiroVencto": primeiro_venc,
        "PgtoAvista": {
            "ValorTotal":    total_nominal,
            "PercDesconto":  "0",
            "ValorDesconto": "0,00",
            "ValorFinal":    valor_avista,
        },
        "CalculoBoleto": {
            "SubtotalBoleto":    valor_avista,
            "HonorarioBoleto":   "0,00",
            "ValorCobrarBoleto": max_parcela_val,
        },
        "ParcelasBoleto": str(max_parcelas),
        "PgtoParceladoCartao": {
            "Parcelas":     str(max_parcelas),
            "ValorParcela": max_parcela_val,
            "ValorFinal":   valor_avista,
        },
    }


def processar_debito_result(cpf: str) -> Dict[str, Any]:
    try:
        dados = consultar_debitos_cpf(cpf)
        debito = _montar_debito(dados)
        if debito is None:
            logger.warning("[DDM_DEBUG] CPF_FINAL=%s SEM_DEBITO_JSON_VALIDO", re.sub(r"\D", "", str(cpf))[-4:])
            return {"ok": True, "error": "", "debito": None, "_no_debt": True}
        return {"ok": True, "error": "", "debito": debito}
    except DDMSoftError as e:
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s SOFT_ERROR=%s", re.sub(r"\D", "", str(cpf))[-4:], e)
        return {"ok": False, "error": str(e), "debito": None, "_soft": True}
    except Exception as e:
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s ERRO=%s", re.sub(r"\D", "", str(cpf))[-4:], e)
        return {"ok": False, "error": str(e), "debito": None, "_soft": False}


def processar_debito(cpf: str) -> Optional[Dict]:
    result = processar_debito_result(cpf)
    return result.get("debito") if result.get("ok") else None
