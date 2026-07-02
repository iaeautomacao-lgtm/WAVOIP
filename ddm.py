import re
import requests
import os
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

DDM_TOKEN    = os.getenv("DDM_TOKEN", "").strip()
DDM_TOKEN_BUSCA = os.getenv("DDM_TOKEN_BUSCA", "2e30b68c0feda298f9d6d40ab36c1a09").strip()
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


def _consolidate_calc_response(data_list: list) -> dict:
    consolidated = {}
    parcelas_list = []
    debitos_list = []
    
    for item in data_list:
        if not isinstance(item, dict):
            continue
        
        # Dados Gerais
        if "Dados" in item:
            consolidated.update(item["Dados"])
            
        # Lista de Débitos
        if "Calculos" in item:
            calculos = item["Calculos"] or []
            for calc in calculos:
                if isinstance(calc, dict) and "debitos" in calc:
                    debitos_list.append(calc["debitos"])
                    
        # Pagamento à Vista
        if "PgtoAvista" in item:
            avista = item["PgtoAvista"] or {}
            consolidated["PgtoAvista"] = avista
            # Adiciona a vista na lista de parcelas
            parcelas_list.append({
                "ValorParcela": avista.get("ValorFinal") or avista.get("ValorTotal") or "0,00",
                "ValorFinal": avista.get("ValorFinal") or avista.get("ValorTotal") or "0,00",
            })
            
        # Opções Parceladas
        if "PgtoParceladoBoleto" in item:
            parcelas_list.append(item["PgtoParceladoBoleto"])
            
        if "PgtoParceladoCartao" in item:
            consolidated["PgtoParceladoCartao"] = item["PgtoParceladoCartao"]
            
    consolidated["ListaParcelas"] = {"Parcelas": parcelas_list}
    consolidated["ListaDebitos"] = {"Debito": debitos_list}
    
    # Define chaves top-level para o _montar_debito encontrar
    consolidated["TotalNominal"] = consolidated.get("nominal") or consolidated.get("nominal_princ") or consolidated.get("PgtoAvista", {}).get("ValorTotal") or "0,00"
    consolidated["Cliente"] = consolidated.get("instituicao") or consolidated.get("Cliente") or ""
    consolidated["NomeDev"] = consolidated.get("nome") or consolidated.get("NomeDevedor") or ""
    consolidated["PrimeiroVencto"] = consolidated.get("PrimeiroVencto") or ""
    consolidated["idcalc"] = consolidated.get("CalculoID") or consolidated.get("iddev") or ""
    
    return consolidated


def consultar_debitos_cpf(cpf: str) -> Dict:
    token_busca = DDM_TOKEN_BUSCA or DDM_TOKEN
    if not token_busca:
        raise DDMHardError("DDM_TOKEN_BUSCA nao configurado")

    cpf_norm = re.sub(r"\D", "", str(cpf))
    cpf_tail = cpf_norm[-4:]
    logger.warning("[DDM_DEBUG] CPF_FINAL=%s consultando localiza_dev.php", cpf_tail)

    try:
        # 1. Localiza iddev pelo CPF
        r1 = requests.get(
            "https://ddmacordos.com/calc/localiza_dev.php",
            params={"tk": token_busca, "cpf": cpf_norm},
            timeout=DDM_TIMEOUT_SECONDS,
        )
        r1.raise_for_status()
        data1 = r1.json()
        if not isinstance(data1, list) or len(data1) == 0:
            logger.warning("[DDM_DEBUG] CPF_FINAL=%s devedor nao localizado", cpf_tail)
            return {}

        dev = data1[0]
        iddev = dev.get("iddev")
        if not iddev:
            logger.warning("[DDM_DEBUG] CPF_FINAL=%s sem iddev", cpf_tail)
            return {}

        # Mapeia dinamicamente o cli com base no sistema retornado pela DDM
        sistema = dev.get("sistema", "").strip().lower()
        cli = "ddm"
        if sistema == "cruzeirodosul":
            cli = "cruzeiro"

        # 2. Consulta débitos no calc/ com o cli dinâmico correspondente
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s consultando calc/ iddev=%s cli=%s", cpf_tail, iddev, cli)
        r2 = requests.get(
            "https://ddmacordos.com/calc/",
            params={"tk": token_busca, "idDev": iddev, "cli": cli},
            timeout=DDM_TIMEOUT_SECONDS,
        )
        r2.raise_for_status()
        raw = r2.json()
    except requests.exceptions.Timeout:
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s TIMEOUT", cpf_tail)
        raise DDMSoftError("timeout")
    except requests.exceptions.ConnectionError:
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s CONEXAO_CAIU", cpf_tail)
        raise DDMSoftError("conexao caiu")
    except Exception as e:
        logger.warning("[DDM_DEBUG] CPF_FINAL=%s ERRO=%s", cpf_tail, e)
        raise DDMSoftError(str(e))

    # Consolida os itens da lista em um único dicionário para compatibilidade com _montar_debito
    data_list = raw if isinstance(raw, list) else [raw]
    consolidated = _consolidate_calc_response(data_list)
    return consolidated


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
