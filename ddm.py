import re
import requests
import os
from typing import Optional, Dict, Any

DDM_TOKEN    = os.getenv("DDM_TOKEN", "").strip()
DDM_TIMEOUT_SECONDS = float(os.getenv("DDM_TIMEOUT_SECONDS", "15"))
DDM_BASE_URL = "https://www.ddmacordos.com"
DDM_CALCULA  = f"{DDM_BASE_URL}/ws_ddm/ws/CalculaDebitos.php"


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
        return {"ERRO": "DDM_TOKEN nao configurado"}

    try:
        r = requests.get(
            f"{DDM_CALCULA}?tk={DDM_TOKEN}&Doc={cpf}",
            timeout=DDM_TIMEOUT_SECONDS
        )
        r.raise_for_status()
        raw = r.json()
        return _safe_dict(raw)
    except Exception as e:
        return {"ERRO": str(e)}


def _montar_debito(dados: Dict[str, Any]) -> Optional[Dict]:
    idcalc = dados.get("idcalc")
    if not idcalc:
        return None

    # Parcelas — pega só as que têm ValorParcela numérico
    parcelas_raw = _safe_list(_safe_dict(dados.get("ListaParcelas", {})).get("Parcelas", []))
    parcelas = [p for p in parcelas_raw if isinstance(p, dict) and p.get("ValorParcela") and p.get("ValorParcela") != "0,00"]

    if not parcelas:
        return None

    # Primeira parcela = à vista
    avista = parcelas[0]
    valor_avista = avista.get("ValorParcela", "0,00")

    if valor_avista == "0,00":
        return None

    # Débitos individuais
    lista_debitos = _safe_list(_safe_dict(dados.get("ListaDebitos", {})).get("Debito", []))

    # Nome do cliente sem "NOVO"
    nome_cliente = re.sub(r'\bNOVO\b', '', dados.get("Cliente", ""), flags=re.IGNORECASE).strip()

    # Monta estrutura compatível com o restante do sistema
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
    dados = consultar_debitos_cpf(cpf)

    if dados.get("ERRO"):
        return {"ok": False, "error": str(dados.get("ERRO") or "erro DDM"), "debito": None}

    return {"ok": True, "error": "", "debito": _montar_debito(dados)}


def processar_debito(cpf: str) -> Optional[Dict]:
    result = processar_debito_result(cpf)
    return result.get("debito") if result.get("ok") else None
