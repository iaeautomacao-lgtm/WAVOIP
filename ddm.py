import re
import requests
import os
from typing import Optional, Dict

DDM_TOKEN    = os.getenv("DDM_TOKEN", "2e30b68c0feda298f9d6d40ab36c1a09")
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
    try:
        r = requests.get(
            f"{DDM_CALCULA}?tk={DDM_TOKEN}&Doc={cpf}",
            timeout=30
        )
        r.raise_for_status()
        raw = r.json()
        return _safe_dict(raw)
    except Exception as e:
        return {"ERRO": str(e)}


def processar_debito(cpf: str) -> Optional[Dict]:
    dados = consultar_debitos_cpf(cpf)

    if dados.get("ERRO"):
        return None

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
