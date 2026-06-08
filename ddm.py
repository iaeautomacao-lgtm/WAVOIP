import re
import requests
import os
from typing import Optional, Dict

DDM_TOKEN    = os.getenv("DDM_TOKEN", "2e30b68c0feda298f9d6d40ab36c1a09")
DDM_BASE_URL = "https://www.ddmacordos.com"
DDM_CALCULA  = f"{DDM_BASE_URL}/ws_ddm/ws/CalculaDebitos.php"
DDM_CALC_ID  = f"{DDM_BASE_URL}/calc/"


def consultar_debitos_cpf(cpf: str) -> Dict:
    try:
        r = requests.get(f"{DDM_CALCULA}?tk={DDM_TOKEN}&Doc={cpf}", timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"ERRO": str(e)}


def consultar_detalhes_iddev(idcalc: str) -> Dict:
    try:
        r = requests.get(f"{DDM_CALC_ID}?tk={DDM_TOKEN}&idDev={idcalc}", timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"ERRO": str(e)}


def processar_debito(cpf: str) -> Optional[Dict]:
    dados_cpf = consultar_debitos_cpf(cpf)
    if dados_cpf.get("ERRO"):
        return None

    idcalc = dados_cpf.get("idcalc")
    if not idcalc:
        return None

    detalhes = consultar_detalhes_iddev(idcalc)
    if detalhes.get("ERRO"):
        return None

    lista_parcelas = detalhes.get("ListaParcelas", {}).get("Parcelas", [])
    if isinstance(lista_parcelas, dict):
        lista_parcelas = [lista_parcelas]
    if not lista_parcelas:
        return None
    if lista_parcelas[0].get("ValorParcela", "0,00") == "0,00":
        return None

    def _last(obj):
        return obj[-1] if isinstance(obj, list) and obj else (obj if isinstance(obj, dict) else {})

    dados       = detalhes.get("Dados", {})
    pgto_avista = _last(detalhes.get("PgtoAvista", {}))
    pgto_boleto = _last(detalhes.get("PgtoParceladoBoleto", {}))
    pgto_cartao = _last(detalhes.get("PgtoParceladoCartao", {}))

    nome_cliente = re.sub(r'\bNOVO\b', '', dados.get("Cliente", ""), flags=re.IGNORECASE).strip()

    lista_debitos = dados_cpf.get("ListaDebitos", {}).get("Debito", [])
    if isinstance(lista_debitos, dict):
        lista_debitos = [lista_debitos]

    return {
        "instituicao":    nome_cliente,
        "nome_devedor":   dados.get("NomeDevedor", ""),
        "numero_debitos": str(len(lista_debitos)),
        "idcalc":         idcalc,
        "PgtoAvista": {
            "ValorTotal":    pgto_avista.get("ValorTotal", "0,00"),
            "PercDesconto":  pgto_avista.get("PercDesconto", "0"),
            "ValorDesconto": pgto_avista.get("ValorDesconto", "0,00"),
            "ValorFinal":    pgto_avista.get("ValorFinal", "0,00"),
        },
        "CalculoBoleto": {
            "SubtotalBoleto":    pgto_boleto.get("Valor", "0,00"),
            "HonorarioBoleto":   pgto_boleto.get("ValorDesconto", "0,00"),
            "ValorCobrarBoleto": pgto_boleto.get("ValorParcela", "0,00"),
        },
        "ParcelasBoleto": pgto_boleto.get("Parcelas", "0"),
        "PgtoParceladoCartao": {
            "Parcelas":     pgto_cartao.get("Parcelas", "0"),
            "ValorParcela": pgto_cartao.get("ValorParcela", "0,00"),
            "ValorFinal":   pgto_cartao.get("ValorFinal", "0,00"),
        },
    }