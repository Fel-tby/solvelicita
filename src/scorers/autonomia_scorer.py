"""
autonomia_scorer.py — Pontuação de Autonomia Fiscal (peso 10%)
Fonte: FINBRA/DCA (dca_indicadores_pb.csv)

Nota: scaixa foi incorporado ao Lliq via RGF Anexo 05 — não calculado aqui.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
from utils.paths import PROCESSED
from scorers.config import PESOS, LIMIAR_AUTONOMIA_CRIT

# Parâmetros sigmoid calibrados com dados 2020–2024 da PB.
# k = 2 / IQR empírico por grupo. Rever anualmente após nova coleta DCA.
SIGMOID_PARAMS = {
    "micro"   : (0.0296, 98.6),
    "pequeno" : (0.0276, 77.9),
    "médio"   : (0.0318, 96.2),
    "grande"  : (0.0228, 306.2),
}


def _porte(pop: int) -> str:
    if pop < 10_000:  return "micro"
    if pop < 50_000:  return "pequeno"
    if pop < 200_000: return "médio"
    return "grande"


def pontuar_autonomia(x, pop: int):
    """
    Receita tributária própria / Receita Corrente Total → [0.0, 1.0].
    Sigmoid regionalizada por porte — calibrada para o perfil real da PB.
    """
    if pd.isna(x) or pd.isna(pop):
        return None
    mu, k = SIGMOID_PARAMS[_porte(int(pop))]
    return round(float(1.0 / (1.0 + np.exp(-k * (x - mu)))), 4)


def carregar_dca(municipios: pd.DataFrame) -> pd.DataFrame:
    """
    Carrega dca_indicadores_pb.csv e calcula contribuição de Autonomia.

    Parâmetros
    ----------
    municipios : DataFrame mestre com colunas [cod_ibge, ente, populacao]

    Retorna
    -------
    DataFrame com colunas:
      cod_ibge, autonomia_media, autonomia_norm, contrib_autonomia
    """
    caminho = PROCESSED / "dca_indicadores_pb.csv"
    if not caminho.exists():
        raise FileNotFoundError(
            f"dca_indicadores_pb.csv não encontrado em {PROCESSED}. "
            "Execute src/collectors/dca.py primeiro."
        )

    dca = pd.read_csv(caminho, dtype={"cod_ibge": str})
    dca = dca.merge(municipios[["cod_ibge", "populacao"]], on="cod_ibge", how="left")

    dca["autonomia_norm"]    = dca.apply(
        lambda r: pontuar_autonomia(r["autonomia_media"], r["populacao"]), axis=1
    )
    dca["contrib_autonomia"] = (PESOS["autonomia"] * dca["autonomia_norm"]).round(4)

    return dca[["cod_ibge", "autonomia_media", "autonomia_norm", "contrib_autonomia"]]
