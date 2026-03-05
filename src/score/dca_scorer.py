"""
dca_scorer.py — Pontuação de Autonomia Fiscal para uso pelo solvency.py

Responsabilidades:
  1. Carregar dca_indicadores_pb.csv
  2. Calcular pontuação da Autonomia via sigmoid regionalizada por porte
  3. Retornar DataFrame pronto para merge no solvency.py

Peso no score total: 10 pts (via contrib_autonomia)

Nota: scaixa (Ativo-Passivo Financeiro / Receita) foi incorporado
à variável Lliq via RGF Anexo 05 no solvency.py — não é calculado aqui.
"""

import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR  = Path(__file__).resolve().parent.parent.parent
PROCESSED = BASE_DIR / "data" / "processed"

PESO_AUTONOMIA = 10

# Parâmetros sigmoid calibrados com dados 2020–2024 da PB.
# k = 2 / IQR empírico por grupo. Rever anualmente com nova coleta DCA.
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

def pontuar_autonomia(x, pop: int) -> float | None:
    """
    Receita tributária própria / Receita Corrente Total → [0.0, 1.0].
    Usa sigmoid regionalizada por porte para calibrar o ponto médio
    ao perfil real de cada grupo populacional da PB.
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
    municipios : DataFrame mestre com colunas cod_ibge, ente, populacao

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

    dca["autonomia_norm"] = dca.apply(
        lambda r: pontuar_autonomia(r["autonomia_media"], r["populacao"]),
        axis=1,
    )
    dca["contrib_autonomia"] = (PESO_AUTONOMIA * dca["autonomia_norm"]).round(4)

    return dca[["cod_ibge", "autonomia_media", "autonomia_norm", "contrib_autonomia"]]
