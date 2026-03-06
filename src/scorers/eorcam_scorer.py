import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from scorers.config import PESOS, ANOS_REF, PESOS_ANO


def pontuar_eorcam(x: float):
    """
    Receita realizada / prevista (%). Zona ótima: 90–105%.
    > 120%      → 0.5  (arrecadação anômala, não sustentável)
    105 – 120   → decaimento linear 1.0→0.5
    90 – 105    → 1.0  (máximo)
    70 – 90     → proporcional 0.0→1.0
    < 70        → 0.0  (colapso de arrecadação)
    """
    if pd.isna(x):
        return None
    if 90 <= x <= 105: return 1.0
    if x > 120:        return 0.5
    if x > 105:        return round(1.0 - (x - 105) / 30, 4)
    if x >= 70:        return round((x - 70) / 20, 4)
    return 0.0


def calcular(df_si: pd.DataFrame) -> pd.DataFrame:
    """
    Média ponderada por recência (PESOS_ANO). 2020 tem peso 0 —
    serve como reserva histórica mas não entra na média ponderada.

    Entrada : df_si com colunas [cod_ibge, ano, entregou_rreo, eorcam]
    Saída   : DataFrame [cod_ibge, eorcam_raw, eorcam_norm, contrib_eorcam]
    """
    df_eo = df_si[
        df_si["ano"].isin(ANOS_REF) &
        df_si["entregou_rreo"] &
        df_si["eorcam"].notna()
    ].copy()

    df_eo["peso_ano"] = df_eo["ano"].map(PESOS_ANO).fillna(0)
    df_eo = df_eo[df_eo["peso_ano"] > 0]
    df_eo["eorcam_w"] = df_eo["eorcam"] * df_eo["peso_ano"]

    df_result = (
        df_eo.groupby("cod_ibge")
        .apply(
            lambda g: round(g["eorcam_w"].sum() / g["peso_ano"].sum(), 4),
            include_groups=False,
        )
        .reset_index()
        .rename(columns={0: "eorcam_raw"})
    )

    df_result["eorcam_norm"]    = df_result["eorcam_raw"].apply(pontuar_eorcam)
    df_result["contrib_eorcam"] = (PESOS["eorcam"] * df_result["eorcam_norm"].fillna(0)).round(4)
    return df_result[["cod_ibge", "eorcam_raw", "eorcam_norm", "contrib_eorcam"]]
