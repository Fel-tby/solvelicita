import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from scorers.config import PESOS, ANOS_REF, LIMIAR_RPROC_CRONICO


def pontuar_rproc_cronico(n: int) -> float:
    """
    Penaliza o padrão histórico de manter RP Processados > 3% da receita.
    Cap duro de classificação (≥5 → teto 🟡) aplicado no classifier.

    0 → 1.00 | 1 → 0.75 | 2 → 0.50 | 3 → 0.30 | 4 → 0.10 | ≥5 → 0.00
    """
    mapa = {0: 1.00, 1: 0.75, 2: 0.50, 3: 0.30, 4: 0.10, 5: 0.00, 6: 0.00}
    return mapa.get(int(n), 0.00)


def calcular(df_si: pd.DataFrame) -> pd.DataFrame:
    """
    Conta anos com rproc_pct > 3% para cada município.

    Entrada : df_si com [cod_ibge, ano, entregou_rreo, rproc_pct]
    Saída   : DataFrame [cod_ibge, n_anos_cronicos, rproc_norm, contrib_rproc]
    """
    df_rp = (
        df_si[
            df_si["ano"].isin(ANOS_REF) &
            df_si["entregou_rreo"] &
            df_si["rproc_pct"].notna()
        ]
        .groupby("cod_ibge")["rproc_pct"]
        .apply(lambda s: int((s > LIMIAR_RPROC_CRONICO).sum()))
        .reset_index()
        .rename(columns={"rproc_pct": "n_anos_cronicos"})
    )
    df_rp["rproc_norm"]    = df_rp["n_anos_cronicos"].apply(pontuar_rproc_cronico)
    df_rp["contrib_rproc"] = (PESOS["rproc"] * df_rp["rproc_norm"]).round(4)
    return df_rp[["cod_ibge", "n_anos_cronicos", "rproc_norm", "contrib_rproc"]]
