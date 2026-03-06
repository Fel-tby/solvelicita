import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from scorers.config import PESOS, ANOS_REF, N_ANOS


def calcular(df_si: pd.DataFrame) -> pd.DataFrame:
    """
    Conta anos com RREO entregue (2020–2025) e normaliza para [0, 1].
    Cap duro de classificação é aplicado no engine/classifier.py.

    Entrada : df_si com colunas [cod_ibge, ano, entregou_rreo]
    Saída   : DataFrame [cod_ibge, anos_entregues, qsiconfi, contrib_qsiconfi]
    """
    df_qsi = (
        df_si[df_si["ano"].isin(ANOS_REF)]
        .groupby("cod_ibge")["entregou_rreo"]
        .agg(anos_entregues="sum")
        .reset_index()
    )
    df_qsi["qsiconfi"]         = df_qsi["anos_entregues"] / N_ANOS
    df_qsi["contrib_qsiconfi"] = (PESOS["qsiconfi"] * df_qsi["qsiconfi"]).round(4)
    return df_qsi[["cod_ibge", "anos_entregues", "qsiconfi", "contrib_qsiconfi"]]
