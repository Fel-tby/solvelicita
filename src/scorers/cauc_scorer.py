import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from scorers.config import PESOS, PENDENCIAS_GRAVES, PENDENCIAS_MODERADAS


def pontuar_ccauc(s) -> float:
    """
    Converte pendências CAUC em score de risco [0, 1].
    Retorna ccauc — quanto MAIOR, PIOR (contrib = peso * (1 - ccauc)).

    não-string (NaN, None) → 1.0  (ausência de dado = pior caso)
    "REGULAR"              → 0.0  (contribuição máxima = 20 pts)
    pendência grave        → 1.0  (contribuição = 0 pts)
    pendências mod/leve    → proporcional, teto 0.5
    """
    if not isinstance(s, str):
        return 1.0
    if s.strip() == "REGULAR":
        return 0.0
    itens = [p.strip() for p in s.split("|")]
    if any(i in PENDENCIAS_GRAVES for i in itens):
        return 1.0
    n_mod  = sum(1 for i in itens if i in PENDENCIAS_MODERADAS)
    n_leve = sum(1 for i in itens if i not in PENDENCIAS_MODERADAS)
    return round(min((n_mod * 2 + n_leve) / 20, 0.5), 4)


def calcular(df_ca: pd.DataFrame) -> pd.DataFrame:
    """
    Entrada : df_ca com colunas [cod_ibge, pendencias]
    Saída   : DataFrame [cod_ibge, ccauc, contrib_ccauc]
    """
    df = df_ca[["cod_ibge", "pendencias"]].copy()
    df["ccauc"]        = df["pendencias"].apply(pontuar_ccauc)
    df["contrib_ccauc"] = (PESOS["ccauc"] * (1 - df["ccauc"])).round(4)
    return df[["cod_ibge", "ccauc", "contrib_ccauc"]]
