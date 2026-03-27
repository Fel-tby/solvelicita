import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from datetime import date
from scorers.config import (
    PESOS, ANOS_REF,
    LIMIAR_LLIQ_SUSPEITO,
    JANELA_RGF_BIMESTRAL, JANELA_RGF_SEMESTRAL,
    FIM_PERIODO_MES,
)

# Calculado no momento da execução, não como constante de módulo
HOJE = date.today()


def pontuar_lliq(x: float):
    """
    DCL pós-RP excl. RPPS / Receita Realizada → [0, 1].
    Valores < −0.50 são capados antes do cálculo (sinalizados separadamente).

    Curva v7.0 — âncoras exatas:
      ≥  0.35       → 1.00
       0.10 – 0.35  → linear 0.60 → 1.00   [ (x−0.10)/0.25 × 0.40 + 0.60 ]
       0.00 – 0.10  → linear 0.35 → 0.60   [ (x/0.10)      × 0.25 + 0.35 ]
      −0.50 – 0.00  → linear 0.00 → 0.35   [ (x+0.50)/0.50 × 0.35        ]

    Motivação da mudança em relação à v6.2:
      - Teto deslocado de 0.20 para 0.35: apenas 8% dos municípios atingem
        norm=1.0 (era 24%), eliminando compressão da escala no intervalo mais
        informativo (0.05 – 0.30, que concentra 74% da distribuição real PB).
      - Piso em lliq=0 reduzido de 0.50 para 0.35: score neutro (caixa zero)
        deixa de receber metade dos pontos máximos.
      - Negativo linear em vez de quadrático: distribui penalidade de forma
        uniforme; a curva quadrática anterior suavizava excessivamente a faixa
        −0.10 a 0.00 (Δ norm v6.2→v7.0 nessa faixa: −0.09).
    """
    if pd.isna(x):
        return None
    x = max(x, LIMIAR_LLIQ_SUSPEITO)
    if x >= 0.35:
        return 1.00
    if x >= 0.10:
        return round(0.60 + (x - 0.10) / 0.25 * 0.40, 4)
    if x >= 0.00:
        return round(0.35 + (x / 0.10) * 0.25, 4)
    return round(max(0.0, (x + 0.50) / 0.50 * 0.35), 4)


def _dias_atraso(ano, periodo, periodicidade) -> int:
    """
    Dias desde a data esperada de publicação do RGF mais recente.
    Assume 2 meses de prazo após o fim do período.
    Retorna 999 quando os metadados de periodicidade estão ausentes.
    """
    if pd.isna(periodo) or pd.isna(periodicidade):
        return 999
    key = (str(periodicidade), int(periodo))
    if key not in FIM_PERIODO_MES:
        return 999
    mes_pub = FIM_PERIODO_MES[key] + 2
    ano_pub = int(ano) + (1 if mes_pub > 12 else 0)
    mes_pub = mes_pub - 12 if mes_pub > 12 else mes_pub
    try:
        return max(0, (HOJE - date(ano_pub, mes_pub, 1)).days)
    except Exception:
        return 999


def _decay(dias: int, populacao: int) -> float:
    """
    Penalidade proporcional quando RGF está fora da janela aceitável.
    decay = max(0, 1 − (dias_atraso − janela) / 365)
    """
    janela = JANELA_RGF_BIMESTRAL if int(populacao) > 50_000 else JANELA_RGF_SEMESTRAL
    if dias <= janela:
        return 1.00
    return round(max(0.0, 1.0 - (dias - janela) / 365.0), 4)


def calcular(df_si: pd.DataFrame, df_mu: pd.DataFrame) -> pd.DataFrame:
    """
    Seleciona o RGF Anexo 05 mais recente por município.
    Prioridade Q > S quando ambas periodicidades existem no mesmo exercício.

    Entrada : df_si com [cod_ibge, ano, lliq, periodo_rgf,
                         periodicidade_rgf, lliq_parcial]
              df_mu com [cod_ibge, populacao]
    Saída   : DataFrame [cod_ibge, lliq_raw, lliq_norm, lliq_ano,
                         lliq_periodo, lliq_periodicidade, lliq_parcial,
                         dias_atraso, decay_fator, dado_defasado,
                         dado_suspeito_lliq, contrib_lliq]
    """
    df_base = (
        df_si[df_si["ano"].isin(ANOS_REF) & df_si["lliq"].notna()]
        .assign(
            _per_sort  = lambda x: x["periodo_rgf"].fillna(-1),
            _prior_per = lambda x: (x["periodicidade_rgf"] == "Q").astype(int),
        )
        .sort_values(
            ["cod_ibge", "ano", "_per_sort", "_prior_per"],
            ascending=[True, False, False, False],
        )
        .groupby("cod_ibge")
        .first()
        .reset_index()
        [["cod_ibge", "lliq", "ano", "periodo_rgf", "periodicidade_rgf", "lliq_parcial"]]
        .rename(columns={
            "lliq"            : "lliq_raw",
            "ano"             : "lliq_ano",
            "periodo_rgf"     : "lliq_periodo",
            "periodicidade_rgf": "lliq_periodicidade",
        })
    )

    df_base = df_base.merge(df_mu[["cod_ibge", "populacao"]], on="cod_ibge", how="left")

    df_base["lliq_norm"]        = df_base["lliq_raw"].apply(pontuar_lliq)
    df_base["dado_suspeito_lliq"] = (
        df_base["lliq_raw"].notna() & (df_base["lliq_raw"] < LIMIAR_LLIQ_SUSPEITO)
    )
    df_base["dias_atraso"] = df_base.apply(
        lambda r: _dias_atraso(r["lliq_ano"], r["lliq_periodo"], r["lliq_periodicidade"])
        if pd.notnull(r.get("lliq_ano")) else 999,
        axis=1,
    )
    df_base["decay_fator"] = df_base.apply(
        lambda r: _decay(r["dias_atraso"], r["populacao"]), axis=1
    )
    df_base["dado_defasado"] = df_base.apply(
        lambda r: r["dias_atraso"] > (
            JANELA_RGF_BIMESTRAL if r["populacao"] > 50_000 else JANELA_RGF_SEMESTRAL
        ),
        axis=1,
    )
    df_base["contrib_lliq"] = (
        (PESOS["lliq"] * df_base["lliq_norm"].fillna(0)) * df_base["decay_fator"]
    ).round(4)

    return df_base.drop(columns=["populacao"])
