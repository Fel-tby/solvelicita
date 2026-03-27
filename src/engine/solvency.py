"""
engine/solvency.py — Orquestrador do Score de Solvência v7.0.0
Metodologia completa em docs/METODOLOGIA.md.

Fórmula base (100 pts) — v7.0:
  S_base = 35·Lliq + 10·(1−Ccauc) + 15·Eorcam
         + 15·Qsiconfi + 10·Autonomia + 15·RPproc

Subtratores pós-base:
  −5 pts se lliq_parcial (fallback pré-RPNP)

Caps de classificação (ver engine/classifier.py):
  RPproc ≥ N_ANOS_CRONICOS_CAP_MEDIO (=4) anos crônicos → teto 🟡 Risco Médio
  Qsiconfi ≤ 2 anos → teto 🔴 Risco Alto
  Qsiconfi = 3 anos → teto 🟡 Risco Médio

Limiares v7.0: 80 / 60 / 40  (v6.2: 75 / 55 / 35)

Rodar individualmente:
  python src/engine/solvency.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
pd.set_option("future.no_silent_downcasting", True)

from utils.paths import PROCESSED, OUTPUTS
from scorers.config import PESOS, LIMIARES_SCORE, N_ANOS_CRONICOS_CAP_MEDIO
from scorers.lliq_scorer import calcular as calcular_lliq
from scorers.eorcam_scorer import calcular as calcular_eorcam
from scorers.qsiconfi_scorer import calcular as calcular_qsiconfi
from scorers.cauc_scorer import calcular as calcular_cauc
from scorers.autonomia_scorer import carregar_dca as calcular_autonomia
from scorers.rproc_scorer import calcular as calcular_rproc
from engine.classifier import classificar, ORDEM_SORT

VERSION = "v7.0.0"


def run() -> pd.DataFrame:
    """
    Calcula o score de solvência para todos os municípios PB.
    Salva em data/outputs/score_municipios_pb.csv e data/processed/.
    Retorna o DataFrame final.
    """

    print("=" * 65)
    print(f" Score de Solvência — SolveLicita {VERSION}")
    print(f" Pesos: Lliq={PESOS['lliq']} | Ccauc={PESOS['ccauc']} | "
          f"Eorcam={PESOS['eorcam']} | Qsi={PESOS['qsiconfi']} | "
          f"Aut={PESOS['autonomia']} | RPproc={PESOS['rproc']}")
    print(f" Limiares: Baixo≥{LIMIARES_SCORE['baixo']} | "
          f"Médio≥{LIMIARES_SCORE['medio']} | "
          f"Alto≥{LIMIARES_SCORE['alto']}")
    print("=" * 65)

    # ── 1. Carga ──────────────────────────────────────────────────────────
    print("\n📂 Carregando dados...")

    for path, label in [
        (PROCESSED / "siconfi_indicadores_pb.csv", "SICONFI indicadores"),
        (PROCESSED / "cauc_situacao_pb.csv",        "CAUC"),
        (PROCESSED / "municipios_pb_tabela.csv",    "Municípios"),
        (PROCESSED / "dca_indicadores_pb.csv",      "DCA"),
    ]:
        if not path.exists():
            raise FileNotFoundError(
                f"{label} não encontrado: {path}\n"
                "Execute as etapas de coleta e processamento antes do score."
            )

    df_si = pd.read_csv(PROCESSED / "siconfi_indicadores_pb.csv", dtype={"cod_ibge": str})
    df_ca = pd.read_csv(PROCESSED / "cauc_situacao_pb.csv",        dtype={"cod_ibge": str})
    df_mu = pd.read_csv(PROCESSED / "municipios_pb_tabela.csv",    dtype={"cod_ibge": str})

    df_si["entregou_rreo"] = df_si["entregou_rreo"].astype(str).str.lower() == "true"
    df_si["lliq_parcial"]  = df_si["lliq_parcial"].astype(str).str.lower()  == "true"

    print(f"  SICONFI : {df_si['cod_ibge'].nunique()} municípios × {df_si['ano'].nunique()} anos")
    print(f"  CAUC    : {len(df_ca)} municípios")

    # ── 2. Scorers ────────────────────────────────────────────────────────
    print("\n⚙️  Calculando indicadores...")

    df_eorcam   = calcular_eorcam(df_si)
    df_qsiconfi = calcular_qsiconfi(df_si)
    df_cauc     = calcular_cauc(df_ca)
    df_lliq     = calcular_lliq(df_si, df_mu)
    df_rproc    = calcular_rproc(df_si)

    print("  DCA : carregando dca_indicadores_pb.csv...")
    df_autonomia = calcular_autonomia(df_mu)
    print(f"  DCA : {df_autonomia['autonomia_norm'].notna().sum()} municípios com Autonomia")

    # RPNP — mantido para visualização no dashboard, sem peso no score
    df_rpnp = (
        df_si[
            df_si["ano"].isin([2020, 2021, 2022, 2023, 2024, 2025]) &
            df_si["entregou_rreo"] &
            df_si["rrestos_nproc_pct"].notna()
        ]
        .sort_values(["cod_ibge", "ano"], ascending=[True, False])
        .groupby("cod_ibge").first().reset_index()
        [["cod_ibge", "rrestos_nproc_pct"]]
    )

    # ── 3. Join ───────────────────────────────────────────────────────────
    df = df_mu[["cod_ibge", "ente", "populacao"]].copy()
    for bloco in [df_eorcam, df_qsiconfi, df_cauc, df_autonomia, df_lliq, df_rpnp, df_rproc]:
        df = df.merge(bloco, on="cod_ibge", how="left")

    df["qsiconfi"]        = df["qsiconfi"].fillna(0)
    df["anos_entregues"]  = df["anos_entregues"].fillna(0).astype(int)
    df["ccauc"]           = df["ccauc"].fillna(1.0)
    df["lliq_parcial"]    = df["lliq_parcial"].fillna(False).infer_objects(copy=False)
    df["n_anos_cronicos"] = df["n_anos_cronicos"].fillna(0).astype(int)
    df["contrib_lliq"]      = df["contrib_lliq"].fillna(0)
    df["contrib_autonomia"] = df["contrib_autonomia"].fillna(0)
    df["contrib_rproc"]     = df["contrib_rproc"].fillna(0)

    print(f"\n  Join  : {len(df)} municípios")
    print(f"  Sem RREO : {df['eorcam_raw'].isna().sum()}")
    print(f"  Com Lliq : {df['lliq_raw'].notna().sum()} | parcial: {df['lliq_parcial'].sum()}")
    print(f"  Crônicos estruturais (≥{N_ANOS_CRONICOS_CAP_MEDIO} anos): "
          f"{(df['n_anos_cronicos'] >= N_ANOS_CRONICOS_CAP_MEDIO).sum()}")

    # ── 4. Score base ─────────────────────────────────────────────────────
    df["score_base"] = (
        df["contrib_lliq"]      +
        df["contrib_eorcam"]    +
        df["contrib_qsiconfi"]  +
        df["contrib_ccauc"]     +
        df["contrib_autonomia"] +
        df["contrib_rproc"]
    )

    # ── 5. Subtratores situacionais ───────────────────────────────────────
    df["pen_lliq_parcial"] = df["lliq_parcial"].apply(lambda x: -5.0 if x else 0.0)
    df["pen_situacional"]  = df[["pen_lliq_parcial"]].sum(axis=1).clip(lower=-10.0)
    df["score_bruto"]      = (df["score_base"] + df["pen_situacional"]).clip(lower=0)
    df["score"]            = df["score_bruto"].round(1)
    df.loc[df["eorcam_raw"].isna(), "score"] = None

    # ── 6. Flags ──────────────────────────────────────────────────────────
    df["dado_suspeito"]    = df.get("dado_suspeito_lliq", pd.Series(False, index=df.index)).fillna(False)
    df["autonomia_critica"] = df["autonomia_media"].notna() & (df["autonomia_media"] < 0.08)

    # ── 7. Classificação ──────────────────────────────────────────────────
    df["classificacao"] = df.apply(
        lambda r: classificar(r["score"], r["anos_entregues"], r["n_anos_cronicos"]),
        axis=1,
    )

    # ── 8. Diagnóstico ────────────────────────────────────────────────────
    n_cap_qsi   = (
        df["classificacao"].isin(["🔴 Risco Alto", "🟡 Risco Médio"]) &
        (df["anos_entregues"] <= 3) &
        df["score"].notna()
    ).sum()
    n_cap_rproc = (df["n_anos_cronicos"] >= N_ANOS_CRONICOS_CAP_MEDIO).sum()
    n_pen_parc  = (df["pen_lliq_parcial"] < 0).sum()
    n_aut_crit  = df["autonomia_critica"].sum()
    stats       = df["score"].dropna()

    print("\n🔍 Distribuição de risco:")
    print(df["classificacao"].value_counts().to_string())
    print(f"\n  Score médio    : {stats.mean():.1f}")
    print(f"  Score mediano  : {stats.median():.1f}")
    print(f"  Score mínimo   : {stats.min():.1f}")
    print(f"  Score máximo   : {stats.max():.1f}")
    print(f"  Cap Qsiconfi   : {n_cap_qsi} rebaixados")
    print(f"  Cap RPcrônico  : {n_cap_rproc} municípios "
          f"(≥{N_ANOS_CRONICOS_CAP_MEDIO} anos acima de 3%)")
    print(f"  Pen lliq parc  : {n_pen_parc} (−5 pts, fallback pré-RPNP)")
    print(f"  Flag aut.crit. : {n_aut_crit} municípios (autonomia < 8% Rec.Corrente)")
    print(f"  dado_suspeito  : {df['dado_suspeito'].sum()}")

    COLS_DEBUG = [
        "ente", "score", "classificacao", "anos_entregues",
        "eorcam_raw", "lliq_raw", "rproc_norm", "n_anos_cronicos",
        "rrestos_nproc_pct", "qsiconfi", "ccauc", "autonomia_media",
        "dias_atraso", "decay_fator", "dado_suspeito",
    ]
    print("\n🏆 Top 10:")
    print(df.nlargest(10, "score")[COLS_DEBUG].to_string(index=False))
    print("\n⚠️  Bottom 10:")
    print(df.nsmallest(10, "score")[COLS_DEBUG].to_string(index=False))

    CHAVE = ["João Pessoa", "Campina Grande", "Sousa", "Patos",
             "Cajazeiras", "Santa Rita", "Bayeux", "Queimadas"]
    mask = df["ente"].apply(lambda x: any(c.lower() in str(x).lower() for c in CHAVE))
    print("\n🔎 Municípios-chave:")
    print(df[mask][[
        "ente", "score", "classificacao", "lliq_raw",
        "n_anos_cronicos", "contrib_lliq", "contrib_rproc",
        "pen_situacional", "dias_atraso", "decay_fator",
    ]].to_string(index=False))

    # ── 9. Exportação ─────────────────────────────────────────────────────
    OUT_COLS = [
        "cod_ibge", "ente", "populacao", "score", "classificacao",
        "anos_entregues",
        "eorcam_raw", "lliq_raw", "rrestos_nproc_pct", "n_anos_cronicos",
        "qsiconfi", "ccauc", "autonomia_media",
        "eorcam_norm", "lliq_norm", "rproc_norm", "autonomia_norm",
        "contrib_eorcam", "contrib_lliq", "contrib_qsiconfi",
        "contrib_ccauc", "contrib_autonomia", "contrib_rproc",
        "pen_lliq_parcial", "pen_situacional",
        "score_base", "score_bruto",
        "dias_atraso", "decay_fator",
        "dado_suspeito", "dado_suspeito_lliq",
        "dado_defasado", "lliq_parcial", "autonomia_critica",
    ]

    df_out = df[OUT_COLS].copy()
    df_out["_ordem"] = df_out["classificacao"].map(ORDEM_SORT)
    df_out = (
        df_out
        .sort_values(["_ordem", "score"], ascending=[True, False], na_position="last")
        .drop(columns="_ordem")
    )

    df_out.to_csv(OUTPUTS  / "score_municipios_pb.csv", index=False, encoding="utf-8-sig")
    df_out.to_csv(PROCESSED / "score_municipios_pb.csv", index=False, encoding="utf-8-sig")

    print(f"\n✅ Score calculado : {df_out['score'].notna().sum()} municípios")
    print(f"   Versão          : {VERSION}")
    print(f"   Salvo em        : data/outputs/score_municipios_pb.csv")
    print("=" * 65)

    return df_out


if __name__ == "__main__":
    run()
