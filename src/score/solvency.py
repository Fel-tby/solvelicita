"""
Motor de cálculo do Score de Solvência (0–100) — v6.4.0
Metodologia completa em METODOLOGIA_v6.md.

Fórmula base (100 pts):
  S_base = 30·f(Lliq) + 20·(1−Ccauc) + 20·g(Eorcam)
         + 15·Qsiconfi + 10·h(Autonomia) + 5·i(RPproc_crônico)

Subtratores pós-base (situacional):
  −5 pts  se lliq_parcial (fallback pré-RPNP)

Caps de classificação (por ordem de precedência):
  1. RPproc crônico  : ≥ 5 anos acima de 3% → teto 🟡 Risco Médio
  2. Qsiconfi        : ≤ 2 anos → teto 🔴 | = 3 anos → teto 🟡

Confidence decay sobre contrib_lliq:
  Bimestral (> 50k hab) : janela aceitável de 90 dias
  Semestral (≤ 50k hab) : janela aceitável de 210 dias
  Fator de decay = max(0, 1 − (dias_atraso − janela) / 365)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date
from dca_scorer import carregar_dca


pd.set_option("future.no_silent_downcasting", True)


BASE_DIR  = Path(__file__).resolve().parent.parent.parent
PROCESSED = BASE_DIR / "data" / "processed"
OUTPUTS   = BASE_DIR / "data" / "outputs"
OUTPUTS.mkdir(parents=True, exist_ok=True)


# ── Pesos ─────────────────────────────────────────────────────────────────────
PESOS = {
    "lliq"     : 30,
    "ccauc"    : 20,
    "eorcam"   : 20,
    "qsiconfi" : 15,
    "autonomia": 10,
    "rproc"    :  5,
}
assert sum(PESOS.values()) == 100


# ── Janela temporal e pesos de recência para Eorcam ──────────────────────────
ANOS_REF  = [2020, 2021, 2022, 2023, 2024, 2025]
N_ANOS    = len(ANOS_REF)
PESOS_ANO = {2025: 0.40, 2024: 0.25, 2023: 0.20, 2022: 0.10, 2021: 0.05, 2020: 0.00}


# ── Limiar de cronicidade de RP Processados ───────────────────────────────────
LIMIAR_RPROC_CRONICO = 3.0   # % da receita realizada


# ── Data de referência para cálculo de defasagem de dado ─────────────────────
HOJE = date.today()
FIM_PERIODO_MES = {
    ("Q", 1): 4, ("Q", 2): 8,  ("Q", 3): 12,
    ("S", 1): 6, ("S", 2): 12,
}


# ── Classificação de pendências CAUC ─────────────────────────────────────────
PENDENCIAS_GRAVES = {
    "Regularidade Fiscal (RFB)", "Regularidade PGFN", "CADIN",
    "SISTN (Dívida Consolidada)", "LRF - Limite Pessoal Executivo",
    "Adimplência TCU", "Adimplência CGU",
}
PENDENCIAS_MODERADAS = {
    "Regularidade FGTS", "Regularidade Trabalhista (TST)",
    "SIOPS (Saúde)", "SIOPE (Educação)",
    "SICONV/TRANSFEREGOV Prestação de Contas",
    "SISTN (Garantias)", "LRF - Limite Pessoal Legislativo",
}


print("=" * 65)
print(" Score de Solvência — SolveLicita  v6.4.0")
print(f" Pesos: Lliq={PESOS['lliq']} | Ccauc={PESOS['ccauc']} | "
      f"Eorcam={PESOS['eorcam']} | Qsi={PESOS['qsiconfi']} | "
      f"Aut={PESOS['autonomia']} | RPproc={PESOS['rproc']}")
print("=" * 65)


# ── 1. Carga ──────────────────────────────────────────────────────────────────
print("\n📂 Carregando dados...")
df_si = pd.read_csv(PROCESSED / "siconfi_indicadores_pb.csv")
df_ca = pd.read_csv(PROCESSED / "cauc_situacao_pb.csv")
df_mu = pd.read_csv(PROCESSED / "municipios_pb_tabela.csv")

df_si["cod_ibge"]      = df_si["cod_ibge"].astype(str)
df_ca["cod_ibge"]      = df_ca["cod_ibge"].astype(str)
df_mu["cod_ibge"]      = df_mu["cod_ibge"].astype(str)
df_si["entregou_rreo"] = df_si["entregou_rreo"].astype(str).str.lower() == "true"
df_si["lliq_parcial"]  = df_si["lliq_parcial"].astype(str).str.lower() == "true"

print(f"  SICONFI : {df_si['cod_ibge'].nunique()} municípios × {df_si['ano'].nunique()} anos")
print(f"  CAUC    : {len(df_ca)} municípios")


# ── Funções de pontuação ──────────────────────────────────────────────────────

def pontuar_lliq(x: float):
    """
    Curva contínua mapeando DCL/Receita → [0, 1].
    Máximo em Lliq ≥ 0.20. Valores abaixo de −0.50 são capados antes do cálculo
    e sinalizados separadamente como dado_suspeito.
    """
    if pd.isna(x): return None
    x = max(x, -0.50)
    if x >= 0.20: return 1.00
    if x >= 0.10: return round(0.75 + (x - 0.10) / 0.10 * 0.25, 4)
    if x >= 0.00: return round(0.50 + (x / 0.10) * 0.25, 4)
    return round(max(0.0, 0.50 * (1 - (-x / 0.50)) ** 2), 4)


def pontuar_eorcam(x: float):
    """
    Receita realizada / prevista em %. Zona ótima: 90–105%.
    Excesso acima de 120% recebe teto de 0.5 (arrecadação não sustentável).
    Abaixo de 70% indica colapso de arrecadação — pontuação zero.
    """
    if pd.isna(x): return None
    if 90 <= x <= 105: return 1.0
    if x > 120:        return 0.5
    if x > 105:        return round(1.0 - (x - 105) / 30, 4)
    if x >= 70:        return round((x - 70) / 20, 4)
    return 0.0


def pontuar_ccauc(s: str) -> float:
    """
    Converte string de pendências CAUC em score de risco [0, 1].
    Pendência grave zera qualquer benefício — resultado: ccauc = 1.0 → contrib_ccauc = 0 pts.
    Pendências moderadas e leves são ponderadas com teto em 0.5.
    Município regular retorna 0.0 → contrib_ccauc = 20 pts (máximo).
    """
    if not isinstance(s, str) or s.strip() == "REGULAR":
        return 0.0
    itens = [p.strip() for p in s.split("|")]
    if any(i in PENDENCIAS_GRAVES for i in itens):
        return 1.0
    n_mod  = sum(1 for i in itens if i in PENDENCIAS_MODERADAS)
    n_leve = sum(1 for i in itens if i not in PENDENCIAS_MODERADAS)
    return round(min((n_mod * 2 + n_leve) / 20, 0.5), 4)


def pontuar_rproc_cronico(n: int) -> float:
    """
    Penaliza o padrão histórico de manter RP Processados > 3% da receita.
    n_anos_cronicos é a contagem de anos (2020–2025) acima desse limiar.
    Quanto mais anos consecutivos acima do limiar, menor a pontuação.
    """
    mapa = {0: 1.00, 1: 0.75, 2: 0.50, 3: 0.30, 4: 0.10, 5: 0.00, 6: 0.00}
    return mapa.get(int(n), 0.00)


def dias_atraso_dado(ano, periodo, periodicidade) -> int:
    """
    Calcula quantos dias se passaram desde a data esperada de publicação
    do RGF Anexo 05 mais recente disponível para o município.
    Assume 2 meses de prazo de publicação após o fim do período.
    Retorna 999 quando o dado de período/periodicidade está ausente.
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


def fator_decay(dias: int, populacao: int) -> float:
    """
    Fator multiplicativo sobre contrib_lliq baseado na defasagem do RGF.
    decay = max(0, 1 − (dias_atraso − janela) / 365)
    """
    janela = 90 if int(populacao) > 50_000 else 210
    if dias <= janela:
        return 1.00
    return round(max(0.0, 1.0 - (dias - janela) / 365.0), 4)


# ── 2. Qsiconfi ───────────────────────────────────────────────────────────────
df_qsi = (
    df_si[df_si["ano"].isin(ANOS_REF)]
    .groupby("cod_ibge")["entregou_rreo"]
    .agg(anos_entregues="sum")
    .reset_index()
)
df_qsi["qsiconfi"] = df_qsi["anos_entregues"] / N_ANOS


# ── 3. Eorcam ponderado por recência ─────────────────────────────────────────
df_eo = df_si[
    df_si["ano"].isin(ANOS_REF) &
    df_si["entregou_rreo"] &
    df_si["eorcam"].notna()
].copy()
df_eo["peso_ano"] = df_eo["ano"].map(PESOS_ANO).fillna(0)
df_eo = df_eo[df_eo["peso_ano"] > 0]
df_eo["eorcam_w"] = df_eo["eorcam"] * df_eo["peso_ano"]
df_eorcam = (
    df_eo.groupby("cod_ibge")
    .apply(
        lambda g: round(g["eorcam_w"].sum() / g["peso_ano"].sum(), 4),
        include_groups=False,
    )
    .reset_index()
    .rename(columns={0: "eorcam_raw"})
)


# ── 4. Lliq — dado mais recente disponível ────────────────────────────────────
df_lliq_base = (
    df_si[df_si["ano"].isin(ANOS_REF) & df_si["lliq"].notna()]
    .assign(_per_sort=lambda x: x["periodo_rgf"].fillna(-1))
    .sort_values(["cod_ibge", "ano", "_per_sort"], ascending=[True, False, False])
    .groupby("cod_ibge")
    .first()
    .reset_index()
    [["cod_ibge", "lliq", "ano", "periodo_rgf", "periodicidade_rgf", "lliq_parcial"]]
    .rename(columns={
        "lliq":              "lliq_raw",
        "ano":               "lliq_ano",
        "periodo_rgf":       "lliq_periodo",
        "periodicidade_rgf": "lliq_periodicidade",
    })
)


# ── 5. RPNP recente (mantido no dataframe para visualização, sem penalidade no score)
df_rpnp = (
    df_si[
        df_si["ano"].isin(ANOS_REF) &
        df_si["entregou_rreo"] &
        df_si["rrestos_nproc_pct"].notna()
    ]
    .sort_values(["cod_ibge", "ano"], ascending=[True, False])
    .groupby("cod_ibge").first().reset_index()
    [["cod_ibge", "rrestos_nproc_pct"]]
)


# ── 6. RP Processados crônico ─────────────────────────────────────────────────
df_rproc = (
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


# ── 7. CAUC ───────────────────────────────────────────────────────────────────
df_ca["ccauc"] = df_ca["pendencias"].apply(pontuar_ccauc)


# ── 8. Autonomia (DCA/FINBRA) ────────────────────────────────────────────────
print("  DCA     : carregando dca_indicadores_pb.csv...")
df_dca = carregar_dca(df_mu)
print(f"  DCA     : {df_dca['autonomia_norm'].notna().sum()} municípios com Autonomia")


# ── 9. Join ───────────────────────────────────────────────────────────────────
df = df_mu[["cod_ibge", "ente", "populacao"]].copy()
df = df.merge(df_eorcam,   on="cod_ibge", how="left")
df = df.merge(df_qsi[["cod_ibge", "qsiconfi", "anos_entregues"]], on="cod_ibge", how="left")
df = df.merge(df_ca[["cod_ibge", "ccauc"]], on="cod_ibge", how="left")
df = df.merge(
    df_dca[["cod_ibge", "autonomia_media", "autonomia_norm", "contrib_autonomia"]],
    on="cod_ibge", how="left",
)
df = df.merge(df_lliq_base, on="cod_ibge", how="left")
df = df.merge(df_rpnp,      on="cod_ibge", how="left")
df = df.merge(df_rproc,     on="cod_ibge", how="left")


# ── Fallbacks ─────────────────────────────────────────────────────────────────
df["qsiconfi"]        = df["qsiconfi"].fillna(0)
df["anos_entregues"]  = df["anos_entregues"].fillna(0).astype(int)
df["ccauc"]           = df["ccauc"].fillna(1.0)    # ausência no CAUC → pior caso
df["lliq_parcial"]    = df["lliq_parcial"].fillna(False).infer_objects(copy=False)
df["n_anos_cronicos"] = df["n_anos_cronicos"].fillna(0).astype(int)

print(f"\n  Join    : {len(df)} municípios")
print(f"  Sem RREO: {df['eorcam_raw'].isna().sum()}")
print(f"  Com Lliq: {df['lliq_raw'].notna().sum()} | parcial: {df['lliq_parcial'].sum()}")
print(f"  Com rproc (≥1 ano crônico): {(df['n_anos_cronicos'] >= 1).sum()}")
print(f"  Crônicos estruturais (5–6 anos): {(df['n_anos_cronicos'] >= 5).sum()}")


# ── 10. Lliq: pontuação e confidence decay ────────────────────────────────────
df["lliq_norm"]          = df["lliq_raw"].apply(pontuar_lliq)
df["dado_suspeito_lliq"] = df["lliq_raw"].notna() & (df["lliq_raw"] < -0.50)

df["dias_atraso"] = df.apply(
    lambda r: dias_atraso_dado(r["lliq_ano"], r["lliq_periodo"], r["lliq_periodicidade"])
    if pd.notnull(r.get("lliq_ano")) else 999,
    axis=1,
)
df["decay_fator"] = df.apply(
    lambda r: fator_decay(r["dias_atraso"], r["populacao"]), axis=1
)
df["dado_defasado"] = df.apply(
    lambda r: r["dias_atraso"] > (90 if r["populacao"] > 50_000 else 210), axis=1
)


# ── 11. Contribuições ao score base ───────────────────────────────────────────
df["eorcam_norm"]  = df["eorcam_raw"].apply(pontuar_eorcam)
df["rproc_norm"]   = df["n_anos_cronicos"].apply(pontuar_rproc_cronico)

df["contrib_lliq"]      = (PESOS["lliq"]      * df["lliq_norm"].fillna(0)) * df["decay_fator"]
df["contrib_eorcam"]    =  PESOS["eorcam"]    * df["eorcam_norm"].fillna(0)
df["contrib_qsiconfi"]  =  PESOS["qsiconfi"]  * df["qsiconfi"]
df["contrib_ccauc"]     =  PESOS["ccauc"]     * (1 - df["ccauc"])
df["contrib_autonomia"] =  df["contrib_autonomia"].fillna(0)
df["contrib_rproc"]     =  PESOS["rproc"]     * df["rproc_norm"]

df["score_base"] = (
    df["contrib_lliq"]      +
    df["contrib_eorcam"]    +
    df["contrib_qsiconfi"]  +
    df["contrib_ccauc"]     +
    df["contrib_autonomia"] +
    df["contrib_rproc"]
)


# ── 12. Subtratores situacionais e Flags ──────────────────────────────────────
df["pen_lliq_parcial"] = df["lliq_parcial"].apply(
    lambda x: -5.0 if x else 0.0
)

# autonomia_media está em escala decimal (ex: 0.0296 = 2,96% da Receita Corrente Total)
df["autonomia_critica"] = (
    df["autonomia_media"].notna() & (df["autonomia_media"] < 0.08)
)

df["pen_situacional"] = (df["pen_lliq_parcial"]).clip(lower=-5.0)

df["score_bruto"] = (df["score_base"] + df["pen_situacional"]).clip(lower=0)
df["score"]       = df["score_bruto"].round(1)
df.loc[df["eorcam_raw"].isna(), "score"] = None

df["dado_suspeito"] = df["dado_suspeito_lliq"].fillna(False)


# ── 13. Classificação ─────────────────────────────────────────────────────────
ORDEM_RISCO = ["🟢 Risco Baixo", "🟡 Risco Médio", "🔴 Risco Alto", "⛔ Crítico"]
ORDEM_SORT  = {c: i for i, c in enumerate(ORDEM_RISCO + ["⚫ Sem Dados"])}


def classificar(score, anos_entregues: int, n_anos_cronicos: int) -> str:
    """
    Atribui etiqueta de risco ao score numérico, aplicando caps por ordem de precedência:
      1. RPproc crônico — ≥ 5 anos acima de 3% → teto Risco Médio
      2. Qsiconfi       — histórico de transparência RREO
    Ausência total de RREO (anos_entregues = 0) resulta em Sem Dados.
    """
    if pd.isna(score):
        return "⚫ Sem Dados"

    if score >= 75:   classe = "🟢 Risco Baixo"
    elif score >= 55: classe = "🟡 Risco Médio"
    elif score >= 35: classe = "🔴 Risco Alto"
    else:             classe = "⛔ Crítico"

    if int(n_anos_cronicos) >= 5:
        cap = "🟡 Risco Médio"
        classe = ORDEM_RISCO[max(ORDEM_RISCO.index(classe), ORDEM_RISCO.index(cap))]

    n = int(anos_entregues) if pd.notnull(anos_entregues) else 0
    if n == 0:
        return "⚫ Sem Dados"
    if n <= 2:
        cap = "🔴 Risco Alto"
        classe = ORDEM_RISCO[max(ORDEM_RISCO.index(classe), ORDEM_RISCO.index(cap))]
    elif n == 3:
        cap = "🟡 Risco Médio"
        classe = ORDEM_RISCO[max(ORDEM_RISCO.index(classe), ORDEM_RISCO.index(cap))]

    return classe


df["classificacao"] = df.apply(
    lambda r: classificar(
        r["score"], r["anos_entregues"], r["n_anos_cronicos"]
    ),
    axis=1,
)


# ── 14. Diagnóstico ───────────────────────────────────────────────────────────
n_cap_qsi   = (df["classificacao"].isin(["🔴 Risco Alto", "🟡 Risco Médio"]) & (df["anos_entregues"] <= 3) & df["score"].notna()).sum()
n_cap_rproc = (df["n_anos_cronicos"] >= 5).sum()
n_pen_parc  = (df["pen_lliq_parcial"] < 0).sum()
n_aut_crit  = df["autonomia_critica"].sum()

print("\n🔍 Distribuição de risco:")
print(df["classificacao"].value_counts().to_string())

stats = df["score"].dropna()
print(f"\n  Score médio   : {stats.mean():.1f}")
print(f"  Score mediano : {stats.median():.1f}")
print(f"  Score mínimo  : {stats.min():.1f}")
print(f"  Score máximo  : {stats.max():.1f}")
print(f"  Cap Qsiconfi  : {n_cap_qsi} rebaixados")
print(f"  Cap RPcrônico : {n_cap_rproc} municípios (≥5 anos acima de 3%)")
print(f"  Pen lliq parc : {n_pen_parc} (−5 pts, fallback pré-RPNP)")
print(f"  Flag aut.crit.: {n_aut_crit} municípios (autonomia < 8% Rec.Corrente)")
print(f"  dado_suspeito : {df['dado_suspeito'].sum()}")

COLS = ["ente", "score", "classificacao", "anos_entregues",
        "eorcam_raw", "lliq_raw", "rproc_norm", "n_anos_cronicos",
        "rrestos_nproc_pct", "qsiconfi", "ccauc", "autonomia_media",
        "dias_atraso", "decay_fator", "dado_suspeito"]
print("\n🏆 Top 10:")
print(df.nlargest(10, "score")[COLS].to_string(index=False))
print("\n⚠️  Bottom 10:")
print(df.nsmallest(10, "score")[COLS].to_string(index=False))

CHAVE = ["João Pessoa", "Campina Grande", "Sousa", "Patos",
         "Cajazeiras", "Santa Rita", "Bayeux", "Queimadas"]
mask = df["ente"].apply(lambda x: any(c.lower() in str(x).lower() for c in CHAVE))
print("\n🔎 Municípios-chave:")
print(df[mask][[
    "ente", "score", "classificacao", "lliq_raw",
    "n_anos_cronicos", "contrib_lliq", "contrib_rproc",
    "pen_situacional", "dias_atraso", "decay_fator"
]].to_string(index=False))


# ── 15. Exportação ────────────────────────────────────────────────────────────
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

df_out.to_csv(OUTPUTS   / "score_municipios_pb.csv", index=False, encoding="utf-8-sig")
df_out.to_csv(PROCESSED / "score_municipios_pb.csv", index=False, encoding="utf-8-sig")

print(f"\n✅ Score calculado : {df_out['score'].notna().sum()} municípios")
print(f"   Versão          : v6.4.0")
print(f"   Salvo em        : data/outputs/score_municipios_pb.csv")
print("=" * 65)