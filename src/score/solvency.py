"""
Motor de cálculo do Score de Solvência (0–100) para municípios da Paraíba.
Fase 0 — score completo com DCA integrado (Balanço Patrimonial).
Metodologia completa em METODOLOGIA.md.

v4 — integra Scaixa e Autonomia Tributária via dca_scorer.py.
     Pesos redistribuídos: 6 variáveis, total 100 pts.
     Flag dado_suspeito emitido no diagnóstico e propagado para o CSV.
"""

import pandas as pd
from pathlib import Path
from dca_scorer import carregar_dca

# ── Configurações de Diretórios ───────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent.parent
PROCESSED = BASE_DIR / "data" / "processed"
OUTPUTS   = BASE_DIR / "data" / "outputs"
OUTPUTS.mkdir(parents=True, exist_ok=True)

# ── Pesos da fórmula (METODOLOGIA.md v5.0) ───────────────────────────────────
# Fase 0: DCA entra com 30 pts — pesos originais reduzidos proporcionalmente.
# Critérios pendentes (DataJud, TCU, CEIS) permanecem reservados como risco zero.
PESOS = {
    "eorcam":    22,   # fluxo de caixa real             (era 31)
    "rrestos":   18,   # dívida herdada — preditor calote (era 25)
    "qsiconfi":  14,   # transparência e governança       (era 19)
    "ccauc":     16,   # bloqueio de repasse federal      (era 25)
    "scaixa":    20,   # solvência patrimonial — DCA novo
    "autonomia": 10,   # autonomia tributária  — DCA novo
    # "datajud":  7,   # reservado Fase 2
    # "atcu":     5,   # reservado Fase 2
    # "sceis":    3,   # reservado Fase 2
}
PESO_DISPONIVEL = sum(PESOS.values())  # 100

# Anos fiscais completos (2025 excluído — exercício incompleto)
ANOS_REF = [2020, 2021, 2022, 2023, 2024]

# ── Classificação de pendências CAUC por gravidade ────────────────────────────
PENDENCIAS_GRAVES = {
    "Regularidade Fiscal (RFB)",
    "Regularidade PGFN",
    "CADIN",
    "SISTN (Dívida Consolidada)",
    "LRF - Limite Pessoal Executivo",
    "Adimplência TCU",
    "Adimplência CGU",
}

PENDENCIAS_MODERADAS = {
    "Regularidade FGTS",
    "Regularidade Trabalhista (TST)",
    "SIOPS (Saúde)",
    "SIOPE (Educação)",
    "SICONV/TRANSFEREGOV Prestação de Contas",
    "SISTN (Garantias)",
    "LRF - Limite Pessoal Legislativo",
}

print("=" * 65)
print(" Score de Solvência — SolveLicita")
print(f" Fase 0 — score completo ({PESO_DISPONIVEL} pts) com DCA integrado")
print(" Modo: limiares conservadores (credit rating)")
print("=" * 65)

# ── 1. Carga dos dados ────────────────────────────────────────────────────────
print("\n📂 Carregando dados...")
df_si = pd.read_csv(PROCESSED / "siconfi_indicadores_pb.csv")
df_ca = pd.read_csv(PROCESSED / "cauc_situacao_pb.csv")
df_mu = pd.read_csv(PROCESSED / "municipios_pb_tabela.csv")

df_si["cod_ibge"] = df_si["cod_ibge"].astype(str)
df_ca["cod_ibge"] = df_ca["cod_ibge"].astype(str)
df_mu["cod_ibge"] = df_mu["cod_ibge"].astype(str)
df_si["entregou_rreo"] = df_si["entregou_rreo"].astype(str).str.lower() == "true"

print(f"  SICONFI : {df_si['cod_ibge'].nunique()} municípios × {df_si['ano'].nunique()} anos")
print(f"  CAUC    : {len(df_ca)} municípios")
print(f"  Tabela  : {len(df_mu)} municípios")

# ── Funções de pontuação ──────────────────────────────────────────────────────
# Todas retornam BONDADE: 1.0 = melhor, 0.0 = pior

def pontuar_eorcam(x: float) -> float:
    """
    Pontuação por limiar fixo de execução orçamentária.
    ≥90% e ≤105% → 1.0: zona saudável (execução precisa).
    105–120%      → decaimento linear: excesso esporádico, não mérito.
    >120%         → 0.5: arrecadação anômala não garante solvência.
    70–90%        → proporcional: zona de atenção.
    ≤70%          → 0.0: colapso de arrecadação ou orçamento fictício.
    """
    if pd.isna(x): return None
    if 90 <= x <= 105: return 1.0
    if x > 120:        return 0.5
    if x > 105:        return round(1.0 - (x - 105) / 30, 4)
    if x >= 70:        return round((x - 70) / 20, 4)
    return 0.0

def pontuar_rrestos(x: float) -> float:
    """
    Threshold calibrado para municípios brasileiros.
    0%    → 1.0: sem dívida herdada.
    0–3%  → decaimento linear suave: faixa aceitável.
    3–10% → decaimento quadrático agressivo: zona de risco.
    ≥10%  → 0.0: dívida crítica.
    """
    if pd.isna(x): return None
    if x <= 0:  return 1.0
    if x >= 10: return 0.0
    if x <= 3:  return round(1.0 - (x / 3) * 0.3, 4)
    return round(0.7 * (1 - (x - 3) / 7) ** 2, 4)

def pontuar_ccauc(pendencias_str: str) -> float:
    """
    Risco CAUC: 0.0 (regular) → 1.0 (crítico).
    Gatilho punitivo: qualquer pendência GRAVE → 1.0 (contribuição zero).
    Apenas moderadas/leves → penalidade proporcional, teto 0.5.
    """
    if not isinstance(pendencias_str, str) or pendencias_str.strip() == "REGULAR":
        return 0.0
    itens = [p.strip() for p in pendencias_str.split("|")]
    if any(item in PENDENCIAS_GRAVES for item in itens):
        return 1.0
    n_mod  = sum(1 for i in itens if i in PENDENCIAS_MODERADAS)
    n_leve = sum(1 for i in itens if i not in PENDENCIAS_MODERADAS)
    return round(min((n_mod * 2 + n_leve * 1) / 20, 0.5), 4)

# ── 2. Qsiconfi ───────────────────────────────────────────────────────────────
df_qsi = (
    df_si[df_si["ano"].isin(ANOS_REF)]
    .groupby("cod_ibge")["entregou_rreo"]
    .sum()
    .div(len(ANOS_REF))
    .reset_index()
    .rename(columns={"entregou_rreo": "qsiconfi"})
)

# ── 3. Eorcam e Rrestos ───────────────────────────────────────────────────────
df_fis = (
    df_si[df_si["ano"].isin(ANOS_REF) & df_si["entregou_rreo"]]
    .groupby("cod_ibge")
    .agg(
        eorcam_raw  = ("eorcam",           "mean"),
        rrestos_raw = ("rrestos_nproc_pct", "mean"),
    )
    .reset_index()
)
df_fis["eorcam_norm"]  = df_fis["eorcam_raw"].apply(pontuar_eorcam)
df_fis["rrestos_norm"] = df_fis["rrestos_raw"].apply(pontuar_rrestos)

# ── 4. CAUC ───────────────────────────────────────────────────────────────────
df_ca["ccauc"] = df_ca["pendencias"].apply(pontuar_ccauc)

# ── 5. DCA — Scaixa e Autonomia ───────────────────────────────────────────────
print("  DCA     : carregando dca_indicadores_pb.csv...")
df_dca = carregar_dca(df_mu)
print(f"  DCA     : {df_dca['scaixa_norm'].notna().sum()} municípios com Scaixa")
print(f"  DCA     : {df_dca['autonomia_norm'].notna().sum()} municípios com Autonomia")

n_suspeitos = df_dca["dado_suspeito"].sum()
if n_suspeitos:
    print(f"\n  ⚠️  {n_suspeitos} município(s) com Scaixa anômalo (dado_suspeito=True):")
    cols_flag = ["cod_ibge", "scaixa_medio"]
    print(df_dca[df_dca["dado_suspeito"]][cols_flag].to_string(index=False))
    print("     → Capping aplicado em -0.50. Provável distorção RPPS.")
    print("     → Verifique o Balanço Patrimonial manualmente.")

# ── 6. Join na tabela mestra ──────────────────────────────────────────────────
df = df_mu[["cod_ibge", "ente", "populacao"]].copy()

df = df.merge(
    df_fis[["cod_ibge", "eorcam_raw", "rrestos_raw", "eorcam_norm", "rrestos_norm"]],
    on="cod_ibge", how="left"
)
df = df.merge(df_qsi,                          on="cod_ibge", how="left")
df = df.merge(df_ca[["cod_ibge", "ccauc"]],    on="cod_ibge", how="left")
df = df.merge(
    df_dca[[
        "cod_ibge",
        "scaixa_medio", "autonomia_media",
        "scaixa_norm",  "autonomia_norm",
        "contrib_scaixa", "contrib_autonomia",
        "dado_suspeito",
    ]],
    on="cod_ibge", how="left"
)

# Fallbacks conservadores
df["qsiconfi"]    = df["qsiconfi"].fillna(0)
df["ccauc"]       = df["ccauc"].fillna(1.0)       # sem CAUC = pior caso
df["rrestos_norm"] = df["rrestos_norm"].fillna(0.0)

print(f"\n  Join: {len(df)} municípios")
print(f"  Sem dados SICONFI: {df['eorcam_raw'].isna().sum()} (score não calculado)")

# ── 7. Aplicar fórmula ────────────────────────────────────────────────────────
df["contrib_eorcam"]   = PESOS["eorcam"]   * df["eorcam_norm"].fillna(0)
df["contrib_rrestos"]  = PESOS["rrestos"]  * df["rrestos_norm"]
df["contrib_qsiconfi"] = PESOS["qsiconfi"] * df["qsiconfi"]
df["contrib_ccauc"]    = PESOS["ccauc"]    * (1 - df["ccauc"])

# DCA: contrib já calculada pelo dca_scorer; fallback 0 se não coletado
df["contrib_scaixa"]    = df["contrib_scaixa"].fillna(0)
df["contrib_autonomia"] = df["contrib_autonomia"].fillna(0)

df["score_bruto"] = (
    df["contrib_eorcam"]   +
    df["contrib_rrestos"]  +
    df["contrib_qsiconfi"] +
    df["contrib_ccauc"]    +
    df["contrib_scaixa"]   +
    df["contrib_autonomia"]
)

df["score"] = df["score_bruto"].round(1)
df.loc[df["eorcam_raw"].isna(), "score"] = None  # sem SICONFI = sem score

# ── 8. Classificação de risco ─────────────────────────────────────────────────
def classificar(s):
    if pd.isna(s):  return "⚫ Sem Dados"
    if s >= 75:     return "🟢 Risco Baixo"
    if s >= 55:     return "🟡 Risco Médio"
    if s >= 35:     return "🔴 Risco Alto"
    return "⛔ Crítico"

df["classificacao"] = df["score"].apply(classificar)

# ── 9. Diagnóstico geral ──────────────────────────────────────────────────────
print("\n🔍 Distribuição de risco:")
print(df["classificacao"].value_counts().to_string())

stats = df["score"].dropna()
print(f"\n  Score médio   : {stats.mean():.1f}")
print(f"  Score mediano : {stats.median():.1f}")
print(f"  Score mínimo  : {stats.min():.1f}")
print(f"  Score máximo  : {stats.max():.1f}")

COLS = ["ente", "score", "classificacao", "eorcam_raw", "rrestos_raw",
        "qsiconfi", "ccauc", "scaixa_medio", "autonomia_media", "dado_suspeito"]

print("\n🏆 Top 10 — Menor risco:")
print(df.nlargest(10, "score")[COLS].to_string(index=False))

print("\n⚠️  Bottom 10 — Maior risco:")
print(df.nsmallest(10, "score")[COLS].to_string(index=False))

# ── 10. Municípios-chave ──────────────────────────────────────────────────────
CHAVE = ["João Pessoa", "Campina Grande", "Sousa", "Patos",
         "Cajazeiras", "Santa Rita", "Bayeux", "Queimadas"]
mask = df["ente"].apply(lambda x: any(c.lower() in str(x).lower() for c in CHAVE))
COLS_EXT = COLS + ["contrib_eorcam", "contrib_rrestos",
                   "contrib_qsiconfi", "contrib_ccauc",
                   "contrib_scaixa", "contrib_autonomia"]
print("\n🔎 Municípios-chave:")
print(df[mask][COLS_EXT].to_string(index=False))

# ── 11. Exportação ────────────────────────────────────────────────────────────
OUT_COLS = [
    "cod_ibge", "ente", "populacao", "score", "classificacao",
    "eorcam_raw",    "rrestos_raw",    "qsiconfi",    "ccauc",
    "scaixa_medio",  "autonomia_media",
    "eorcam_norm",   "rrestos_norm",   "scaixa_norm", "autonomia_norm",
    "contrib_eorcam", "contrib_rrestos", "contrib_qsiconfi", "contrib_ccauc",
    "contrib_scaixa", "contrib_autonomia",
    "dado_suspeito",
]
df_out = df[OUT_COLS].sort_values("score", ascending=False, na_position="last")
df_out.to_csv(OUTPUTS   / "score_municipios_pb.csv", index=False, encoding="utf-8-sig")
df_out.to_csv(PROCESSED / "score_municipios_pb.csv", index=False, encoding="utf-8-sig")

print(f"\n✅ Score calculado : {df_out['score'].notna().sum()} municípios")
print(f"   Fase 1          : {PESO_DISPONIVEL} pts — SICONFI + CAUC + DCA")
print(f"   Salvo em        : data/outputs/score_municipios_pb.csv")
print("=" * 65)