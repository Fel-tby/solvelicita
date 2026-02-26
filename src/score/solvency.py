"""
Motor de cálculo do Score de Solvência (0–100) para municípios da Paraíba.
Fase 0 — score parcial com 80% do peso (DataJud, TCU e CEIS/CNEP pendentes).
Metodologia completa em METODOLOGIA.md.

v3 — adota limiares fixos (credit rating) em vez de normalização relativa Min-Max.
Abordagem conservadora: penaliza ativamente gestão abaixo do padrão mínimo aceitável.
CAUC com gatilho punitivo: qualquer pendência grave zera a contribuição do indicador.
"""

import pandas as pd
from pathlib import Path

# ── Configurações de Diretórios ───────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent.parent
PROCESSED = BASE_DIR / "data" / "processed"
OUTPUTS   = BASE_DIR / "data" / "outputs"
OUTPUTS.mkdir(parents=True, exist_ok=True)

# ── Pesos da fórmula (METODOLOGIA.md v2.0) ────────────────────────────────────
PESOS = {
    "eorcam":   15,  # reduzido: fotografia anual, não comportamento
    "rrestos":  25,  # aumentado: calote herdado é o melhor preditor
    "qsiconfi": 15,
    "ccauc":    20,
    # "jdatajud": 10,
    # "atcu":      7,
    # "sceis":     3,
}
PESO_DISPONIVEL = sum(PESOS.values())  # 80 de 100

# Anos fiscais completos para Qsiconfi (2025 excluído — exercício ainda incompleto)
ANOS_REF = [2020, 2021, 2022, 2023, 2024]

# ── Classificação de pendências CAUC por gravidade ────────────────────────────
# Graves: bloqueio direto de repasse federal, dívida com a União ou previdência
# Se QUALQUER uma estiver presente → ccauc = 1.0 → contribuição cai a zero
PENDENCIAS_GRAVES = {
    "Regularidade Previdenciária (RPPS)",
    "Regularidade Fiscal (RFB)",
    "Regularidade PGFN",
    "CADIN",
    "SISTN (Dívida Consolidada)",
    "LRF - Limite Pessoal Executivo",
    "SICONV/TRANSFEREGOV Débitos",
    "Adimplência TCU",
    "Adimplência CGU",
}

# Moderadas: obrigações setoriais — penalidade parcial (máx. 0.5 sem grave)
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
print("  Score de Solvência — SolveLicita")
print(f"  Fase 0 — score parcial ({PESO_DISPONIVEL}% do peso total)")
print("  Modo: limiares conservadores (credit rating)")
print("=" * 65)

# ── 1. Carga dos dados ────────────────────────────────────────────────────────
print("\n📂 Carregando dados...")
df_si = pd.read_csv(PROCESSED / "siconfi_indicadores_pb.csv")
df_ca = pd.read_csv(PROCESSED / "cauc_situacao_pb.csv")
df_mu = pd.read_csv(PROCESSED / "municipios_pb_tabela.csv")

df_si["cod_ibge"]      = df_si["cod_ibge"].astype(str)
df_ca["cod_ibge"]      = df_ca["cod_ibge"].astype(str)
df_mu["cod_ibge"]      = df_mu["cod_ibge"].astype(str)
df_si["entregou_rreo"] = df_si["entregou_rreo"].astype(str).str.lower() == "true"

print(f"  SICONFI:  {df_si['cod_ibge'].nunique()} municípios × {df_si['ano'].nunique()} anos")
print(f"  CAUC:     {len(df_ca)} municípios")
print(f"  Tabela:   {len(df_mu)} municípios")

# ── Funções de pontuação por limiares fixos ───────────────────────────────────
# Todas retornam BONDADE: 1.0 = melhor, 0.0 = pior

def pontuar_eorcam(x: float) -> float:
    """
    Pontuação por limiar fixo de execução orçamentária.
    ≥90% e ≤105% → 1.0: zona saudável (execução precisa).
    105–120% → decaimento linear: excesso por emenda esporádica, não mérito.
    >120% → 0.5: teto — arrecadação anômala não garante solvência contínua.
    70–90% → proporcional: zona de atenção.
    ≤70% → 0.0: colapso de arrecadação ou orçamento fictício.
    """
    if pd.isna(x): return None
    if 90 <= x <= 105: return 1.0
    if x > 120:        return 0.5
    if x > 105:        return round(1.0 - (x - 105) / 30, 4)  # decai de 1.0 até 0.5
    if x >= 70:        return round((x - 70) / 20, 4)
    return 0.0


def pontuar_rrestos(x: float) -> float:
    """
    Threshold calibrado para realidade dos municípios brasileiros:
    0%     → 1.0: sem dívida herdada
    0–3%   → decaimento linear suave: faixa aceitável
    3–10%  → decaimento quadrático agressivo: zona de risco
    ≥10%   → 0.0: dívida crítica (Patos: 9.78% → quase zero)
    """
    if pd.isna(x): return None
    if x <= 0:    return 1.0
    if x >= 10:   return 0.0
    if x <= 3:    return round(1.0 - (x / 3) * 0.3, 4)   # perde no máx 30% até 3%
    return round(0.7 * (1 - (x - 3) / 7) ** 2, 4)         # decai rápido de 3% a 10%


def pontuar_ccauc(pendencias_str: str) -> float:
    """
    Retorna risco CAUC de 0.0 (regular) a 1.0 (máximo crítico).
    Gatilho punitivo: qualquer pendência GRAVE → 1.0 (contribuição zero).
    Apenas moderadas/leves → penalidade proporcional, teto 0.5.
    """
    if not isinstance(pendencias_str, str) or pendencias_str.strip() == "REGULAR":
        return 0.0

    itens = [p.strip() for p in pendencias_str.split("|")]

    # Gatilho: qualquer pendência grave dispara risco máximo
    if any(item in PENDENCIAS_GRAVES for item in itens):
        return 1.0

    # Apenas moderadas e leves: penalidade limitada
    n_mod  = sum(1 for i in itens if i in PENDENCIAS_MODERADAS)
    n_leve = sum(1 for i in itens if i not in PENDENCIAS_MODERADAS)
    pontos = n_mod * 2 + n_leve * 1
    return round(min(pontos / 20, 0.5), 4)  # teto 0.5 sem pendência grave

# ── 2. Qsiconfi — % de anos de referência com RREO entregue ─────────────────
df_qsi = (
    df_si[df_si["ano"].isin(ANOS_REF)]
    .groupby("cod_ibge")["entregou_rreo"]
    .sum()
    .div(len(ANOS_REF))
    .reset_index()
    .rename(columns={"entregou_rreo": "qsiconfi"})
)

# ── 3. Eorcam e Rrestos — média dos anos entregues no período ────────────────
df_fis = (
    df_si[df_si["ano"].isin(ANOS_REF) & df_si["entregou_rreo"]]
    .groupby("cod_ibge")
    .agg(
        eorcam_raw  = ("eorcam",            "mean"),
        rrestos_raw = ("rrestos_nproc_pct", "mean"),
    )
    .reset_index()
)

# Aplica pontuação por limiares (ambas retornam bondade: 1.0=bom, 0.0=ruim)
df_fis["eorcam_norm"]  = df_fis["eorcam_raw"].apply(pontuar_eorcam)
df_fis["rrestos_norm"] = df_fis["rrestos_raw"].apply(pontuar_rrestos)

# ── 4. CAUC com gatilho punitivo ──────────────────────────────────────────────
df_ca["ccauc"] = df_ca["pendencias"].apply(pontuar_ccauc)

# ── 5. Join na tabela mestra ──────────────────────────────────────────────────
df = df_mu[["cod_ibge", "ente", "populacao"]].copy()
df = df.merge(
    df_fis[["cod_ibge", "eorcam_raw", "rrestos_raw", "eorcam_norm", "rrestos_norm"]],
    on="cod_ibge", how="left"
)
df = df.merge(df_qsi,                       on="cod_ibge", how="left")
df = df.merge(df_ca[["cod_ibge", "ccauc"]], on="cod_ibge", how="left")

df["qsiconfi"] = df["qsiconfi"].fillna(0)
df["ccauc"]    = df["ccauc"].fillna(1.0)  # sem CAUC = pior caso (conservador)

# Rrestos sem dado → mediana do estado (comportamento neutro)
mediana_rrestos_norm = df["rrestos_norm"].median()
df["rrestos_norm"] = df["rrestos_norm"].fillna(mediana_rrestos_norm)

print(f"\n  Join: {len(df)} municípios")
print(f"  Sem dados SICONFI: {df['eorcam_raw'].isna().sum()} (score não calculado)")

# ── 6. Aplicar fórmula ────────────────────────────────────────────────────────
# Atenção: eorcam_norm e rrestos_norm já expressam BONDADE (1=bom).
# ccauc expressa RISCO (0=bom, 1=ruim) → inverte com (1 - ccauc).
df["contrib_eorcam"]   = PESOS["eorcam"]   * df["eorcam_norm"].fillna(0)
df["contrib_rrestos"]  = PESOS["rrestos"]  * df["rrestos_norm"]
df["contrib_qsiconfi"] = PESOS["qsiconfi"] * df["qsiconfi"]
df["contrib_ccauc"]    = PESOS["ccauc"]    * (1 - df["ccauc"])

df["score_bruto"] = (
    df["contrib_eorcam"]  +
    df["contrib_rrestos"] +
    df["contrib_qsiconfi"] +
    df["contrib_ccauc"]
)

# Escala conservadora: divide por 100 (não por 80).
# Os 20% ausentes (DataJud, TCU, CEIS) permanecem como risco não avaliado = zero.
# Isso reflete a realidade: não sabemos se o município é bom nesses critérios.
df["score"] = df["score_bruto"].round(1)
df.loc[df["eorcam_raw"].isna(), "score"] = None

# ── 7. Classificação de risco ─────────────────────────────────────────────────
# Thresholds ajustados para Fase 0 (max atingível = 80 pontos)
def classificar(s):
    if pd.isna(s): return "⚫ Sem Dados"
    if s >= 65:   return "🟢 Risco Baixo"   # excelente nos 4 critérios disponíveis
    if s >= 50:   return "🟡 Risco Médio"
    if s >= 35:   return "🔴 Risco Alto"
    return "⛔ Crítico"

df["classificacao"] = df["score"].apply(classificar)

# ── 8. Diagnóstico geral ──────────────────────────────────────────────────────
print("\n🔍 Distribuição de risco:")
print(df["classificacao"].value_counts().to_string())

stats = df["score"].dropna()
print(f"\n   Score médio:   {stats.mean():.1f}")
print(f"   Score mediano: {stats.median():.1f}")
print(f"   Score mínimo:  {stats.min():.1f}")
print(f"   Score máximo:  {stats.max():.1f}")
print(f"   (máx. atingível Fase 0 = 80 pontos)")

COLS = ["ente", "score", "classificacao", "eorcam_raw", "rrestos_raw", "qsiconfi", "ccauc"]
print("\n🏆 Top 10 — Menor risco:")
print(df.nlargest(10, "score")[COLS].to_string(index=False))

print("\n⚠️  Bottom 10 — Maior risco:")
print(df.nsmallest(10, "score")[COLS].to_string(index=False))

# ── 9. Diagnóstico de municípios-chave ────────────────────────────────────────
CHAVE = ["João Pessoa", "Campina Grande", "Sousa", "Patos",
         "Cajazeiras", "Santa Rita", "Bayeux", "Queimadas"]
print("\n🔎 Municípios-chave:")
mask = df["ente"].apply(lambda x: any(c.lower() in str(x).lower() for c in CHAVE))
COLS_EXT = COLS + ["contrib_eorcam", "contrib_rrestos", "contrib_qsiconfi", "contrib_ccauc"]
print(df[mask][COLS_EXT].to_string(index=False))

# ── 10. Exportação ────────────────────────────────────────────────────────────
OUT_COLS = [
    "cod_ibge", "ente", "populacao", "score", "classificacao",
    "eorcam_raw", "rrestos_raw", "qsiconfi", "ccauc",
    "contrib_eorcam", "contrib_rrestos", "contrib_qsiconfi", "contrib_ccauc",
]
df_out = df[OUT_COLS].sort_values("score", ascending=False, na_position="last")
df_out.to_csv(OUTPUTS   / "score_municipios_pb.csv", index=False, encoding="utf-8-sig")
df_out.to_csv(PROCESSED / "score_municipios_pb.csv", index=False, encoding="utf-8-sig")

print(f"\n✅ Score calculado: {df_out['score'].notna().sum()} municípios")
print(f"   ⚠️  Score parcial — faltam DataJud (10%) + TCU (7%) + CEIS/CNEP (3%)")
print(f"   Nota máxima Fase 0: 80 pontos")
print(f"   Salvo em: data/outputs/score_municipios_pb.csv")
print("=" * 65)
