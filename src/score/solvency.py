"""
Motor de cálculo do Score de Solvência (0–100) para municípios da Paraíba.
Fase 0 — score com 4 variáveis disponíveis (SICONFI + CAUC).
DataJud, TCU e CEIS/CNEP descartados ou pendentes para Fase 1.
Metodologia completa em METODOLOGIA.md.

v4 — correções aplicadas:
  - Rrestos NaN → mediana estadual do período (não mais zero)
  - Rrestos < 0  → clampado a 0.0 + flag `dado_suspeito = True`
  - CAUC: penalização por GRAVIDADE das pendências, não quantidade
  - Docstring do cabeçalho e comentários alinhados à realidade da Fase 0
"""

import pandas as pd
from pathlib import Path

# ── Configurações de Diretórios ───────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent.parent
PROCESSED = BASE_DIR / "data" / "processed"
OUTPUTS   = BASE_DIR / "data" / "outputs"
OUTPUTS.mkdir(parents=True, exist_ok=True)

# ── Pesos da fórmula (Fase 0 — 4 variáveis, total = 100 pts) ─────────────────
PESOS = {
    "eorcam":   31,   # execução orçamentária média — fluxo de caixa real
    "rrestos":  25,   # restos a pagar não processados — melhor preditor de calote
    "qsiconfi": 19,   # % de anos com RREO entregue — transparência/governança
    "ccauc":    25,   # gravidade das pendências no CAUC — risco fiscal verificado
}
PESO_TOTAL = sum(PESOS.values())  # 100

# Anos fiscais de referência (2025 excluído — exercício ainda incompleto)
ANOS_REF = [2020, 2021, 2022, 2023, 2024]

# ── Classificação de pendências CAUC por gravidade ────────────────────────────
# A penalização é definida pela GRAVIDADE da pendência, não pelo número delas.
# Uma única pendência grave basta para zerar a contribuição do CAUC.

PENDENCIAS_GRAVES = {
    "Regularidade Fiscal (RFB)",        # dívida tributária com a União
    "Regularidade PGFN",                # dívida ativa da União
    "CADIN",                            # devedor da União
    "SISTN (Dívida Consolidada)",       # dívida consolidada com a União
    "LRF - Limite Pessoal Executivo",   # gasto com pessoal acima do limite legal
    "Adimplência TCU",                  # condenação pelo TCU
    "Adimplência CGU",                  # condenação pela CGU
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
print("  Score de Solvência — SolveLicita")
print(f"  Fase 0 — {PESO_TOTAL} pontos possíveis (SICONFI + CAUC)")
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
    ≥90% e ≤105% → 1.0 : zona saudável (execução precisa).
    105–120%       → decaimento linear até 0.5: excesso por emenda esporádica.
    >120%          → 0.5 : teto — arrecadação anômala não garante solvência futura.
    70–90%         → proporcional 0.0→1.0: zona de atenção.
    ≤70%           → 0.0 : colapso de arrecadação ou orçamento fictício.
    """
    if pd.isna(x):
        return None
    if 90 <= x <= 105:
        return 1.0
    if x > 120:
        return 0.5
    if x > 105:
        return round(1.0 - (x - 105) / 30, 4)   # 1.0 → 0.5 entre 105% e 120%
    if x >= 70:
        return round((x - 70) / 20, 4)           # 0.0 → 1.0 entre 70% e 90%
    return 0.0


def pontuar_rrestos(x: float) -> float:
    """
    Pontuação por limiar fixo de restos a pagar não processados.
    0%     → 1.0 : sem dívida herdada.
    0–3%   → decaimento linear suave: faixa aceitável.
    3–10%  → decaimento quadrático agressivo: zona de risco crescente.
    ≥10%   → 0.0 : dívida crítica para o fornecedor.

    Valores negativos (estorno/erro de lançamento no SICONFI) são
    clampados a 0.0 antes da pontuação — ver flag `dado_suspeito`.
    """
    if pd.isna(x):
        return None
    x = max(x, 0.0)   # clamp: rrestos < 0 é impossível, trata como 0
    if x == 0:
        return 1.0
    if x >= 10:
        return 0.0
    if x <= 3:
        return round(1.0 - (x / 3) * 0.3, 4)          # perde no máx 30% até 3%
    return round(0.7 * (1 - (x - 3) / 7) ** 2, 4)     # decai rápido de 3% a 10%


def pontuar_ccauc(pendencias_str: str) -> float:
    """
    Penalização por GRAVIDADE das pendências no CAUC, não por quantidade.

    Lógica:
      - Município REGULAR                  → ccauc = 0.0  (sem penalidade)
      - Qualquer pendência GRAVE presente  → ccauc = 1.0  (contribuição zerada)
      - Apenas pendências MODERADAS/LEVES  → ccauc proporcional, teto = 0.5

    Municípios sem dado no CAUC são tratados como pior caso (ccauc = 1.0)
    na etapa de join — conservadorismo explícito.
    """
    if not isinstance(pendencias_str, str) or pendencias_str.strip().upper() == "REGULAR":
        return 0.0

    itens = [p.strip() for p in pendencias_str.split("|")]

    # Uma pendência grave basta para zerar toda a contribuição do CAUC
    if any(item in PENDENCIAS_GRAVES for item in itens):
        return 1.0

    # Apenas moderadas e leves: penalidade proporcional, teto 0.5
    n_mod  = sum(1 for i in itens if i in PENDENCIAS_MODERADAS)
    n_leve = sum(1 for i in itens if i not in PENDENCIAS_MODERADAS)
    pontos = n_mod * 2 + n_leve * 1
    return round(min(pontos / 20, 0.5), 4)

# ── 2. Qsiconfi — % de anos de referência com RREO entregue ──────────────────
df_qsi = (
    df_si[df_si["ano"].isin(ANOS_REF)]
    .groupby("cod_ibge")["entregou_rreo"]
    .sum()
    .div(len(ANOS_REF))
    .reset_index()
    .rename(columns={"entregou_rreo": "qsiconfi"})
)

# ── 3. Eorcam e Rrestos — média dos anos entregues no período ─────────────────
df_fis = (
    df_si[df_si["ano"].isin(ANOS_REF) & df_si["entregou_rreo"]]
    .groupby("cod_ibge")
    .agg(
        eorcam_raw  = ("eorcam",            "mean"),
        rrestos_raw = ("rrestos_nproc_pct", "mean"),
    )
    .reset_index()
)

# ── FLAG: rrestos negativo — dado suspeito (estorno ou erro de lançamento) ────
df_fis["dado_suspeito"] = df_fis["rrestos_raw"] < 0

# Aplica pontuação por limiares — clamp de negativos ocorre dentro de pontuar_rrestos
df_fis["eorcam_norm"]  = df_fis["eorcam_raw"].apply(pontuar_eorcam)
df_fis["rrestos_norm"] = df_fis["rrestos_raw"].apply(pontuar_rrestos)

# ── 4. CAUC — penalização por gravidade ───────────────────────────────────────
df_ca["ccauc"] = df_ca["pendencias"].apply(pontuar_ccauc)

# ── 5. Join na tabela mestra ──────────────────────────────────────────────────
df = df_mu[["cod_ibge", "ente", "populacao"]].copy()
df = df.merge(
    df_fis[["cod_ibge", "eorcam_raw", "rrestos_raw", "dado_suspeito",
            "eorcam_norm", "rrestos_norm"]],
    on="cod_ibge", how="left"
)
df = df.merge(df_qsi,                       on="cod_ibge", how="left")
df = df.merge(df_ca[["cod_ibge", "ccauc"]], on="cod_ibge", how="left")

df["qsiconfi"]    = df["qsiconfi"].fillna(0)
df["dado_suspeito"] = df["dado_suspeito"].fillna(False)

# CAUC ausente → pior caso conservador (sem dado = risco não calculável)
df["ccauc"] = df["ccauc"].fillna(1.0)

# ── CORREÇÃO: Rrestos NaN → mediana estadual do período ──────────────────────
# A mediana é calculada ANTES de aplicar a pontuação, sobre os valores brutos.
# Municípios sem dado não recebem nem o máximo (injusto) nem o mínimo (punitivo).
mediana_rrestos = df_fis["rrestos_raw"].clip(lower=0).median()
print(f"\n  ℹ️  Mediana estadual rrestos (usada p/ NaN): {mediana_rrestos:.4f}%")

# Aplica mediana nos municípios sem rrestos_norm calculado
mask_nan_rrestos = df["rrestos_norm"].isna() & df["eorcam_raw"].notna()
df.loc[mask_nan_rrestos, "rrestos_norm"] = pontuar_rrestos(mediana_rrestos)
df.loc[mask_nan_rrestos, "rrestos_raw"]  = mediana_rrestos

n_mediana = mask_nan_rrestos.sum()
if n_mediana > 0:
    print(f"  ℹ️  {n_mediana} município(s) com rrestos ausente — mediana aplicada:")
    print(df.loc[mask_nan_rrestos, ["ente", "rrestos_raw"]].to_string(index=False))

# ── 6. Aplicar fórmula ────────────────────────────────────────────────────────
# eorcam_norm e rrestos_norm expressam BONDADE (1=bom).
# ccauc expressa RISCO (0=bom, 1=ruim) → invertido com (1 - ccauc).
df["contrib_eorcam"]   = PESOS["eorcam"]   * df["eorcam_norm"].fillna(0)
df["contrib_rrestos"]  = PESOS["rrestos"]  * df["rrestos_norm"]
df["contrib_qsiconfi"] = PESOS["qsiconfi"] * df["qsiconfi"]
df["contrib_ccauc"]    = PESOS["ccauc"]    * (1 - df["ccauc"])

df["score"] = (
    df["contrib_eorcam"]  +
    df["contrib_rrestos"] +
    df["contrib_qsiconfi"] +
    df["contrib_ccauc"]
).round(1)

# Municípios sem SICONFI → score não calculável
df.loc[df["eorcam_raw"].isna(), "score"] = None

# ── 7. Classificação de risco ─────────────────────────────────────────────────
def classificar(s):
    if pd.isna(s):  return "⚫ Sem Dados"
    if s >= 75:     return "🟢 Risco Baixo"
    if s >= 55:     return "🟡 Risco Médio"
    if s >= 35:     return "🔴 Risco Alto"
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

COLS = ["ente", "score", "classificacao", "eorcam_raw", "rrestos_raw",
        "qsiconfi", "ccauc", "dado_suspeito"]
print("\n🏆 Top 10 — Menor risco:")
print(df.nlargest(10, "score")[COLS].to_string(index=False))

print("\n⚠️  Bottom 10 — Maior risco:")
print(df.nsmallest(10, "score")[COLS].to_string(index=False))

CHAVE = ["João Pessoa", "Campina Grande", "Sousa", "Patos",
         "Cajazeiras", "Santa Rita", "Bayeux", "Queimadas"]
print("\n🔎 Municípios-chave:")
mask = df["ente"].apply(lambda x: any(c.lower() in str(x).lower() for c in CHAVE))
COLS_EXT = COLS + ["contrib_eorcam", "contrib_rrestos", "contrib_qsiconfi", "contrib_ccauc"]
print(df[mask][COLS_EXT].to_string(index=False))

# ── 9. Alerta de dados suspeitos ──────────────────────────────────────────────
suspeitos = df[df["dado_suspeito"] == True]
if not suspeitos.empty:
    print("\n⚠️  DADOS SUSPEITOS (rrestos_raw < 0 — possível estorno ou erro SICONFI):")
    print(suspeitos[["ente", "rrestos_raw", "score", "classificacao"]].to_string(index=False))
    print("   → Rrestos clampado a 0.0 para cálculo. Verificar manualmente.")

# ── 10. Exportação ────────────────────────────────────────────────────────────
EXPORT_COLS = [
    "cod_ibge", "ente", "populacao",
    "score", "classificacao",
    "eorcam_raw", "rrestos_raw", "qsiconfi", "ccauc",
    "dado_suspeito",
    "contrib_eorcam", "contrib_rrestos", "contrib_qsiconfi", "contrib_ccauc",
]
out_path = OUTPUTS / "score_municipios_pb.csv"
df[EXPORT_COLS].sort_values("score", ascending=False, na_position="last") \
    .to_csv(out_path, index=False)

print(f"\n✅ Exportado: {out_path}")
print(f"   {len(df)} municípios | {df['score'].notna().sum()} com score calculado")