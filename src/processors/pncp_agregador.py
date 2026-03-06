"""
pncp_agregador.py — Enriquece o score de solvência com dados de mercado do PNCP.
SolveLicita

Responsabilidades:
1. Ler pncp_licitacoes_pb.csv
2. Agregar por município: volume, valor homologado, padrão de compra, temporalidade
3. Fazer merge com score_municipios_pb.csv (output do solvency.py)
4. Exportar score_municipios_pb_pncp.csv para uso no app e no relatório

Não recalcula scores — apenas enriquece o output do solvency.py com
a dimensão de "exposição de mercado" (quanto o município compra e como).

Uso:
    python src/processors/pncp_agregador.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
from utils.paths import PROCESSED, OUTPUTS

# Modalidades que dispensam competição (IDs fixos da Lei 14.133/2021)
MODALIDADES_SEM_LICITACAO = {8, 9}  # 8 = Dispensa, 9 = Inexigibilidade

print("=" * 65)
print(" PNCP Aggregator — SolveLicita")
print(" Enriquecimento do score com dados de mercado")
print("=" * 65)


# ── 1. Carga ──────────────────────────────────────────────────────────────────
print("\n📂 Carregando dados...")

pncp  = pd.read_csv(PROCESSED / "pncp_licitacoes_pb.csv",
                    dtype={"municipio_ibge": str})
score = pd.read_csv(OUTPUTS / "score_municipios_pb.csv",
                    dtype={"cod_ibge": str})

# Normalizar cod_ibge para 7 dígitos nos dois lados
pncp["municipio_ibge"] = pncp["municipio_ibge"].str.zfill(7)
score["cod_ibge"]      = score["cod_ibge"].str.zfill(7)

print(f"  PNCP  : {len(pncp):,} licitações | "
      f"{pncp['municipio_ibge'].nunique()} municípios")
print(f"  Score : {len(score)} municípios")


# ── 2. Flags auxiliares ───────────────────────────────────────────────────────
pncp["eh_dispensa"] = pncp["modalidadeId"].isin(MODALIDADES_SEM_LICITACAO)

pncp["valor_hom_dispensa"] = pncp["valorTotalHomologado"].where(
    pncp["eh_dispensa"], 0
).fillna(0)

pncp["valorTotalHomologado"] = pncp["valorTotalHomologado"].fillna(0)


# ── 3. Agregação por município ────────────────────────────────────────────────
print("\n🔄 Agregando por município...")

agg = pncp.groupby("municipio_ibge").agg(
    n_licitacoes          = ("numeroControlePNCP",   "count"),
    valor_homologado_total = ("valorTotalHomologado", "sum"),
    n_dispensa            = ("eh_dispensa",           "sum"),
    valor_hom_dispensa    = ("valor_hom_dispensa",    "sum"),
    ano_ultima_licitacao  = ("anoCompra",             "max"),
).reset_index().rename(columns={"municipio_ibge": "cod_ibge"})

# Percentual do valor homologado via dispensa/inexigibilidade
agg["pct_dispensa"] = (
    agg["valor_hom_dispensa"] /
    agg["valor_homologado_total"].replace(0, np.nan)
).round(4)

# Flag diagnóstica: alto volume de dispensa (não entra no score)
agg["alerta_dispensa"] = (agg["pct_dispensa"] > 0.30)

print(f"  Municípios com dados PNCP : {len(agg)}")
print(f"  Sem dados PNCP            : {len(score) - len(agg)}")
print(f"  Total licitações          : {agg['n_licitacoes'].sum():,.0f}")
print(f"  Valor homologado total    : "
      f"R$ {agg['valor_homologado_total'].sum():,.0f}")


# ── 4. Merge com score ────────────────────────────────────────────────────────
merged = score.merge(agg, on="cod_ibge", how="left")


# ── 5. Diagnóstico de cruzamentos para o relatório ───────────────────────────
print("\n📊 Cruzamentos para o relatório:")

# A — Mercado seguro (Risco Baixo)
risco_baixo = merged[merged["classificacao"] == "🟢 Risco Baixo"]
print(f"\n  🟢 Risco Baixo ({len(risco_baixo)} municípios):")
print(f"     Valor homologado : "
      f"R$ {risco_baixo['valor_homologado_total'].sum():,.0f}")
print(f"     Licitações       : "
      f"{risco_baixo['n_licitacoes'].sum():,.0f}")

# B — Exposição em municípios com liquidez negativa
# (lliq_raw < 0 = passivo financeiro maior que ativo — substitui scaixa_medio)
lliq_neg = merged[merged["lliq_raw"].fillna(0) < 0]
print(f"\n  🔴 Lliq negativo ({len(lliq_neg)} municípios):")
print(f"     Valor homologado : "
      f"R$ {lliq_neg['valor_homologado_total'].sum():,.0f}")
print(f"     Licitações       : "
      f"{lliq_neg['n_licitacoes'].sum():,.0f}")

# C — Invisíveis ativos (Sem Dados + com licitações no PNCP)
sem_dados = merged[
    (merged["classificacao"] == "⚫ Sem Dados") &
    (merged["n_licitacoes"].notna())
]
print(f"\n  ⚫ Sem Dados com licitações ({len(sem_dados)} municípios):")
print(f"     Valor homologado : "
      f"R$ {sem_dados['valor_homologado_total'].sum():,.0f}")
print(f"     Licitações       : "
      f"{sem_dados['n_licitacoes'].sum():,.0f}")

# D — Alerta: risco alto/crítico + alto percentual de dispensa
alerta = merged[
    merged["alerta_dispensa"].fillna(False).infer_objects(copy=False) &
    merged["classificacao"].isin(["🔴 Risco Alto", "⛔ Crítico"])
]
print(f"\n  ⚠️  Risco Alto/Crítico + >30% dispensa: {len(alerta)} municípios")
if len(alerta):
    print(f"  {alerta[['ente', 'score', 'pct_dispensa']].to_string(index=False)}")


# ── 6. Exportação ─────────────────────────────────────────────────────────────
OUT = OUTPUTS / "score_municipios_pb_pncp.csv"
merged.to_csv(OUT, index=False, encoding="utf-8-sig")

print(f"\n✅ Exportado : {OUT.name}")
print(f"   {len(merged)} municípios | {len(merged.columns)} colunas")
print("=" * 65)
