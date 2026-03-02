#!/usr/bin/env python3
"""
prep_data.py
Baixa o GeoJSON dos municípios da PB e faz merge com score_municipios_pb_pncp.csv.
Execute UMA vez antes de rodar o app, e sempre que o pncp_aggregator for reprocessado.

Uso:
    python app/prep_data.py
"""

import pandas as pd
import geopandas as gpd
import requests
from pathlib import Path

BASE = Path(__file__).resolve().parent
CSV  = BASE.parent / "data" / "outputs" / "score_municipios_pb_pncp.csv"
OUT  = BASE / "data" / "pb_score.geojson"

GEOJSON_URL = (
    "https://raw.githubusercontent.com/tbrugz/geodata-br/"
    "master/geojson/geojs-25-mun.json"
)

print("Baixando GeoJSON da Paraíba...")
r = requests.get(GEOJSON_URL, timeout=30)
r.raise_for_status()

geo = gpd.read_file(r.text, driver="GeoJSON")
print(f"  {len(geo)} polígonos carregados")

# ── Normalizar cod_ibge ───────────────────────────────────────────────────────
geo["cod_ibge"] = geo["id"].astype(str).str[:7].astype(int)

# ── Carregar CSV enriquecido ──────────────────────────────────────────────────
df = pd.read_csv(CSV)
df["cod_ibge"] = df["cod_ibge"].astype(int)
print(f"  {len(df)} municípios no CSV | {len(df.columns)} colunas")

# ── Merge ─────────────────────────────────────────────────────────────────────
merged = geo.merge(df, on="cod_ibge", how="left")
print(f"  {merged['score'].notna().sum()} municípios com score após merge")
print(f"  {merged['score'].isna().sum()} sem score")

# ── Campos de display ─────────────────────────────────────────────────────────
merged["classificacao"] = merged["classificacao"].fillna("⚫ Sem Dados")

merged["score_display"] = merged["score"].apply(
    lambda x: f"{x:.1f}" if pd.notna(x) else "—"
)

def fmt_valor(v):
    if pd.isna(v):
        return "—"
    if v >= 1_000_000_000:
        return f"R$ {v/1_000_000_000:.1f} bi"
    if v >= 1_000_000:
        return f"R$ {v/1_000_000:.1f} mi"
    return f"R$ {v:,.0f}"

merged["valor_homologado_display"] = merged["valor_homologado_total"].apply(fmt_valor)

merged["n_licitacoes_display"] = merged["n_licitacoes"].apply(
    lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "—"
)

merged["pct_dispensa_display"] = merged["pct_dispensa"].apply(
    lambda x: f"{x*100:.1f}%" if pd.notna(x) else "—"
)

merged["ano_ultima_licitacao_display"] = merged["ano_ultima_licitacao"].apply(
    lambda x: str(int(x)) if pd.notna(x) else "—"
)

# Fix FutureWarning: infer_objects antes do OR booleano
alerta_dispensa = merged["alerta_dispensa"].fillna(False).infer_objects(copy=False)
scaixa_neg      = (merged["scaixa_medio"].fillna(0) < 0)
dado_suspeito   = merged["dado_suspeito"].fillna(False).infer_objects(copy=False)

merged["alerta_composto"] = alerta_dispensa | scaixa_neg | dado_suspeito

# ── Exportar ──────────────────────────────────────────────────────────────────
OUT.parent.mkdir(parents=True, exist_ok=True)
merged.to_file(OUT, driver="GeoJSON")

colunas_finais = [c for c in merged.columns if c != "geometry"]
print(f"\n✅ Salvo em {OUT}")
print(f"   {len(colunas_finais)} colunas | {len(merged)} municípios")
