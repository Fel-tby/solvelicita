#!/usr/bin/env python3
"""
preparar_dados.py
Baixa o GeoJSON dos municípios da PB e faz merge com o score_municipios_pb.csv.
Execute UMA vez antes de rodar o app.

Uso:
    python app/preparar_dados.py
"""

import pandas as pd
import geopandas as gpd
import requests
from pathlib import Path

BASE = Path(__file__).resolve().parent
CSV  = BASE.parent / "data" / "outputs" / "score_municipios_pb.csv"
OUT  = BASE / "data" / "pb_score.geojson"

# ── 1. Baixar GeoJSON ────────────────────────────────────────────────────────
GEOJSON_URL = (
    "https://raw.githubusercontent.com/tbrugz/geodata-br/"
    "master/geojson/geojs-25-mun.json"
)

print("Baixando GeoJSON da Paraíba...")
r = requests.get(GEOJSON_URL, timeout=30)
r.raise_for_status()

geo = gpd.read_file(r.text, driver="GeoJSON")
print(f"  {len(geo)} polígonos carregados")
print(f"  Colunas GeoJSON: {list(geo.columns)}")

# ── 2. Normalizar cod_ibge ───────────────────────────────────────────────────
geo["cod_ibge"] = geo["id"].astype(str).str[:7].astype(int)

# ── 3. Carregar CSV de scores ────────────────────────────────────────────────
df = pd.read_csv(CSV)
df["cod_ibge"] = df["cod_ibge"].astype(int)
print(f"  {len(df)} municípios no CSV de scores")

# ── 4. Merge ─────────────────────────────────────────────────────────────────
merged = geo.merge(df, on="cod_ibge", how="left")
print(f"  {merged['score'].notna().sum()} municípios com score após merge")
print(f"  {merged['score'].isna().sum()} sem score (Sem Dados)")

# ── 5. Preencher campos ausentes ─────────────────────────────────────────────
merged["classificacao"] = merged["classificacao"].fillna("⚫ Sem Dados")
merged["score_display"] = merged["score"].apply(
    lambda x: f"{x:.1f}" if pd.notna(x) else "—"
)

# ── 6. Exportar ──────────────────────────────────────────────────────────────
OUT.parent.mkdir(parents=True, exist_ok=True)  # cria app/data/ se não existir
merged.to_file(OUT, driver="GeoJSON")
print(f"\n✅ Salvo em {OUT}")
print(f"   Colunas: {[c for c in merged.columns if c != 'geometry']}")