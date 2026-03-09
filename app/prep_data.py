#!/usr/bin/env python3
"""
prep_data.py
Baixa o GeoJSON dos municípios da PB e faz merge com score_municipios_pb_pncp.csv.
Gera app/data/pb_score.geojson — arquivo consumido pelo dashboard Streamlit.

Deve rodar sempre que o pncp_agregador for reprocessado (etapa 'app' do pipeline).

Uso direto:
    python app/prep_data.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pandas as pd
import geopandas as gpd
import requests
from utils.paths import OUTPUTS

BASE = Path(__file__).resolve().parent
CSV  = OUTPUTS / "score_municipios_pb_pncp.csv"
OUT  = BASE / "data" / "pb_score.geojson"

GEOJSON_URL = (
    "https://raw.githubusercontent.com/tbrugz/geodata-br/"
    "master/geojson/geojs-25-mun.json"
)


def run() -> None:
    """
    Baixa o GeoJSON da PB, faz merge com o score enriquecido e
    salva app/data/pb_score.geojson para uso no dashboard.

    Depende de:
        data/outputs/score_municipios_pb_pncp.csv  (pncp_agregador.py)
    """
    if not CSV.exists():
        raise FileNotFoundError(
            f"Score PNCP não encontrado: {CSV}\n"
            "Execute primeiro a etapa 'score' do pipeline "
            "(solvency.py + pncp_agregador.py)."
        )

    print("Baixando GeoJSON da Paraíba...")
    r = requests.get(GEOJSON_URL, timeout=30)
    r.raise_for_status()

    geo = gpd.read_file(r.text, driver="GeoJSON")
    print(f"  {len(geo)} polígonos carregados")

    # ── Normalizar cod_ibge ───────────────────────────────────────────────────
    geo["cod_ibge"] = geo["id"].astype(str).str[:7].astype(int)

    # ── Carregar CSV enriquecido ──────────────────────────────────────────────
    df = pd.read_csv(CSV)
    df["cod_ibge"] = df["cod_ibge"].astype(int)
    print(f"  {len(df)} municípios no CSV | {len(df.columns)} colunas")

    # ── Merge ─────────────────────────────────────────────────────────────────
    merged = geo.merge(df, on="cod_ibge", how="left")
    print(f"  {merged['score'].notna().sum()} municípios com score após merge")
    print(f"  {merged['score'].isna().sum()} sem score")

    # ── Campos de display ─────────────────────────────────────────────────────
    merged["classificacao"] = merged["classificacao"].fillna("⚫ Sem Dados")

    merged["score_display"] = merged["score"].apply(
        lambda x: f"{x:.1f}" if pd.notna(x) else "—"
    )

    def fmt_valor(v):
        if pd.isna(v):             return "—"
        if v >= 1_000_000_000:     return f"R$ {v/1_000_000_000:.1f} bi"
        if v >= 1_000_000:         return f"R$ {v/1_000_000:.1f} mi"
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

    # ── Alerta composto ───────────────────────────────────────────────────────
    alerta_dispensa = merged["alerta_dispensa"].fillna(False).infer_objects(copy=False)
    lliq_neg        = (merged["lliq_raw"].fillna(0) < 0)
    dado_suspeito   = merged["dado_suspeito"].fillna(False).infer_objects(copy=False)

    merged["alerta_composto"] = alerta_dispensa | lliq_neg | dado_suspeito

    # ── Exportar ──────────────────────────────────────────────────────────────
    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_file(OUT, driver="GeoJSON")

    colunas_finais = [c for c in merged.columns if c != "geometry"]
    print(f"\n✅ GeoJSON salvo em {OUT}")
    print(f"   {len(colunas_finais)} colunas | {len(merged)} municípios")


if __name__ == "__main__":
    run()