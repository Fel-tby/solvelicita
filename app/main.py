#!/usr/bin/env python3
"""
main.py — SolveLicita Dashboard
Mapa coroplético interativo dos municípios da Paraíba.

Uso:
    streamlit run app/main.py
"""

import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from folium.features import GeoJsonTooltip, GeoJsonPopup
from streamlit_folium import st_folium
from pathlib import Path
import numpy as np

# ── Configuração da página ───────────────────────────────────────────────────
st.set_page_config(
    page_title="SolveLicita — Paraíba",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE = Path(__file__).resolve().parent

# ── Paleta de cores por classificação ───────────────────────────────────────
CORES = {
    "🟢 Risco Baixo": "#22c55e",
    "🟡 Risco Médio": "#eab308",
    "🔴 Risco Alto":  "#ef4444",
    "⛔ Crítico":     "#7f1d1d",
    "⚫ Sem Dados":   "#6b7280",
}

ORDEM = ["🟢 Risco Baixo", "🟡 Risco Médio", "🔴 Risco Alto", "⛔ Crítico", "⚫ Sem Dados"]

def cor_por_score(score):
    if pd.isna(score):
        return CORES["⚫ Sem Dados"]
    if score >= 75:
        return CORES["🟢 Risco Baixo"]
    if score >= 55:
        return CORES["🟡 Risco Médio"]
    if score >= 35:
        return CORES["🔴 Risco Alto"]
    return CORES["⛔ Crítico"]

# ── Carregamento de dados ────────────────────────────────────────────────────
@st.cache_data
def carregar_dados():
    geo_path = BASE / "data" / "pb_score.geojson"
    if not geo_path.exists():
        st.error("❌ Arquivo pb_score.geojson não encontrado. Execute primeiro:\n\n"
                 "    python app/preparar_dados.py")
        st.stop()
    gdf = gpd.read_file(geo_path)
    gdf["score"] = pd.to_numeric(gdf["score"], errors="coerce")
    gdf["cor"] = gdf["score"].apply(cor_por_score)
    gdf["eorcam_raw"]  = pd.to_numeric(gdf.get("eorcam_raw"),  errors="coerce")
    gdf["rrestos_raw"] = pd.to_numeric(gdf.get("rrestos_raw"), errors="coerce")
    gdf["scaixa_medio"] = pd.to_numeric(gdf.get("scaixa_medio"), errors="coerce")
    gdf["autonomia_media"] = pd.to_numeric(gdf.get("autonomia_media"), errors="coerce")
    return gdf

gdf = carregar_dados()

# ── Sidebar — filtros ────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/color/96/brazil.png", width=60)
    st.title("SolveLicita")
    st.caption("Score de Solvência Municipal · Paraíba · Fase 0")
    st.divider()

    st.subheader("Filtros")
    classes_selecionadas = st.multiselect(
        "Classificação de risco",
        options=ORDEM,
        default=ORDEM,
    )

    score_min, score_max = st.slider(
        "Faixa de score",
        min_value=0, max_value=100,
        value=(0, 100), step=1,
    )

    busca = st.text_input("🔍 Buscar município", placeholder="Ex: Campina Grande")
    st.divider()

    st.subheader("Legenda")
    for classe, cor in CORES.items():
        st.markdown(
            f'<span style="background:{cor};border-radius:4px;'
            f'padding:2px 10px;color:white;font-size:12px">{classe}</span>',
            unsafe_allow_html=True
        )
        st.markdown("")

    st.divider()
    st.caption("Dados: SICONFI · CAUC · Tesouro Nacional")
    st.caption("Fase 0 — máximo atingível: 100 pts")

# ── Filtrar dados ────────────────────────────────────────────────────────────
gdf_filtrado = gdf[gdf["classificacao"].isin(classes_selecionadas)].copy()

# filtro de score (ignora Sem Dados no slider)
mask_score = (
    gdf_filtrado["score"].isna() |
    ((gdf_filtrado["score"] >= score_min) & (gdf_filtrado["score"] <= score_max))
)
gdf_filtrado = gdf_filtrado[mask_score]

if busca.strip():
    gdf_filtrado = gdf_filtrado[
        gdf_filtrado["ente"].str.contains(busca.strip(), case=False, na=False)
    ]

# ── Métricas no topo ─────────────────────────────────────────────────────────
st.markdown("## 🗺️ Mapa de Saúde Fiscal — Paraíba 2024")

col1, col2, col3, col4, col5 = st.columns(5)

com_score = gdf["score"].dropna()
col1.metric("Score médio PB",    f"{com_score.mean():.1f}")
col2.metric("Score mediano",      f"{com_score.median():.1f}")
col3.metric("🟢 Risco Baixo",    str((gdf["classificacao"] == "🟢 Risco Baixo").sum()))
col4.metric("🔴 Alto + ⛔ Crítico",
            str(((gdf["classificacao"] == "🔴 Risco Alto") |
                 (gdf["classificacao"] == "⛔ Crítico")).sum()))
col5.metric("⚫ Sem Dados",       str((gdf["classificacao"] == "⚫ Sem Dados").sum()))

st.divider()

# ── Layout: mapa + painel lateral ────────────────────────────────────────────
col_mapa, col_painel = st.columns([2, 1])

with col_mapa:
    # Construir mapa Folium
    m = folium.Map(
        location=[-7.1, -36.8],
        zoom_start=7,
        tiles="CartoDB dark_matter",
        prefer_canvas=True,
    )

    def estilo(feature):
        score = feature["properties"].get("score")
        try:
            score = float(score)
        except (TypeError, ValueError):
            score = None
        return {
            "fillColor": cor_por_score(score),
            "color": "#1a1a2e",
            "weight": 0.6,
            "fillOpacity": 0.8,
        }

    def hover(feature):
        return {
            "fillOpacity": 1.0,
            "weight": 2,
            "color": "#ffffff",
        }

    tooltip = GeoJsonTooltip(
        fields=["ente", "score_display", "classificacao", "populacao"],
        aliases=["Município", "Score", "Classificação", "População"],
        style=(
            "background-color:#1e293b;color:white;"
            "font-family:monospace;font-size:12px;"
            "border-radius:6px;padding:8px;"
        ),
        sticky=True,
    )

    popup = GeoJsonPopup(
        fields=[
            "ente", "score_display", "classificacao", "populacao",
            "eorcam_raw", "rrestos_raw", "qsiconfi", "ccauc",
        ],
        aliases=[
            "Município", "Score", "Risco", "População",
            "Exec. Orçam. (%)", "Restos a Pagar (%)", "SICONFI", "CAUC",
        ],
        style=(
            "background-color:#1e293b;color:white;"
            "font-family:monospace;font-size:12px;"
            "border-radius:8px;padding:10px;"
        ),
        max_width=280,
    )

    folium.GeoJson(
        gdf_filtrado.__geo_interface__,
        style_function=estilo,
        highlight_function=hover,
        tooltip=tooltip,
        popup=popup,
        name="Municípios PB",
    ).add_to(m)

    # Renderizar
    resultado = st_folium(m, width="100%", height=600, returned_objects=[])

with col_painel:
    st.subheader("📊 Distribuição")

    dist = gdf["classificacao"].value_counts().reindex(ORDEM, fill_value=0)
    total = len(gdf)

    for classe in ORDEM:
        n = dist.get(classe, 0)
        pct = n / total * 100
        cor = CORES[classe]
        st.markdown(
            f"""
            <div style="margin-bottom:8px">
                <div style="display:flex;justify-content:space-between;font-size:13px">
                    <span>{classe}</span>
                    <span><b>{n}</b> ({pct:.0f}%)</span>
                </div>
                <div style="background:#374151;border-radius:4px;height:8px">
                    <div style="background:{cor};width:{pct}%;height:8px;border-radius:4px"></div>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.divider()
    st.subheader("🏆 Top 5 — Melhores")
    top5 = gdf.nlargest(5, "score")[["ente", "score", "classificacao"]].dropna(subset=["score"])
    for _, r in top5.iterrows():
        st.markdown(f"**{r['ente']}** — {r['score']:.1f} pts")

    st.divider()
    st.subheader("⚠️ Top 5 — Piores")
    bot5 = gdf[gdf["score"].notna()].nsmallest(5, "score")[["ente", "score", "classificacao"]]
    for _, r in bot5.iterrows():
        st.markdown(f"**{r['ente']}** — {r['score']:.1f} pts")

# ── Tabela completa ──────────────────────────────────────────────────────────
st.divider()
with st.expander("📋 Ver todos os municípios", expanded=False):
    colunas_tabela = ["ente", "populacao", "score", "classificacao",
                      "eorcam_raw", "rrestos_raw", "qsiconfi", "ccauc"]
    colunas_ok = [c for c in colunas_tabela if c in gdf_filtrado.columns]
    df_tabela = gdf_filtrado[colunas_ok].sort_values("score", ascending=False, na_position="last")
    df_tabela.columns = ["Município", "População", "Score", "Classificação",
                         "Exec. Orç. (%)", "Restos (%)", "SICONFI", "CAUC"][:len(colunas_ok)]
    st.dataframe(df_tabela, use_container_width=True, hide_index=True)
