#!/usr/bin/env python3
"""
main.py — SolveLicita Dashboard
Mapa coroplético interativo dos municípios da Paraíba.
Uso: streamlit run app/main.py
"""

import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from folium.features import GeoJsonTooltip, GeoJsonPopup
from streamlit_folium import st_folium
from pathlib import Path

st.set_page_config(
    page_title="SolveLicita — Análise de Solvência Municipal · PB",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS institucional ─────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Fundo geral */
    .stApp { background-color: #0f1117; }

    /* Cabeçalho institucional */
    .inst-header {
        border-left: 4px solid #475569;
        padding: 6px 0 6px 14px;
        margin-bottom: 4px;
    }
    .inst-header h1 {
        font-size: 1.3rem;
        font-weight: 700;
        color: #e2e8f0;
        margin: 0;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }
    .inst-header p {
        font-size: 0.75rem;
        color: #64748b;
        margin: 2px 0 0 0;
        font-family: monospace;
    }

    /* Bloco de métrica customizado */
    .kpi-block {
        background: #1e2433;
        border: 1px solid #2d3748;
        border-top: 3px solid #475569;
        padding: 12px 16px;
        border-radius: 3px;
    }
    .kpi-label {
        font-size: 0.68rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-family: monospace;
    }
    .kpi-value {
        font-size: 1.6rem;
        font-weight: 700;
        color: #e2e8f0;
        font-family: monospace;
        line-height: 1.2;
    }

    /* Tabela de distribuição */
    .dist-row {
        display: flex;
        align-items: center;
        padding: 5px 0;
        border-bottom: 1px solid #1e2433;
        font-family: monospace;
        font-size: 0.82rem;
    }
    .dist-bar-bg {
        flex: 1;
        background: #1e2433;
        border-radius: 2px;
        height: 6px;
        margin: 0 10px;
    }
    .dist-bar-fill {
        height: 6px;
        border-radius: 2px;
    }
    .dist-count {
        color: #94a3b8;
        min-width: 28px;
        text-align: right;
    }

    /* Seção de painel */
    .panel-section {
        background: #131720;
        border: 1px solid #1e2433;
        border-radius: 3px;
        padding: 14px 16px;
        margin-bottom: 12px;
    }
    .panel-title {
        font-size: 0.68rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #64748b;
        font-family: monospace;
        margin-bottom: 10px;
        padding-bottom: 6px;
        border-bottom: 1px solid #1e2433;
    }

    /* Badges de risco */
    .badge {
        display: inline-block;
        padding: 1px 7px;
        border-radius: 2px;
        font-size: 0.7rem;
        font-family: monospace;
        font-weight: 600;
        letter-spacing: 0.05em;
    }
    .badge-verde  { background:#14532d; color:#4ade80; border:1px solid #166534; }
    .badge-amarelo{ background:#422006; color:#fbbf24; border:1px solid #713f12; }
    .badge-vermelho{background:#450a0a; color:#f87171; border:1px solid #7f1d1d; }
    .badge-critico{ background:#1c0505; color:#ef4444; border:1px solid #450a0a; }
    .badge-nd     { background:#1e2433; color:#64748b; border:1px solid #2d3748; }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #0c0f16;
        border-right: 1px solid #1e2433;
    }
    section[data-testid="stSidebar"] .stMarkdown p {
        font-size: 0.78rem;
    }

    /* Esconde toolbar do dataframe */
    [data-testid="stElementToolbar"] { display: none; }

    /* Rodapé */
    .footer-line {
        font-size: 0.68rem;
        color: #334155;
        font-family: monospace;
        border-top: 1px solid #1e2433;
        padding-top: 10px;
        margin-top: 10px;
    }
</style>
""", unsafe_allow_html=True)

BASE = Path(__file__).resolve().parent

# ── Paleta ────────────────────────────────────────────────────────────────────
CORES = {
    "🟢 Risco Baixo": "#22c55e",
    "🟡 Risco Médio": "#ca8a04",
    "🔴 Risco Alto":  "#ef4444",
    "⛔ Crítico":     "#991b1b",
    "⚫ Sem Dados":   "#374151",
}
ORDEM = ["🟢 Risco Baixo", "🟡 Risco Médio", "🔴 Risco Alto", "⛔ Crítico", "⚫ Sem Dados"]

BADGE = {
    "🟢 Risco Baixo": "badge-verde",
    "🟡 Risco Médio": "badge-amarelo",
    "🔴 Risco Alto":  "badge-vermelho",
    "⛔ Crítico":     "badge-critico",
    "⚫ Sem Dados":   "badge-nd",
}
BADGE_LABEL = {
    "🟢 Risco Baixo": "BAIXO",
    "🟡 Risco Médio": "MÉDIO",
    "🔴 Risco Alto":  "ALTO",
    "⛔ Crítico":     "CRÍTICO",
    "⚫ Sem Dados":   "S/D",
}


def cor_por_score(score):
    if pd.isna(score):  return CORES["⚫ Sem Dados"]
    if score >= 75:     return CORES["🟢 Risco Baixo"]
    if score >= 55:     return CORES["🟡 Risco Médio"]
    if score >= 35:     return CORES["🔴 Risco Alto"]
    return CORES["⛔ Crítico"]


# ── Dados ─────────────────────────────────────────────────────────────────────
@st.cache_data
def carregar_dados():
    geo_path = BASE / "data" / "pb_score.geojson"
    if not geo_path.exists():
        st.error("❌ pb_score.geojson não encontrado. Execute: python app/prep_data.py")
        st.stop()
    gdf = gpd.read_file(geo_path)
    for col in ["score", "eorcam_raw", "rrestos_raw", "qsiconfi",
                "ccauc", "scaixa_medio", "autonomia_media", "populacao"]:
        gdf[col] = pd.to_numeric(gdf.get(col), errors="coerce")
    gdf["cor"] = gdf["score"].apply(cor_por_score)
    return gdf


gdf = carregar_dados()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding:12px 0 8px 0'>
        <div style='font-size:1.1rem;font-weight:700;color:#e2e8f0;
                    letter-spacing:0.06em;font-family:monospace'>SOLVELICITA</div>
        <div style='font-size:0.68rem;color:#475569;font-family:monospace;
                    margin-top:2px'>ANÁLISE DE SOLVÊNCIA MUNICIPAL · PB</div>
    </div>
    """, unsafe_allow_html=True)
    st.divider()

    st.markdown('<p style="font-size:0.68rem;color:#64748b;font-family:monospace;'
                'text-transform:uppercase;letter-spacing:0.08em">Filtros</p>',
                unsafe_allow_html=True)

    classes_selecionadas = st.multiselect(
        "Classificação de risco",
        options=ORDEM, default=ORDEM, label_visibility="collapsed",
    )
    score_min, score_max = st.slider(
        "Faixa de score", min_value=0, max_value=100, value=(0, 100), step=1,
    )
    busca = st.text_input("Buscar município", placeholder="Ex: Campina Grande")

    st.divider()
    st.markdown('<p style="font-size:0.68rem;color:#64748b;font-family:monospace;'
                'text-transform:uppercase;letter-spacing:0.08em">Legenda de Risco</p>',
                unsafe_allow_html=True)
    for classe in ORDEM:
        cor = CORES[classe]
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:8px;'
            f'padding:3px 0;font-family:monospace;font-size:0.78rem;color:#94a3b8">'
            f'<div style="width:10px;height:10px;border-radius:2px;'
            f'background:{cor};flex-shrink:0"></div>{classe}</div>',
            unsafe_allow_html=True,
        )
    st.divider()
    st.markdown(
        '<p style="font-size:0.65rem;color:#334155;font-family:monospace">'
        'Fontes: SICONFI · CAUC/STN · FINBRA/DCA<br>'
        'Período de referência: 2020–2024<br>'
        'Snapshot CAUC: 24/02/2026</p>',
        unsafe_allow_html=True,
    )

# ── Filtros ───────────────────────────────────────────────────────────────────
gdf_f = gdf[gdf["classificacao"].isin(classes_selecionadas)].copy()
mask = (
    gdf_f["score"].isna() |
    ((gdf_f["score"] >= score_min) & (gdf_f["score"] <= score_max))
)
gdf_f = gdf_f[mask]
if busca.strip():
    gdf_f = gdf_f[gdf_f["ente"].str.contains(busca.strip(), case=False, na=False)]

# ── Cabeçalho ─────────────────────────────────────────────────────────────────
st.markdown("""
<div class="inst-header">
    <h1>Capacidade de Pagamento dos Municípios — Paraíba</h1>
    <p>SCORE DE SOLVÊNCIA · 223 MUNICÍPIOS · REFERÊNCIA 2020–2024 · FASE 0</p>
</div>
""", unsafe_allow_html=True)

# ── KPIs ──────────────────────────────────────────────────────────────────────
com_score = gdf["score"].dropna()
n_baixo   = (gdf["classificacao"] == "🟢 Risco Baixo").sum()
n_alto    = ((gdf["classificacao"] == "🔴 Risco Alto") |
             (gdf["classificacao"] == "⛔ Crítico")).sum()
n_nd      = (gdf["classificacao"] == "⚫ Sem Dados").sum()

k1, k2, k3, k4, k5 = st.columns(5)
for col, label, val in [
    (k1, "Score Médio PB",       f"{com_score.mean():.1f}"),
    (k2, "Score Mediano",         f"{com_score.median():.1f}"),
    (k3, "Risco Baixo",          str(n_baixo)),
    (k4, "Alto + Crítico",       str(n_alto)),
    (k5, "Sem Dados SICONFI",    str(n_nd)),
]:
    col.markdown(
        f'<div class="kpi-block">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{val}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

st.markdown("<div style='margin-top:18px'></div>", unsafe_allow_html=True)

# ── Mapa + Painel ─────────────────────────────────────────────────────────────
col_mapa, col_painel = st.columns([3, 2])

with col_mapa:
    m = folium.Map(
        location=[-7.1, -36.8],
        zoom_start=7,
        tiles="CartoDB dark_matter",
        prefer_canvas=True,
    )

    def estilo(feature):
        try:    s = float(feature["properties"].get("score"))
        except: s = None
        return {
            "fillColor":   cor_por_score(s),
            "color":       "#0f1117",
            "weight":      0.5,
            "fillOpacity": 0.85,
        }

    def hover(_):
        return {"fillOpacity": 1.0, "weight": 2, "color": "#94a3b8"}

    tooltip = GeoJsonTooltip(
        fields=["ente", "score_display", "classificacao"],
        aliases=["Município", "Score", "Classificação"],
        style=(
            "background-color:#1e2433;color:#e2e8f0;"
            "font-family:monospace;font-size:11px;"
            "border:1px solid #2d3748;border-radius:3px;padding:8px;"
        ),
        sticky=True,
    )

    popup = GeoJsonPopup(
        fields=[
            "ente", "score_display", "classificacao", "populacao",
            "eorcam_raw", "rrestos_raw", "qsiconfi", "ccauc",
            "scaixa_medio", "autonomia_media",
        ],
        aliases=[
            "Município", "Score", "Risco", "População",
            "Exec. Orçam. (%)", "Restos a Pagar (%)",
            "SICONFI (conformidade)", "CAUC (risco 0→1)",
            "Scaixa — DCA/FINBRA", "Autonomia — DCA/FINBRA",
        ],
        style=(
            "background-color:#1e2433;color:#e2e8f0;"
            "font-family:monospace;font-size:11px;"
            "border:1px solid #2d3748;border-radius:3px;padding:10px;"
        ),
        max_width=300,
    )

    folium.GeoJson(
        gdf_f.__geo_interface__,
        style_function=estilo,
        highlight_function=hover,
        tooltip=tooltip,
        popup=popup,
        name="Municípios PB",
    ).add_to(m)

    st_folium(m, width="100%", height=560, returned_objects=[])

with col_painel:
    # Distribuição
    st.markdown('<div class="panel-section">'
                '<div class="panel-title">Distribuição por Faixa de Risco</div>',
                unsafe_allow_html=True)
    dist  = gdf["classificacao"].value_counts().reindex(ORDEM, fill_value=0)
    total = len(gdf)
    for classe in ORDEM:
        n   = dist.get(classe, 0)
        pct = n / total * 100
        cor = CORES[classe]
        st.markdown(
            f'<div class="dist-row">'
            f'<span style="color:{cor};min-width:90px">{BADGE_LABEL[classe]}</span>'
            f'<div class="dist-bar-bg"><div class="dist-bar-fill" '
            f'style="width:{pct:.0f}%;background:{cor}"></div></div>'
            f'<span class="dist-count">{n}</span>'
            f'<span style="color:#334155;font-size:0.68rem;min-width:38px;text-align:right">'
            f'{pct:.0f}%</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    st.markdown('</div>', unsafe_allow_html=True)

    # Estatísticas das variáveis
    st.markdown('<div class="panel-section">'
                '<div class="panel-title">Indicadores — Mediana Estadual</div>',
                unsafe_allow_html=True)
    stats = {
        "Exec. Orçamentária (%)":  ("eorcam_raw",      "{:.1f}%"),
        "Restos a Pagar (%)":      ("rrestos_raw",     "{:.1f}%"),
        "Conformidade SICONFI":    ("qsiconfi",        "{:.0%}"),
        "Scaixa / Rec. Corrente":  ("scaixa_medio",    "{:.3f}"),
        "Autonomia Tributária":    ("autonomia_media", "{:.3f}"),
    }
    for label, (col, fmt) in stats.items():
        val = gdf[col].median()
        val_str = fmt.format(val) if pd.notna(val) else "—"
        st.markdown(
            f'<div style="display:flex;justify-content:space-between;'
            f'padding:4px 0;border-bottom:1px solid #1e2433;'
            f'font-family:monospace;font-size:0.78rem">'
            f'<span style="color:#64748b">{label}</span>'
            f'<span style="color:#e2e8f0;font-weight:600">{val_str}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    st.markdown('</div>', unsafe_allow_html=True)

# ── Tabela completa ───────────────────────────────────────────────────────────
st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)
st.markdown('<div class="panel-title" style="margin-bottom:8px">'
            'DADOS COMPLETOS — MUNICÍPIOS FILTRADOS</div>',
            unsafe_allow_html=True)

df_tabela = (
    gdf_f[[
        "ente", "score", "classificacao", "populacao",
        "eorcam_raw", "rrestos_raw", "qsiconfi", "ccauc",
        "scaixa_medio", "autonomia_media", "dado_suspeito",
    ]]
    .dropna(subset=["score"])
    .sort_values("score", ascending=False)
    .reset_index(drop=True)
)
df_tabela.index += 1

df_tabela = df_tabela.rename(columns={
    "ente":            "Município",
    "score":           "Score",
    "classificacao":   "Risco",
    "populacao":       "Pop.",
    "eorcam_raw":      "Exec.Orç.%",
    "rrestos_raw":     "Restos.%",
    "qsiconfi":        "SICONFI",
    "ccauc":           "CAUC",
    "scaixa_medio":    "Scaixa",
    "autonomia_media": "Autonomia",
    "dado_suspeito":   "⚑ Suspeito",
})

# Formatar colunas numéricas
for col, fmt in [
    ("Exec.Orç.%", "{:.1f}"),
    ("Restos.%",   "{:.1f}"),
    ("SICONFI",    "{:.0%}"),
    ("CAUC",       "{:.2f}"),
    ("Scaixa",     "{:.3f}"),
    ("Autonomia",  "{:.3f}"),
    ("Score",      "{:.1f}"),
]:
    if col in df_tabela.columns:
        df_tabela[col] = df_tabela[col].apply(
            lambda x: fmt.format(x) if pd.notna(x) else "—"
        )

df_tabela["Pop."] = df_tabela["Pop."].apply(
    lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "—"
)

st.dataframe(
    df_tabela,
    use_container_width=True,
    height=420,
)

# ── Rodapé ────────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="footer-line">'
    'SolveLicita · Dados públicos e abertos · '
    'Fontes: SICONFI/Tesouro Nacional · CAUC/STN · FINBRA/DCA/STN · PNCP · '
    '<a href="https://github.com/Fel-tby/solvelicita/blob/main/METODOLOGIA.md" '
    'style="color:#475569">Metodologia completa</a> · '
    '<a href="https://github.com/Fel-tby/solvelicita" style="color:#475569">GitHub</a>'
    '</div>',
    unsafe_allow_html=True,
)