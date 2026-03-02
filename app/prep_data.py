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
from streamlit_folium import st_folium
from pathlib import Path

st.set_page_config(
    page_title="SolveLicita — Análise de Solvência Municipal · PB",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  [data-testid="stAppViewContainer"] { background: #0f172a; }
  [data-testid="stSidebar"]          { background: #1e293b; }
  h1, h2, h3, label, p, div         { color: #f1f5f9 !important; }

  .badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 99px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .5px;
  }
  .badge-verde    { background:#166534; color:#bbf7d0; }
  .badge-amarelo  { background:#854d0e; color:#fef08a; }
  .badge-vermelho { background:#991b1b; color:#fecaca; }
  .badge-critico  { background:#4c0519; color:#fda4af; }
  .badge-nd       { background:#1e293b; color:#94a3b8; }

  .metric-row {
    display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 6px;
  }
  .metric-box {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 8px;
    padding: 8px 12px;
    min-width: 90px;
    flex: 1;
  }
  .metric-label { font-size: 10px; color: #94a3b8; text-transform: uppercase; }
  .metric-value { font-size: 18px; font-weight: 700; color: #f1f5f9; }
  .metric-sub   { font-size: 11px; color: #64748b; }

  .section-title {
    font-size: 11px;
    font-weight: 600;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin: 12px 0 4px;
  }
  .alerta-box {
    background: #431407;
    border: 1px solid #c2410c;
    border-radius: 6px;
    padding: 6px 10px;
    font-size: 12px;
    color: #fed7aa;
    margin-top: 8px;
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

BADGE_CSS = {
    "🟢 Risco Baixo": ("badge-verde",    "BAIXO"),
    "🟡 Risco Médio": ("badge-amarelo",  "MÉDIO"),
    "🔴 Risco Alto":  ("badge-vermelho", "ALTO"),
    "⛔ Crítico":     ("badge-critico",  "CRÍTICO"),
    "⚫ Sem Dados":   ("badge-nd",       "S/D"),
}

def cor_por_score(score):
    if pd.isna(score):        return CORES["⚫ Sem Dados"]
    if score >= 75:           return CORES["🟢 Risco Baixo"]
    if score >= 55:           return CORES["🟡 Risco Médio"]
    if score >= 35:           return CORES["🔴 Risco Alto"]
    return CORES["⛔ Crítico"]

# ── Dados ─────────────────────────────────────────────────────────────────────
@st.cache_data
def carregar_dados():
    geo_path = BASE / "data" / "pb_score.geojson"
    if not geo_path.exists():
        st.error("❌ pb_score.geojson não encontrado. Execute: python app/prep_data.py")
        st.stop()

    gdf = gpd.read_file(geo_path)

    # Colunas numéricas — score original
    for col in ["score", "eorcam_raw", "rrestos_raw", "qsiconfi", "ccauc",
                "scaixa_medio", "autonomia_media",
                "contrib_eorcam", "contrib_rrestos", "contrib_qsiconfi",
                "contrib_ccauc", "contrib_scaixa", "contrib_autonomia"]:
        gdf[col] = pd.to_numeric(gdf.get(col), errors="coerce")

    # Colunas numéricas — PNCP
    for col in ["populacao", "n_licitacoes", "valor_homologado_total",
                "pct_dispensa", "ano_ultima_licitacao"]:
        gdf[col] = pd.to_numeric(gdf.get(col), errors="coerce")

    # Booleanos
    for col in ["dado_suspeito", "alerta_dispensa", "alerta_composto"]:
        gdf[col] = gdf.get(col, False).fillna(False).infer_objects(copy=False)

    gdf["cor"] = gdf["score"].apply(cor_por_score)
    return gdf

gdf = carregar_dados()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("<div class='section-title'>Filtros</div>", unsafe_allow_html=True)

    classes_selecionadas = st.multiselect(
        "Classificação de risco",
        options=ORDEM,
        default=ORDEM,
        label_visibility="collapsed",
    )

    score_min, score_max = st.slider(
        "Faixa de score",
        min_value=0, max_value=100,
        value=(0, 100), step=1,
    )

    # Filtro de população — corrigido
    pop_values = gdf["populacao"].dropna().astype(int)
    pop_min_data = int(pop_values.min())
    pop_max_data = int(pop_values.max())

    pop_min, pop_max = st.slider(
        "Faixa de população",
        min_value=pop_min_data,
        max_value=pop_max_data,
        value=(pop_min_data, pop_max_data),
        step=500,
        format="%d hab",
    )

    busca = st.text_input("Buscar município", placeholder="Ex: Campina Grande")

    st.divider()
    st.markdown(
        "<div style='font-size:11px;color:#64748b;line-height:1.7'>"
        "Fontes: SICONFI · CAUC/STN · FINBRA/DCA · PNCP<br>"
        "Período fiscal: 2020–2024<br>"
        "Licitações PNCP: 2023–fev/2026<br>"
        "Snapshot CAUC: 24/02/2026"
        "</div>",
        unsafe_allow_html=True,
    )

# ── Aplicar filtros ───────────────────────────────────────────────────────────
mask = pd.Series([True] * len(gdf), index=gdf.index)

# Classificação
mask &= gdf["classificacao"].isin(classes_selecionadas)

# Score — inclui Sem Dados (score NaN) quando a classe está selecionada
mask_score = (
    (gdf["score"] >= score_min) & (gdf["score"] <= score_max)
) | gdf["score"].isna()
mask &= mask_score

# População — só aplica onde há dado
mask_pop = (
    (gdf["populacao"] >= pop_min) & (gdf["populacao"] <= pop_max)
) | gdf["populacao"].isna()
mask &= mask_pop

# Busca textual
if busca.strip():
    mask &= gdf["ente"].str.contains(busca.strip(), case=False, na=False)

gdf_vis = gdf[mask].copy()

# ── Cabeçalho ─────────────────────────────────────────────────────────────────
col_title, col_stats = st.columns([3, 2])

with col_title:
    st.markdown(
        "<h1 style='font-size:26px;font-weight:800;margin-bottom:0'>📋 SolveLicita</h1>"
        "<p style='font-size:13px;color:#94a3b8;margin-top:2px'>"
        "Score de Solvência Municipal · Paraíba · 223 municípios</p>",
        unsafe_allow_html=True,
    )

with col_stats:
    contagem = gdf_vis["classificacao"].value_counts()
    partes = " &nbsp;·&nbsp; ".join(
        f"<span style='color:{CORES[c]};font-weight:700'>{contagem.get(c,0)}</span> {c}"
        for c in ORDEM if contagem.get(c, 0) > 0
    )
    st.markdown(
        f"<div style='padding-top:14px;font-size:12px'>{partes}</div>",
        unsafe_allow_html=True,
    )

# ── Mapa ──────────────────────────────────────────────────────────────────────
m = folium.Map(
    location=[-7.2, -36.8],
    zoom_start=7,
    tiles="CartoDB dark_matter",
    prefer_canvas=True,
)

def popup_html(row):
    cls   = row.get("classificacao", "⚫ Sem Dados")
    css, label = BADGE_CSS.get(cls, ("badge-nd", "S/D"))
    score = row.get("score_display", "—")
    ente  = row.get("ente", "—")
    pop   = row.get("populacao")
    pop_s = f"{int(pop):,}".replace(",", ".") if pd.notna(pop) else "—"

    # Indicadores fiscais
    eorcam    = row.get("eorcam_raw")
    rrestos   = row.get("rrestos_raw")
    scaixa    = row.get("scaixa_medio")
    autonomia = row.get("autonomia_media")
    cauc_val  = row.get("ccauc")

    eorcam_s    = f"{eorcam:.1f}%"         if pd.notna(eorcam)    else "—"
    rrestos_s   = f"{rrestos*100:.2f}%"    if pd.notna(rrestos)   else "—"
    scaixa_s    = f"{scaixa:.3f}"          if pd.notna(scaixa)    else "—"
    autonomia_s = f"{autonomia*100:.1f}%"  if pd.notna(autonomia) else "—"
    cauc_s      = "⚠️ Pendência" if pd.notna(cauc_val) and cauc_val > 0 else "✅ Regular"

    # PNCP
    val_hom   = row.get("valor_homologado_display", "—")
    n_lic     = row.get("n_licitacoes_display", "—")
    pct_disp  = row.get("pct_dispensa_display", "—")
    ano_ult   = row.get("ano_ultima_licitacao_display", "—")
    alerta    = row.get("alerta_composto", False)

    alerta_html = (
        "<div class='alerta-box'>⚠️ Atenção: Scaixa negativo, dado suspeito "
        "ou alto índice de dispensa detectado.</div>"
        if alerta else ""
    )

    return f"""
    <div style='font-family:sans-serif;min-width:240px;max-width:280px;
                background:#0f172a;border-radius:10px;padding:14px;color:#f1f5f9'>
      <div style='display:flex;justify-content:space-between;align-items:center;
                  margin-bottom:10px'>
        <span style='font-size:15px;font-weight:700'>{ente}</span>
        <span class='badge {css}'>{label}</span>
      </div>

      <div class='metric-row'>
        <div class='metric-box'>
          <div class='metric-label'>Score</div>
          <div class='metric-value'>{score}</div>
          <div class='metric-sub'>/ 100 pts</div>
        </div>
        <div class='metric-box'>
          <div class='metric-label'>População</div>
          <div class='metric-value' style='font-size:14px'>{pop_s}</div>
          <div class='metric-sub'>hab.</div>
        </div>
      </div>

      <div class='section-title'>Indicadores Fiscais</div>
      <div style='font-size:12px;line-height:2;color:#cbd5e1'>
        Execução orçamentária &nbsp;<b>{eorcam_s}</b><br>
        Restos a pagar &nbsp;<b>{rrestos_s}</b><br>
        Saldo de caixa (Scaixa) &nbsp;<b>{scaixa_s}</b><br>
        Autonomia tributária &nbsp;<b>{autonomia_s}</b><br>
        CAUC &nbsp;<b>{cauc_s}</b>
      </div>

      <div class='section-title'>Compras Públicas · PNCP</div>
      <div style='font-size:12px;line-height:2;color:#cbd5e1'>
        Valor homologado &nbsp;<b>{val_hom}</b><br>
        Licitações publicadas &nbsp;<b>{n_lic}</b><br>
        Via dispensa/inexigibilidade &nbsp;<b>{pct_disp}</b><br>
        Última licitação &nbsp;<b>{ano_ult}</b>
      </div>

      {alerta_html}
    </div>
    """

for _, row in gdf_vis.iterrows():
    if row.geometry is None:
        continue
    folium.GeoJson(
        row.geometry.__geo_interface__,
        style_function=lambda _, r=row: {
            "fillColor":   r["cor"],
            "color":       "#1e293b",
            "weight":      0.6,
            "fillOpacity": 0.75,
        },
        highlight_function=lambda _: {
            "weight":      2,
            "color":       "#f1f5f9",
            "fillOpacity": 0.95,
        },
        popup=folium.Popup(popup_html(row), max_width=300),
        tooltip=folium.Tooltip(
            f"{row.get('ente','—')} · {row.get('score_display','—')} pts",
            sticky=False,
        ),
    ).add_to(m)

map_col, _ = st.columns([5, 1])
with map_col:
    st_folium(m, width=None, height=620, returned_objects=[])

# ── Rodapé ────────────────────────────────────────────────────────────────────
st.markdown(
    "<div style='text-align:center;font-size:10px;color:#475569;margin-top:8px'>"
    "SCORE DE SOLVÊNCIA · 223 MUNICÍPIOS · REFERÊNCIA 2020–2024 · FASE 0 · SOLVELICITA"
    "</div>",
    unsafe_allow_html=True,
)
