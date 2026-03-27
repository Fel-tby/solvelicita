"""
backtest_validacao.py — Validação walk-forward do Score de Solvência v7.0
─────────────────────────────────────────────────────────────────────────────
Estratégia: para cada par de anos consecutivos (T0, T1), calcula o score com
dados disponíveis em T0 e usa rproc_pct observado em T1 como variável de
desfecho. Isso replica a situação real de uso do score: prever comportamento
futuro a partir de informação presente.

Dois regimes de dados:
  Era Parcial  (2020→2021, 2021→2022, 2022→2023)
    lliq ausente — RGF Anexo 05 não coletado para esse período.
    Apenas 55% dos pesos ativos (eorcam + qsiconfi + rproc).

  Era Completa (2023→2024, 2024→2025)
    Score pleno. Todos os componentes ativos exceto CAUC e Autonomia
    (ver limitações abaixo). 75% dos pesos ativos.

Limitações conhecidas que afetam os resultados:
  · CAUC sem série histórica: fixado em neutro (0.0) em todos os pares.
    O peso de 10% é redistribuído proporcionalmente entre os ativos.
  · Autonomia sem série histórica no siconfi_indicadores: fixada em 0.5
    (ponto médio da sigmoid). O peso de 10% é redistribuído.
  · RPproc tem circularidade parcial com o desfecho: n_anos_cronicos em T0
    é calculado com base no histórico de rproc_pct, e rproc_t1 é o desfecho.
    Use --sem-rproc para quantificar o quanto o sinal depende dessa
    circularidade.
  · 2020 foi ano de repasses emergenciais COVID (LC 173/2020). Scores
    calculados com T0=2020 podem estar superestimados para municípios de
    risco real. Use --excluir-t0 2020 para isolar esse efeito.

Métricas de validação:
  · Spearman: correlação ordinal entre score_T0 e rproc_T1 (escala contínua).
    Valida se a ordenação do score corresponde à ordenação do risco real.
  · AUC-ROC: poder discriminatório binário (rproc_T1 > 3% = evento crônico).
    Valida se o score separa municípios que vão se tornar crônicos dos que não.
  Mann-Whitney foi removido: a dicotomia alto/baixo replica parcialmente o
  AUC com menos informação e produz resultados enganosos quando n_alto é
  pequeno (como na era parcial com limiares v7.0).

Uso:
  python src/backtest_validacao.py
  python src/backtest_validacao.py --pares completa
  python src/backtest_validacao.py --pares parcial
  python src/backtest_validacao.py --sem-rproc
  python src/backtest_validacao.py --excluir-t0 2020
  python src/backtest_validacao.py --excluir-t0 2020 2021

Saída:
  data/outputs/backtest_pares.csv   — registro por par município×ano
  data/outputs/backtest_resumo.txt  — relatório estatístico completo
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT         = Path(__file__).resolve().parent.parent.parent
SICONFI_PATH = ROOT / "data" / "processed" / "siconfi_indicadores_pb.csv"
OUTPUT_DIR   = ROOT / "data" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Anos com ruído estrutural externo conhecido.
# O relatório sinaliza automaticamente pares cuja correlação pode estar
# contaminada por esses eventos.
ANOS_ATIPICOS = {
    2020: "COVID — repasses emergenciais LC 173/2020 distorcem eorcam e lliq"
}

# ── Scorer: Lliq (peso 35%) ───────────────────────────────────────────────────
def score_lliq(lliq):
    """
    Curva linear por segmentos — v7.0.
    Retorna (norm 0.0–1.0, flag_suspeito).

    Âncoras calibradas empiricamente sobre o universo PB 2023-2025:
      lliq >= 0.35  -> 1.00  (plena solvência de curto prazo)
      lliq  = 0.10  -> 0.60  (caixa positivo, margem estreita)
      lliq  = 0.00  -> 0.35  (equilíbrio — sem folga nem déficit)
      lliq  = -0.50 -> 0.00  (teto inferior; valores abaixo são truncados)

    Flag suspeito: ativado quando lliq < -0.50, indicando dado fora da
    faixa de calibração. O valor é truncado em -0.50 antes do cálculo.
    """
    if pd.isna(lliq):
        return None, False

    suspeito = lliq < -0.50
    if suspeito:
        lliq = -0.50

    if lliq >= 0.35:
        return 1.00, suspeito
    elif lliq >= 0.10:
        return round(0.60 + (lliq - 0.10) / 0.25 * 0.40, 4), suspeito
    elif lliq >= 0.00:
        return round(0.35 + (lliq / 0.10) * 0.25, 4), suspeito
    else:
        return round(max(0.0, (lliq + 0.50) / 0.50 * 0.35), 4), suspeito

# ── Scorer: Eorcam (peso 15%) ─────────────────────────────────────────────────
def score_eorcam(eorcam_pct):
    """
    Execução orçamentária — zona ideal 90–105%.
    Penaliza sub-execução progressivamente abaixo de 90% e
    super-execução acima de 120% (sinal de orçamento mal dimensionado).
    """
    if pd.isna(eorcam_pct):
        return None
    e = eorcam_pct
    if   90.0 <= e <= 105.0: return 1.00
    elif 105.0 < e <= 120.0: return 1.00 - (e - 105.0) / 15.0 * 0.50
    elif e > 120.0:          return 0.50
    elif e >= 70.0:          return (e - 70.0) / 20.0
    else:                    return 0.00

def eorcam_ponderado(df_mun, ano_t):
    """
    Média ponderada dos últimos 5 anos de eorcam disponíveis até T.
    Pesos decrescentes (mais recente = maior peso): 40, 25, 20, 10, 5.
    Renormaliza automaticamente se menos de 5 anos estiverem disponíveis.
    Dados além do 5º ano mais recente recebem peso zero (não contribuem).
    """
    pesos_rel    = [40, 25, 20, 10, 5]
    anos_disp    = sorted(
        df_mun[(df_mun["ano"] <= ano_t) & df_mun["eorcam"].notna()]["ano"].unique(),
        reverse=True,
    )
    total_peso = total_pond = 0.0
    for i, ano in enumerate(anos_disp[:5]):
        p           = pesos_rel[i]
        total_pond += p * df_mun.loc[df_mun["ano"] == ano, "eorcam"].values[0]
        total_peso += p
    return total_pond / total_peso if total_peso > 0 else None

# ── Scorer: Qsiconfi (peso 15%) ───────────────────────────────────────────────
def score_qsiconfi(anos_entregues, max_anos):
    """Proporção de anos com RREO entregue dentro da janela 2020–T."""
    return min(anos_entregues / max_anos, 1.0) if max_anos > 0 else 0.0

# ── Scorer: RPproc (peso 15%) ─────────────────────────────────────────────────
def score_rproc(n_cronicos):
    """
    Penalização por cronicidade de restos a pagar processados (> 3%).
    A tabela foi calibrada para refletir persistência: cada ano adicional
    além do limiar representa deterioração incremental da capacidade de
    liquidação de obrigações anteriores.
    """
    tabela = {0: 1.00, 1: 0.75, 2: 0.50, 3: 0.30, 4: 0.10}
    return tabela.get(n_cronicos, 0.00)

# ── Score agregado ─────────────────────────────────────────────────────────────
# Espelho de scorers/config.py — manter sincronizado a cada versão.
PESOS = dict(lliq=35, cauc=10, eorcam=15, qsiconfi=15, autonomia=10, rproc=15)

def calcular_score(lliq_n, eorcam_n, qsiconfi_n, rproc_n,
                   cauc_n=0.0, autonomia_n=0.5,
                   incluir_rproc=True):
    """
    Agrega os componentes normalizados no score final (0–100).

    Componentes ausentes (None) ou explicitamente excluídos são removidos
    e seus pesos redistribuídos proporcionalmente entre os ativos.
    Isso garante que o score sempre some 100 pts independentemente de
    quais componentes estão disponíveis.

    Era inferida pelo estado de lliq_n:
      None  -> era parcial (lliq não disponível para esse par)
      float -> era completa
    """
    componentes = {
        "lliq":      (lliq_n,      PESOS["lliq"]),
        "cauc":      (1 - cauc_n,  PESOS["cauc"]),
        "eorcam":    (eorcam_n,    PESOS["eorcam"]),
        "qsiconfi":  (qsiconfi_n,  PESOS["qsiconfi"]),
        "autonomia": (autonomia_n, PESOS["autonomia"]),
        "rproc":     (rproc_n,     PESOS["rproc"]),
    }
    excluir = set()
    if lliq_n is None:    excluir.add("lliq")
    if not incluir_rproc: excluir.add("rproc")

    ativos = {k: v for k, v in componentes.items() if k not in excluir}
    # Substitui None remanescente por 0.5 (neutro) — não deve ocorrer em produção
    ativos = {k: (0.5 if v is None else v, p) for k, (v, p) in ativos.items()}

    peso_total = sum(p for _, p in ativos.values())
    score      = sum(v * p for v, p in ativos.values()) / peso_total * 100
    era        = "completa" if lliq_n is not None else "parcial"
    return round(score, 2), era

def classificar(score):
    """Limiares v7.0: ≥80 Baixo | ≥60 Médio | ≥40 Alto | <40 Crítico."""
    if   score >= 80: return "BAIXO"
    elif score >= 60: return "MEDIO"
    elif score >= 40: return "ALTO"
    else:             return "CRITICO"

# ── Construção dos pares walk-forward ─────────────────────────────────────────
PARES_ANOS = [
    (2020, 2021), (2021, 2022), (2022, 2023),   # era parcial
    (2023, 2024), (2024, 2025),                  # era completa
]

def construir_pares(df, incluir_rproc=True, excluir_t0=None):
    """
    Para cada par (T0, T1) e cada município com dados em ambos os anos,
    calcula o score com informação disponível em T0 e registra rproc_pct
    de T1 como desfecho.

    Exclusões aplicadas:
      · Municípios com periodo_rgf ausente em 2025 (bimestre incompleto).
      · Pares cujo T0 esteja em excluir_t0 (ex: anos atípicos).
      · Registros sem desfecho (rproc_t1 = NaN).

    n_cronicos_t0 conta anos com rproc_pct > 3% ANTES de T0 (histórico),
    não incluindo T0 em si — evita vazamento de informação do desfecho.
    """
    excluir_t0 = set(excluir_t0 or [])

    parciais_2025 = set(df.loc[(df["ano"] == 2025) & df["periodo_rgf"].isna(), "cod_ibge"])

    registros = []
    for t0, t1 in PARES_ANOS:
        if t0 in excluir_t0:
            continue

        df_t0 = df[df["ano"] == t0].set_index("cod_ibge")
        df_t1 = df[df["ano"] == t1].set_index("cod_ibge")

        for cod in set(df_t0.index) & set(df_t1.index):
            row_t0   = df_t0.loc[cod]
            row_t1   = df_t1.loc[cod]
            rproc_t1 = row_t1["rproc_pct"]

            if t1 == 2025 and cod in parciais_2025: continue
            if pd.isna(rproc_t1):                   continue

            df_mun = df[df["cod_ibge"] == cod]

            eorcam_w = eorcam_ponderado(df_mun, t0)
            eorcam_n = score_eorcam(eorcam_w)

            lliq_raw           = row_t0.get("lliq", np.nan)
            lliq_raw           = None if pd.isna(lliq_raw) else lliq_raw
            lliq_n, suspeito   = score_lliq(lliq_raw)

            anos_janela    = list(range(2020, t0 + 1))
            anos_entregues = int(df_mun[
                df_mun["ano"].isin(anos_janela) & (df_mun["entregou_rreo"] == True)
            ].shape[0])
            qsiconfi_n = score_qsiconfi(anos_entregues, len(anos_janela))

            # Histórico de rproc até (exclusive) T0 — sem look-ahead
            n_cronicos = int((
                df_mun[(df_mun["ano"] < t0) & df_mun["rproc_pct"].notna()]["rproc_pct"] > 3.0
            ).sum())
            rproc_n = score_rproc(n_cronicos)

            score, era = calcular_score(
                lliq_n, eorcam_n, qsiconfi_n, rproc_n,
                cauc_n=0.0, autonomia_n=0.5,
                incluir_rproc=incluir_rproc,
            )

            registros.append({
                "cod_ibge":      cod,
                "municipio":     row_t0["instituicao"],
                "populacao":     row_t0["populacao"],
                "ano_t0":        t0,
                "ano_t1":        t1,
                "era":           era,
                "score_t0":      score,
                "classe_t0":     classificar(score),
                "lliq_raw":      lliq_raw,
                "lliq_norm":     lliq_n,
                "eorcam_w":      round(eorcam_w, 2) if eorcam_w is not None else None,
                "eorcam_norm":   round(eorcam_n, 4) if eorcam_n is not None else None,
                "qsiconfi_norm": round(qsiconfi_n, 4),
                "n_cronicos_t0": n_cronicos,
                "rproc_norm":    round(rproc_n, 4),
                "rproc_t0":      row_t0["rproc_pct"],
                "rproc_t1":      rproc_t1,
                "dado_suspeito": suspeito,
            })

    return pd.DataFrame(registros)

# ── Análises estatísticas ──────────────────────────────────────────────────────
def analise_spearman(pares, label):
    """Correlação ordinal entre score_T0 e rproc_T1 (escala contínua)."""
    r, p = stats.spearmanr(pares["score_t0"], pares["rproc_t1"])
    return {"label": label, "n": len(pares), "r": round(r, 4), "p": round(p, 4)}

def analise_roc(pares, label, threshold=3.0):
    """
    AUC-ROC para o evento binário rproc_T1 > threshold.
    Score é invertido (100 - score) para que valor alto corresponda
    a maior probabilidade do evento negativo.
    """
    try:
        from sklearn.metrics import roc_auc_score
    except ImportError:
        return {"label": label, "auc": "sklearn nao instalado"}

    y_true = (pares["rproc_t1"] > threshold).astype(int)
    if y_true.sum() < 3:
        return {"label": label, "auc": "n_positivos insuficiente (< 3)"}

    auc = roc_auc_score(y_true, 100 - pares["score_t0"])
    return {
        "label":        label,
        "n":            len(pares),
        "n_positivos":  int(y_true.sum()),
        "pct_positivos": round(100 * y_true.mean(), 1),
        "auc":          round(auc, 4),
    }

def tabela_desfecho_por_classe(pares):
    """
    Para cada classe de risco em T0: n, mediana de rproc_T1
    e proporção que cruzou o limiar de 3% em T1.
    """
    linhas = []
    for classe in ["BAIXO", "MEDIO", "ALTO", "CRITICO"]:
        sub = pares[pares["classe_t0"] == classe]["rproc_t1"]
        if len(sub) == 0:
            continue
        linhas.append({
            "classe":           classe,
            "n":                len(sub),
            "mediana_rproc_t1": round(sub.median(), 2),
            "pct_cronicos_t1":  round(100 * (sub > 3.0).mean(), 1),
        })
    return pd.DataFrame(linhas)

# ── Relatório ──────────────────────────────────────────────────────────────────
def gerar_relatorio(pares, incluir_rproc, excluir_t0=None):
    excluir_t0 = set(excluir_t0 or [])
    L = []
    w = L.append

    w("=" * 70)
    w("BACKTEST WALK-FORWARD — SCORE DE SOLVENCIA SOLVELICITA v7.0")
    w("Paraiba . dados SICONFI 2020-2025")
    w("=" * 70)
    w(f"  RPproc no score : {'ativo' if incluir_rproc else 'desativado (--sem-rproc)'}")
    w(f"  CAUC            : neutro (0.0) — sem serie historica")
    w(f"  Autonomia       : neutra (0.5) — sem serie historica no siconfi")
    w(f"  Pesos ativos    : lliq={PESOS['lliq']} eorcam={PESOS['eorcam']} "
      f"qsiconfi={PESOS['qsiconfi']} rproc={PESOS['rproc']} "
      f"cauc={PESOS['cauc']} autonomia={PESOS['autonomia']}")
    if excluir_t0:
        for ano in sorted(excluir_t0):
            nota = ANOS_ATIPICOS.get(ano, "excluido via --excluir-t0")
            w(f"  T0 excluido     : {ano} ({nota})")
    w("")

    # ── Pares disponíveis ──────────────────────────────────────────────────
    w("-- PARES WALK-FORWARD " + "-" * 48)
    resumo = (
        pares.groupby(["ano_t0", "ano_t1", "era"])
        .agg(n=("score_t0", "count"),
             score_med=("score_t0", "mean"),
             rproc_t1_med=("rproc_t1", "median"))
        .reset_index()
    )
    for _, row in resumo.iterrows():
        atip = f"  ⚠ {ANOS_ATIPICOS[int(row.ano_t0)]}" if int(row.ano_t0) in ANOS_ATIPICOS else ""
        w(f"  {int(row.ano_t0)}->{int(row.ano_t1)} [{row.era:8s}]  "
          f"n={int(row.n):3d}  score_med={row.score_med:.1f}  "
          f"rproc_t1_med={row.rproc_t1_med:.2f}%{atip}")

    # ── Spearman por par ───────────────────────────────────────────────────
    w("")
    w("-- SPEARMAN: score_T0 x rproc_T1 (correlacao ordinal) " + "-" * 16)
    w("  Sinal esperado: r negativo (score alto -> menos RP futuro)")
    w("  Referencia: |r| < 0.10 fraco | 0.10-0.30 moderado | > 0.30 forte")
    for (t0, t1), g in pares.groupby(["ano_t0", "ano_t1"]):
        res = analise_spearman(g, f"{int(t0)}->{int(t1)}")
        sig = "***" if res["p"] < 0.001 else "**" if res["p"] < 0.01 else "*" if res["p"] < 0.05 else "n.s."
        forca = "forte" if abs(res["r"]) >= 0.30 else "moderado" if abs(res["r"]) >= 0.10 else "fraco"
        atip  = f"  ⚠ ano atipico: {ANOS_ATIPICOS[int(t0)]}" if int(t0) in ANOS_ATIPICOS else ""
        w(f"  {res['label']}  n={res['n']:3d}  r={res['r']:+.4f} ({forca})  p={res['p']:.4f} {sig}{atip}")

    w("")
    for label, sub in [("Era parcial (sem lliq) ", pares[pares["era"] == "parcial"]),
                        ("Era completa (com lliq)", pares[pares["era"] == "completa"]),
                        ("Total geral            ", pares)]:
        if len(sub) == 0: continue
        res = analise_spearman(sub, label)
        sig = "***" if res["p"] < 0.001 else "**" if res["p"] < 0.01 else "*" if res["p"] < 0.05 else "n.s."
        w(f"  {res['label']}  n={res['n']:3d}  r={res['r']:+.4f}  p={res['p']:.4f} {sig}")

    # ── AUC-ROC ───────────────────────────────────────────────────────────
    w("")
    w("-- AUC-ROC: probabilidade de evento cronico (rproc_T1 > 3%) " + "-" * 10)
    w("  Interpretacao: AUC = P(score correto | par aleatorio cronico vs nao-cronico)")
    w("  Referencia: 0.50 aleatorio | 0.60 fraco | 0.70 moderado | 0.80 forte")
    for label, sub in [("Era parcial ", pares[pares["era"] == "parcial"]),
                        ("Era completa", pares[pares["era"] == "completa"]),
                        ("Total       ", pares)]:
        if len(sub) == 0: continue
        res = analise_roc(sub, label)
        if isinstance(res["auc"], float):
            forca = "forte" if res["auc"] >= 0.80 else "moderado" if res["auc"] >= 0.70 else "fraco" if res["auc"] >= 0.60 else "nao discrimina"
            w(f"  {res['label']}  n={res['n']}  positivos={res['n_positivos']} ({res['pct_positivos']}%)  "
              f"AUC={res['auc']:.4f} ({forca})")
        else:
            w(f"  {res['label']}  {res['auc']}")

    # ── Desfecho por classe ────────────────────────────────────────────────
    w("")
    w("-- DESFECHO (rproc_T1) POR CLASSE DE RISCO EM T0 " + "-" * 21)
    for label, sub in [("Era completa (score pleno com lliq)", pares[pares["era"] == "completa"]),
                        ("Era parcial  (sem lliq)            ", pares[pares["era"] == "parcial"])]:
        if len(sub) == 0: continue
        w(f"  {label}:")
        for _, row in tabela_desfecho_por_classe(sub).iterrows():
            barra = chr(9608) * int(row["pct_cronicos_t1"] / 5)
            w(f"    {row['classe']:7s}  n={int(row['n']):3d}  "
              f"mediana={row['mediana_rproc_t1']:5.2f}%  "
              f"cronicos_t1={row['pct_cronicos_t1']:5.1f}%  {barra}")
        w("")

    # ── Casos extremos ────────────────────────────────────────────────────
    w("-- ERROS EXTREMOS (era completa) " + "-" * 37)
    w("  Falsos positivos: classificados como ALTO/CRITICO, rproc_T1 < 1%")
    fp = pares[(pares["era"] == "completa") &
               pares["classe_t0"].isin(["ALTO", "CRITICO"]) &
               (pares["rproc_t1"] < 1.0)][["municipio","ano_t0","score_t0","rproc_t1"]].head(8)
    for _, r in fp.iterrows():
        w(f"    {r.municipio:<30s}  score={r.score_t0:.1f}  rproc_t1={r.rproc_t1:.2f}%")

    w("  Falsos negativos: classificados como BAIXO/MEDIO, rproc_T1 > 5%")
    fn = pares[(pares["era"] == "completa") &
               pares["classe_t0"].isin(["BAIXO", "MEDIO"]) &
               (pares["rproc_t1"] > 5.0)][["municipio","ano_t0","score_t0","rproc_t1"]].head(8)
    for _, r in fn.iterrows():
        w(f"    {r.municipio:<30s}  score={r.score_t0:.1f}  rproc_t1={r.rproc_t1:.2f}%")

    w("")
    w("=" * 70)
    w("LIMITACOES DA VALIDACAO")
    w("  1. CAUC e Autonomia sem serie historica — 20% dos pesos neutralizados.")
    w("     O AUC real do score completo e provavelmente superior ao reportado.")
    w("  2. RPproc tem circularidade parcial com o desfecho.")
    w("     Rode --sem-rproc e compare os AUCs para quantificar o efeito.")
    w("  3. n_positivos na era completa = 81. IC do AUC tem amplitude ~0.12.")
    w("     Nao otimizar pesos com base nesses resultados — risco de overfitting.")
    w("  4. 2020 como T0 produz correlacao n.s. (r=-0.09) por distorcao COVID.")
    w("     Recomendado rodar --excluir-t0 2020 como analise de sensibilidade.")
    w("=" * 70)
    return "\n".join(L)

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Backtest walk-forward — Score de Solvencia SolveLicita v7.0"
    )
    parser.add_argument("--pares", choices=["completa", "parcial", "todos"],
                        default="todos")
    parser.add_argument("--sem-rproc", action="store_true",
                        help="Remove RPproc do score para isolar circularidade com o desfecho")
    parser.add_argument("--excluir-t0", nargs="+", type=int, default=[],
                        metavar="ANO",
                        help="Exclui pares cujo T0 seja um desses anos (ex: --excluir-t0 2020)")
    args = parser.parse_args()

    if not SICONFI_PATH.exists():
        print(f"[ERRO] {SICONFI_PATH} nao encontrado.")
        sys.exit(1)

    df = pd.read_csv(SICONFI_PATH)
    df["entregou_rreo"] = df["entregou_rreo"].map(
        {True: True, False: False, "True": True, "False": False}
    ).fillna(False)

    print(f"[OK] {len(df)} registros | {df['cod_ibge'].nunique()} municipios | "
          f"anos: {sorted(df['ano'].unique())}")
    if args.excluir_t0:
        print(f"     T0 excluidos: {args.excluir_t0}")

    pares = construir_pares(df, incluir_rproc=not args.sem_rproc,
                            excluir_t0=args.excluir_t0)

    if args.pares == "completa":
        pares = pares[pares["era"] == "completa"].copy()
    elif args.pares == "parcial":
        pares = pares[pares["era"] == "parcial"].copy()

    print(f"[OK] {len(pares)} pares | completa={( pares['era']=='completa').sum()} "
          f"| parcial={(pares['era']=='parcial').sum()}")

    sufixo   = ("_sem_rproc" if args.sem_rproc else "") +                (f"_ex{'_'.join(str(a) for a in sorted(args.excluir_t0))}" if args.excluir_t0 else "")
    csv_path = OUTPUT_DIR / f"backtest_pares{sufixo}.csv"
    pares.to_csv(csv_path, index=False)

    relatorio = gerar_relatorio(pares, incluir_rproc=not args.sem_rproc,
                                excluir_t0=args.excluir_t0)
    txt_path = OUTPUT_DIR / f"backtest_resumo{sufixo}.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(relatorio)

    print(f"[OK] CSV  -> {csv_path}")
    print(f"[OK] TXT  -> {txt_path}")
    print()
    print(relatorio)

if __name__ == "__main__":
    main()
