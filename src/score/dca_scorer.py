"""
dca_scorer.py — Pontuação DCA para uso pelo solvency.py
SolveLicita | Fase 1

Responsabilidades:
  1. Carregar dca_indicadores_pb.csv
  2. Aplicar flag de anomalia em Scaixa (provável distorção RPPS)
  3. Calcular pontuação do Scaixa com decaimento quadrático + capping
  4. Calcular pontuação da Autonomia via sigmoid regionalizada por porte
  5. Retornar DataFrame pronto para merge no solvency.py

Pesos DCA no score total (Fase 1 = 100 pts):
  scaixa    : 20 pts
  autonomia : 10 pts
"""

import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR  = Path(__file__).resolve().parent.parent.parent
PROCESSED = BASE_DIR / "data" / "processed"

# ── Pesos ─────────────────────────────────────────────────────────────────────
PESO_SCAIXA    = 20
PESO_AUTONOMIA = 10

# ── Capping de Scaixa ─────────────────────────────────────────────────────────
SCAIXA_CAP      = -0.50   # Scaixa < -0.50 → dado_suspeito = True + capping
SCAIXA_ANOMALIA = -0.50

# ── Parâmetros sigmoid de Autonomia (calibrados em 27/02/2026) ────────────────
# k = 2 / IQR_empírico por grupo | Rever anualmente com nova coleta DCA.
SIGMOID_PARAMS = {
    "micro"   : (0.0296, 98.6),    # < 10.000 hab
    "pequeno" : (0.0276, 77.9),    # 10.000 – 50.000 hab
    "médio"   : (0.0318, 96.2),    # 50.000 – 200.000 hab
    "grande"  : (0.0228, 306.2),   # > 200.000 hab
}


# ── Funções auxiliares ────────────────────────────────────────────────────────

def _porte(pop: int) -> str:
    if pop < 10_000:   return "micro"
    if pop < 50_000:   return "pequeno"
    if pop < 200_000:  return "médio"
    return "grande"


def pontuar_scaixa(x) -> float | None:
    """
    Score de Scaixa (0.0 → 1.0) com limiares fixos.

    Scaixa = (Ativo Financeiro - Passivo Financeiro) / Receita Corrente

    Limiares positivos (bondade cresce):
      >= 0.20       → 1.00  excelente folga de caixa
      0.10 – 0.20   → 0.75  folga razoável
      0.00 – 0.10   → linear 0.50 → 0.75

    Limiares negativos (decaimento quadrático — consistente com pontuar_rrestos):
      0.00          → 0.50  ponto neutro
      -0.50 – 0.00  → max(0, 0.50 × (1 − (|x| / 0.50))²)
      <= -0.50      → 0.00  + flag dado_suspeito
    """
    if pd.isna(x):
        return None
    x = max(x, SCAIXA_CAP)           # capping
    if x >= 0.20:
        return 1.00
    if x >= 0.10:
        return 0.75
    if x >= 0.00:
        return round(0.50 + (x / 0.10) * 0.25, 4)
    # negativo: decaimento quadrático
    return round(max(0.0, 0.50 * (1 - (-x / 0.50)) ** 2), 4)


def pontuar_autonomia(x, pop: int) -> float | None:
    """
    Score de Autonomia (0.0 → 1.0) via sigmoid regionalizada por porte.

    score(x) = 1 / (1 + exp(-k × (x - μ_porte)))

    Parâmetros calibrados com dados reais 2020-2024 da PB.
    """
    if pd.isna(x) or pd.isna(pop):
        return None
    porte = _porte(int(pop))
    mu, k = SIGMOID_PARAMS[porte]
    return round(float(1.0 / (1.0 + np.exp(-k * (x - mu)))), 4)


# ── Carregamento e processamento ──────────────────────────────────────────────

def carregar_dca(municipios: pd.DataFrame) -> pd.DataFrame:
    """
    Carrega dca_indicadores_pb.csv, aplica flags e calcula scores DCA.

    Parâmetros
    ----------
    municipios : DataFrame mestre com colunas cod_ibge, ente, populacao

    Retorna
    -------
    DataFrame com colunas:
      cod_ibge, scaixa_medio, autonomia_media,
      dado_suspeito, scaixa_norm, autonomia_norm,
      contrib_scaixa, contrib_autonomia
    """
    caminho = PROCESSED / "dca_indicadores_pb.csv"
    if not caminho.exists():
        raise FileNotFoundError(
            f"dca_indicadores_pb.csv não encontrado em {PROCESSED}. "
            "Execute src/collectors/dca.py primeiro."
        )

    dca = pd.read_csv(caminho, dtype={"cod_ibge": str})

    dca = dca.merge(
        municipios[["cod_ibge", "populacao"]],
        on="cod_ibge", how="left"
    )

    # ── Flag de anomalia ──────────────────────────────────────────────────────
    dca["dado_suspeito"] = (
        dca["scaixa_medio"].notna() &
        (dca["scaixa_medio"] < SCAIXA_ANOMALIA)
    )

    # ── Scores normalizados (0.0 – 1.0) ──────────────────────────────────────
    dca["scaixa_norm"] = dca["scaixa_medio"].apply(pontuar_scaixa)

    dca["autonomia_norm"] = dca.apply(
        lambda r: pontuar_autonomia(r["autonomia_media"], r["populacao"]),
        axis=1
    )

    # ── Contribuições para o score total ─────────────────────────────────────
    dca["contrib_scaixa"]    = (PESO_SCAIXA    * dca["scaixa_norm"]).round(4)
    dca["contrib_autonomia"] = (PESO_AUTONOMIA * dca["autonomia_norm"]).round(4)

    cols_out = [
        "cod_ibge",
        "scaixa_medio", "autonomia_media",
        "dado_suspeito",
        "scaixa_norm",  "autonomia_norm",
        "contrib_scaixa", "contrib_autonomia",
    ]
    return dca[cols_out]