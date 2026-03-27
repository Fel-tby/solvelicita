"""
classifier.py — Classificação de risco e caps duros.
Único arquivo do projeto onde ORDEM_RISCO e as regras de cap existem.
Versão: 7.0
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from scorers.config import LIMIARES_SCORE, N_ANOS_CRONICOS_CAP_MEDIO

ORDEM_RISCO = ["🟢 Risco Baixo", "🟡 Risco Médio", "🔴 Risco Alto", "⛔ Crítico"]
ORDEM_SORT  = {c: i for i, c in enumerate(ORDEM_RISCO + ["⚫ Sem Dados"])}


def _cap(classe_atual: str, teto: str) -> str:
    """Retorna o mais restritivo entre classe atual e teto."""
    return ORDEM_RISCO[max(ORDEM_RISCO.index(classe_atual), ORDEM_RISCO.index(teto))]


def classificar(score, anos_entregues: int, n_anos_cronicos: int) -> str:
    """
    Atribui classificação de risco ao score numérico.

    Limiares v7.0 (lidos de scorers.config.LIMIARES_SCORE):
      ≥ 80  → 🟢 Risco Baixo   (v6.2: 75)
      ≥ 60  → 🟡 Risco Médio   (v6.2: 55)
      ≥ 40  → 🔴 Risco Alto    (v6.2: 35)
       < 40  → ⛔ Crítico       (v6.2: < 35)

    Ordem de verificação:
      0. score ausente ou anos_entregues = 0  → ⚫ Sem Dados
      1. score numérico                       → classe base
      2. Cap RPproc : >= N_ANOS_CRONICOS_CAP_MEDIO (=4) → teto 🟡 Risco Médio
                      (v6.2: >= 5)
      3. Cap Qsiconfi: ≤ 2 anos entregues     → teto 🔴 Risco Alto  (inalterado)
                       = 3 anos entregues     → teto 🟡 Risco Médio (inalterado)
    """
    if pd.isna(score) or int(anos_entregues) == 0:
        return "⚫ Sem Dados"

    # ── 1. Classe base via limiares ───────────────────────────────────────
    if   score >= LIMIARES_SCORE["baixo"]: classe = "🟢 Risco Baixo"
    elif score >= LIMIARES_SCORE["medio"]: classe = "🟡 Risco Médio"
    elif score >= LIMIARES_SCORE["alto"]:  classe = "🔴 Risco Alto"
    else:                                  classe = "⛔ Crítico"

    # ── 2. Cap RPproc (v7.0: ≥ 4 anos crônicos) ──────────────────────────
    if int(n_anos_cronicos) >= N_ANOS_CRONICOS_CAP_MEDIO:
        classe = _cap(classe, "🟡 Risco Médio")

    # ── 3. Cap Qsiconfi (inalterado v6.2) ────────────────────────────────
    n = int(anos_entregues)
    if n <= 2:
        classe = _cap(classe, "🔴 Risco Alto")
    elif n == 3:
        classe = _cap(classe, "🟡 Risco Médio")

    return classe
