"""
classifier.py — Classificação de risco e caps duros.
Único arquivo do projeto onde ORDEM_RISCO e as regras de cap existem.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

ORDEM_RISCO = ["🟢 Risco Baixo", "🟡 Risco Médio", "🔴 Risco Alto", "⛔ Crítico"]
ORDEM_SORT  = {c: i for i, c in enumerate(ORDEM_RISCO + ["⚫ Sem Dados"])}


def _cap(classe_atual: str, teto: str) -> str:
    """Retorna o mais restritivo entre classe atual e teto."""
    return ORDEM_RISCO[max(ORDEM_RISCO.index(classe_atual), ORDEM_RISCO.index(teto))]


def classificar(score, anos_entregues: int, n_anos_cronicos: int) -> str:
    """
    Atribui classificação de risco ao score numérico.

    Ordem de verificação:
      0. score ausente ou anos_entregues = 0    → ⚫ Sem Dados
      1. score numérico                         → classe base
      2. Cap RPproc: ≥ 5 anos crônicos         → teto 🟡 Risco Médio
      3. Cap Qsiconfi: ≤ 2 anos entregues      → teto 🔴 Risco Alto
                       = 3 anos entregues      → teto 🟡 Risco Médio
    """
    if pd.isna(score) or int(anos_entregues) == 0:
        return "⚫ Sem Dados"

    if score >= 75:   classe = "🟢 Risco Baixo"
    elif score >= 55: classe = "🟡 Risco Médio"
    elif score >= 35: classe = "🔴 Risco Alto"
    else:             classe = "⛔ Crítico"

    if int(n_anos_cronicos) >= 5:
        classe = _cap(classe, "🟡 Risco Médio")

    n = int(anos_entregues)
    if n <= 2:
        classe = _cap(classe, "🔴 Risco Alto")
    elif n == 3:
        classe = _cap(classe, "🟡 Risco Médio")

    return classe
