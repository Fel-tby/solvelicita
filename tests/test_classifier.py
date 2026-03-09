"""
tests/test_classifier.py
Testa o classificador de risco e os caps duros da metodologia.

O classifier não lê arquivos — recebe apenas 3 números:
  (score, anos_entregues, n_anos_cronicos)

Por isso os fixtures aqui são fabricados diretamente no código,
não lidos de CSV. Cada caso representa uma situação municipal real.

Rodar:
    pytest tests/test_classifier.py -v
"""

import sys
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from engine.classifier import classificar


# ══════════════════════════════════════════════════════════════════════════════
# Classificação base (sem caps)
# ══════════════════════════════════════════════════════════════════════════════

class TestClassificacaoBase:
    """Testa os limiares numéricos sem nenhum cap ativo."""

    def test_score_alto_risco_baixo(self):
        """Score ≥ 75 → 🟢 Risco Baixo."""
        assert classificar(75.0, 6, 0) == "🟢 Risco Baixo"
        assert classificar(90.0, 6, 0) == "🟢 Risco Baixo"

    def test_score_medio_risco_medio(self):
        """55 ≤ score < 75 → 🟡 Risco Médio."""
        assert classificar(55.0, 6, 0) == "🟡 Risco Médio"
        assert classificar(65.0, 6, 0) == "🟡 Risco Médio"
        assert classificar(74.9, 6, 0) == "🟡 Risco Médio"

    def test_score_baixo_risco_alto(self):
        """35 ≤ score < 55 → 🔴 Risco Alto."""
        assert classificar(35.0, 6, 0) == "🔴 Risco Alto"
        assert classificar(45.0, 6, 0) == "🔴 Risco Alto"
        assert classificar(54.9, 6, 0) == "🔴 Risco Alto"

    def test_score_critico(self):
        """Score < 35 → ⛔ Crítico."""
        assert classificar(34.9, 6, 0) == "⛔ Crítico"
        assert classificar(0.0,  6, 0) == "⛔ Crítico"

    def test_score_exatamente_nos_limiares(self):
        """Testa os valores exatos de fronteira entre classes."""
        assert classificar(75.0, 6, 0) == "🟢 Risco Baixo"
        assert classificar(55.0, 6, 0) == "🟡 Risco Médio"
        assert classificar(35.0, 6, 0) == "🔴 Risco Alto"


# ══════════════════════════════════════════════════════════════════════════════
# Sem Dados
# ══════════════════════════════════════════════════════════════════════════════

class TestSemDados:
    """Municípios sem dados suficientes nunca recebem classificação de risco."""

    def test_score_ausente_sem_dados(self):
        """Score None → ⚫ Sem Dados, independente do resto."""
        assert classificar(None, 6, 0) == "⚫ Sem Dados"

    def test_zero_anos_entregues_sem_dados(self):
        """Município que nunca entregou RREO → ⚫ Sem Dados."""
        assert classificar(80.0, 0, 0) == "⚫ Sem Dados"

    def test_score_nan_sem_dados(self):
        import math
        assert classificar(float("nan"), 6, 0) == "⚫ Sem Dados"


# ══════════════════════════════════════════════════════════════════════════════
# Cap RPproc — Cronicidade de Restos a Pagar
# ══════════════════════════════════════════════════════════════════════════════

class TestCapRproc:
    """
    Cap duro: município com ≥ 5 anos crônicos de RP Processados
    não pode ser classificado melhor que 🟡 Risco Médio.
    """

    def test_score_excelente_com_5_anos_cronicos_vira_medio(self):
        """Score 85 (seria 🟢) mas 5 anos crônicos → teto 🟡."""
        assert classificar(85.0, 6, 5) == "🟡 Risco Médio"

    def test_score_medio_com_5_anos_cronicos_permanece_medio(self):
        """Score 65 (já 🟡) com 5 anos crônicos → permanece 🟡."""
        assert classificar(65.0, 6, 5) == "🟡 Risco Médio"

    def test_score_alto_com_5_anos_cronicos_permanece_alto(self):
        """Score 45 (🔴) com 5 anos crônicos → cap não melhora, permanece 🔴."""
        assert classificar(45.0, 6, 5) == "🔴 Risco Alto"

    def test_critico_com_5_anos_cronicos_permanece_critico(self):
        """Score 20 (⛔) com 5 anos crônicos → cap não ajuda, permanece ⛔."""
        assert classificar(20.0, 6, 5) == "⛔ Crítico"

    def test_4_anos_cronicos_nao_ativa_cap(self):
        """4 anos crônicos ainda não ativa o cap — score 🟢 permanece 🟢."""
        assert classificar(80.0, 6, 4) == "🟢 Risco Baixo"

    def test_6_anos_cronicos_tambem_ativa_cap(self):
        """Cap vale para ≥ 5 — 6 anos também trava em 🟡."""
        assert classificar(90.0, 6, 6) == "🟡 Risco Médio"


# ══════════════════════════════════════════════════════════════════════════════
# Cap Qsiconfi — Qualidade de Transparência
# ══════════════════════════════════════════════════════════════════════════════

class TestCapQsiconfi:
    """
    Cap duro por transparência fiscal:
      ≤ 2 anos entregues → teto 🔴 Risco Alto
      = 3 anos entregues → teto 🟡 Risco Médio
      ≥ 4 anos entregues → sem cap
    """

    def test_1_ano_entregue_score_alto_vira_risco_alto(self):
        """Score 80 (seria 🟢) mas só 1 ano entregue → teto 🔴."""
        assert classificar(80.0, 1, 0) == "🔴 Risco Alto"

    def test_2_anos_entregues_score_alto_vira_risco_alto(self):
        """Score 80 com 2 anos entregues → teto 🔴."""
        assert classificar(80.0, 2, 0) == "🔴 Risco Alto"

    def test_2_anos_entregues_ja_risco_alto_permanece(self):
        """Score 45 (🔴) com 2 anos → cap não muda nada."""
        assert classificar(45.0, 2, 0) == "🔴 Risco Alto"

    def test_2_anos_entregues_critico_permanece_critico(self):
        """Score 20 (⛔) com 2 anos → cap não melhora."""
        assert classificar(20.0, 2, 0) == "⛔ Crítico"

    def test_3_anos_entregues_score_alto_vira_medio(self):
        """Score 80 com 3 anos entregues → teto 🟡."""
        assert classificar(80.0, 3, 0) == "🟡 Risco Médio"

    def test_3_anos_entregues_ja_medio_permanece(self):
        """Score 60 (🟡) com 3 anos → permanece 🟡."""
        assert classificar(60.0, 3, 0) == "🟡 Risco Médio"

    def test_3_anos_entregues_risco_alto_permanece(self):
        """Score 45 (🔴) com 3 anos → cap não melhora."""
        assert classificar(45.0, 3, 0) == "🔴 Risco Alto"

    def test_4_anos_entregues_sem_cap(self):
        """4 anos entregues → sem cap, score 🟢 permanece 🟢."""
        assert classificar(80.0, 4, 0) == "🟢 Risco Baixo"

    def test_6_anos_entregues_sem_cap(self):
        """Transparência total → classificação depende só do score."""
        assert classificar(80.0, 6, 0) == "🟢 Risco Baixo"
        assert classificar(45.0, 6, 0) == "🔴 Risco Alto"


# ══════════════════════════════════════════════════════════════════════════════
# Caps combinados
# ══════════════════════════════════════════════════════════════════════════════

class TestCapsCombinados:
    """Quando os dois caps incidem ao mesmo tempo, prevalece o mais restritivo."""

    def test_3_anos_e_5_cronicos_prevalece_mais_restritivo(self):
        """3 anos (teto 🟡) + 5 crônicos (teto 🟡) → 🟡.
        Score 90 (seria 🟢) → cai para 🟡."""
        assert classificar(90.0, 3, 5) == "🟡 Risco Médio"

    def test_2_anos_e_5_cronicos_prevalece_risco_alto(self):
        """2 anos (teto 🔴) + 5 crônicos (teto 🟡) → prevalece 🔴.
        Score 90 (seria 🟢) → cai para 🔴."""
        assert classificar(90.0, 2, 5) == "🔴 Risco Alto"
