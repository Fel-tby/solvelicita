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
# Classificação base (sem caps)  —  limiares v7.0: 80 / 60 / 40
# ══════════════════════════════════════════════════════════════════════════════

class TestClassificacaoBase:
    """Testa os limiares numéricos sem nenhum cap ativo."""

    def test_score_alto_risco_baixo(self):
        """Score >= 80 -> Risco Baixo."""
        assert classificar(80.0, 6, 0) == "🟢 Risco Baixo"
        assert classificar(90.0, 6, 0) == "🟢 Risco Baixo"

    def test_score_medio_risco_medio(self):
        """60 <= score < 80 -> Risco Medio."""
        assert classificar(60.0, 6, 0) == "🟡 Risco Médio"
        assert classificar(65.0, 6, 0) == "🟡 Risco Médio"
        assert classificar(79.9, 6, 0) == "🟡 Risco Médio"

    def test_score_baixo_risco_alto(self):
        """40 <= score < 60 -> Risco Alto."""
        assert classificar(40.0, 6, 0) == "🔴 Risco Alto"
        assert classificar(45.0, 6, 0) == "🔴 Risco Alto"
        assert classificar(59.9, 6, 0) == "🔴 Risco Alto"

    def test_score_critico(self):
        """Score < 40 -> Critico."""
        assert classificar(39.9, 6, 0) == "⛔ Crítico"
        assert classificar(0.0,  6, 0) == "⛔ Crítico"

    def test_score_exatamente_nos_limiares(self):
        """Testa os valores exatos de fronteira entre classes."""
        assert classificar(80.0, 6, 0) == "🟢 Risco Baixo"
        assert classificar(60.0, 6, 0) == "🟡 Risco Médio"
        assert classificar(40.0, 6, 0) == "🔴 Risco Alto"


# ══════════════════════════════════════════════════════════════════════════════
# Sem Dados
# ══════════════════════════════════════════════════════════════════════════════

class TestSemDados:
    """Municípios sem dados suficientes nunca recebem classificação de risco."""

    def test_score_ausente_sem_dados(self):
        """Score None -> Sem Dados, independente do resto."""
        assert classificar(None, 6, 0) == "⚫ Sem Dados"

    def test_zero_anos_entregues_sem_dados(self):
        """Município que nunca entregou RREO -> Sem Dados."""
        assert classificar(80.0, 0, 0) == "⚫ Sem Dados"

    def test_score_nan_sem_dados(self):
        import math
        assert classificar(float("nan"), 6, 0) == "⚫ Sem Dados"


# ══════════════════════════════════════════════════════════════════════════════
# Cap RPproc — Cronicidade de Restos a Pagar
# ══════════════════════════════════════════════════════════════════════════════

class TestCapRproc:
    """
    Cap duro: município com >= 4 anos crônicos de RP Processados
    não pode ser classificado melhor que Risco Médio (v7.0: threshold baixou de 5 para 4).
    """

    def test_score_excelente_com_5_anos_cronicos_vira_medio(self):
        """Score 85 (seria Baixo) mas 5 anos crônicos -> teto Médio."""
        assert classificar(85.0, 6, 5) == "🟡 Risco Médio"

    def test_score_medio_com_5_anos_cronicos_permanece_medio(self):
        """Score 65 (já Médio) com 5 anos crônicos -> permanece Médio."""
        assert classificar(65.0, 6, 5) == "🟡 Risco Médio"

    def test_score_alto_com_5_anos_cronicos_permanece_alto(self):
        """Score 45 (Alto) com 5 anos crônicos -> cap não melhora, permanece Alto."""
        assert classificar(45.0, 6, 5) == "🔴 Risco Alto"

    def test_critico_com_5_anos_cronicos_permanece_critico(self):
        """Score 20 (Crítico) com 5 anos crônicos -> cap não ajuda, permanece Crítico."""
        assert classificar(20.0, 6, 5) == "⛔ Crítico"

    def test_3_anos_cronicos_nao_ativa_cap(self):
        """3 anos crônicos ainda não ativa o cap — score Baixo permanece Baixo."""
        assert classificar(80.0, 6, 3) == "🟢 Risco Baixo"

    def test_4_anos_cronicos_ativa_cap(self):
        """v7.0: threshold do cap baixou de >=5 para >=4 — score Baixo cai para Médio."""
        assert classificar(80.0, 6, 4) == "🟡 Risco Médio"

    def test_6_anos_cronicos_tambem_ativa_cap(self):
        """Cap vale para >= 4 — 6 anos também trava em Médio."""
        assert classificar(90.0, 6, 6) == "🟡 Risco Médio"


# ══════════════════════════════════════════════════════════════════════════════
# Cap Qsiconfi — Qualidade de Transparência
# ══════════════════════════════════════════════════════════════════════════════

class TestCapQsiconfi:
    """
    Cap duro por transparência fiscal:
        <= 2 anos entregues -> teto Risco Alto
        = 3 anos entregues  -> teto Risco Médio
        >= 4 anos entregues -> sem cap
    """

    def test_1_ano_entregue_score_alto_vira_risco_alto(self):
        """Score 80 (seria Baixo) mas só 1 ano entregue -> teto Alto."""
        assert classificar(80.0, 1, 0) == "🔴 Risco Alto"

    def test_2_anos_entregues_score_alto_vira_risco_alto(self):
        """Score 80 com 2 anos entregues -> teto Alto."""
        assert classificar(80.0, 2, 0) == "🔴 Risco Alto"

    def test_2_anos_entregues_ja_risco_alto_permanece(self):
        """Score 45 (Alto) com 2 anos -> cap não muda nada."""
        assert classificar(45.0, 2, 0) == "🔴 Risco Alto"

    def test_2_anos_entregues_critico_permanece_critico(self):
        """Score 20 (Crítico) com 2 anos -> cap não melhora."""
        assert classificar(20.0, 2, 0) == "⛔ Crítico"

    def test_3_anos_entregues_score_alto_vira_medio(self):
        """Score 80 com 3 anos entregues -> teto Médio."""
        assert classificar(80.0, 3, 0) == "🟡 Risco Médio"

    def test_3_anos_entregues_ja_medio_permanece(self):
        """Score 60 (Médio) com 3 anos -> permanece Médio."""
        assert classificar(60.0, 3, 0) == "🟡 Risco Médio"

    def test_3_anos_entregues_risco_alto_permanece(self):
        """Score 45 (Alto) com 3 anos -> cap não melhora."""
        assert classificar(45.0, 3, 0) == "🔴 Risco Alto"

    def test_4_anos_entregues_sem_cap(self):
        """4 anos entregues -> sem cap de transparência, score Baixo permanece Baixo."""
        assert classificar(80.0, 4, 0) == "🟢 Risco Baixo"

    def test_6_anos_entregues_sem_cap(self):
        """Transparência total -> classificação depende só do score."""
        assert classificar(80.0, 6, 0) == "🟢 Risco Baixo"
        assert classificar(45.0, 6, 0) == "🔴 Risco Alto"


# ══════════════════════════════════════════════════════════════════════════════
# Caps combinados
# ══════════════════════════════════════════════════════════════════════════════

class TestCapsCombinados:
    """Quando os dois caps incidem ao mesmo tempo, prevalece o mais restritivo."""

    def test_3_anos_e_5_cronicos_prevalece_mais_restritivo(self):
        """3 anos (teto Médio) + 5 crônicos (teto Médio) -> Médio.
        Score 90 (seria Baixo) -> cai para Médio."""
        assert classificar(90.0, 3, 5) == "🟡 Risco Médio"

    def test_2_anos_e_5_cronicos_prevalece_risco_alto(self):
        """2 anos (teto Alto) + 5 crônicos (teto Médio) -> prevalece Alto.
        Score 90 (seria Baixo) -> cai para Alto."""
        assert classificar(90.0, 2, 5) == "🔴 Risco Alto"
