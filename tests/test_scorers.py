"""
tests/test_scorers.py
Testa cada scorer com dados reais dos fixtures.

Municípios presentes nos fixtures e por que foram escolhidos:
  2500304  Alagoa Grande  → entrega todos os 6 anos, rproc crônico (2020-22),
                            eorcam anômalo (152% em 2022), CAUC REGULAR
  2500502  Alagoinha      → lliq alto (0.60 em 2023), eorcam abaixo de 70% (2020),
                            rproc crônico só em 2022 (4.86%)
  2500205  Aguiar         → eorcam muito alto (127% em 2024), CAUC com pendência grave
  2500106  Água Branca    → nunca entregou RREO — testa ausência total de dados

Rodar:
    pytest tests/test_scorers.py -v
"""

import sys
from pathlib import Path
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from scorers.lliq_scorer     import pontuar_lliq
from scorers.eorcam_scorer   import pontuar_eorcam, calcular as calcular_eorcam
from scorers.cauc_scorer     import pontuar_ccauc, calcular as calcular_cauc
from scorers.qsiconfi_scorer import calcular as calcular_qsiconfi
from scorers.rproc_scorer    import pontuar_rproc_cronico, calcular as calcular_rproc
from scorers.config          import PESOS

FIXTURES = Path(__file__).parent / "fixtures"


# ── Helpers ───────────────────────────────────────────────────────────────────

def carregar_siconfi() -> pd.DataFrame:
    df = pd.read_csv(FIXTURES / "siconfi_sample.csv", dtype={"cod_ibge": str})
    df["entregou_rreo"] = df["entregou_rreo"].astype(str).str.lower() == "true"
    df["lliq_parcial"]  = df["lliq_parcial"].astype(str).str.lower()  == "true"
    return df

def carregar_cauc() -> pd.DataFrame:
    return pd.read_csv(FIXTURES / "cauc_sample.csv", dtype={"cod_ibge": str})

def carregar_municipios() -> pd.DataFrame:
    return pd.read_csv(FIXTURES / "municipios_sample.csv", dtype={"cod_ibge": str})


# ══════════════════════════════════════════════════════════════════════════════
# LLIQ — Liquidez Líquida (peso 30)
# ══════════════════════════════════════════════════════════════════════════════

class TestLliq:

    # ── Regras da curva ───────────────────────────────────────────────────────

    def test_lliq_acima_020_retorna_maximo(self):
        """Lliq ≥ 0.20 → score 1.0 (folga sólida de caixa)."""
        assert pontuar_lliq(0.20) == 1.0
        assert pontuar_lliq(0.50) == 1.0

    def test_lliq_zero_retorna_meio(self):
        """Lliq = 0.0 → score 0.5 (ponto de inflexão da curva)."""
        assert pontuar_lliq(0.0) == 0.50

    def test_lliq_negativo_moderado_entre_0_e_05(self):
        """-0.50 < Lliq < 0 → curva quadrática, resultado entre 0 e 0.50."""
        resultado = pontuar_lliq(-0.25)
        assert 0.0 < resultado < 0.50

    def test_lliq_extremo_negativo_capado_em_zero(self):
        """Lliq < -0.50 é capado antes do cálculo — nunca retorna negativo."""
        assert pontuar_lliq(-0.99) == 0.0
        assert pontuar_lliq(-10.0) == 0.0

    def test_lliq_ausente_retorna_none(self):
        """Dado ausente não deve produzir score."""
        assert pontuar_lliq(None)          is None
        assert pontuar_lliq(float("nan"))  is None

    # ── Casos reais dos fixtures ──────────────────────────────────────────────

    def test_alagoinha_2023_lliq_alto(self):
        """Alagoinha 2023: lliq=0.60 → zona máxima (≥ 0.20 → 1.0)."""
        assert pontuar_lliq(0.600615) == 1.0

    def test_alagoa_grande_2024_lliq_baixo(self):
        """Alagoa Grande 2024: lliq=0.067 → zona estreita, entre 0.50 e 0.75."""
        resultado = pontuar_lliq(0.067115)
        assert 0.50 < resultado < 0.75

    def test_aguiar_2023_lliq_razoavel(self):
        """Aguiar 2023: lliq=0.163 → zona razoável, entre 0.75 e 1.0."""
        resultado = pontuar_lliq(0.162712)
        assert 0.75 < resultado < 1.0

    def test_contrib_nunca_excede_peso(self):
        """contrib_lliq nunca ultrapassa o peso configurado (30 pts)."""
        from scorers.lliq_scorer import calcular
        result = calcular(carregar_siconfi(), carregar_municipios())
        assert (result["contrib_lliq"] <= PESOS["lliq"]).all()
        assert (result["contrib_lliq"] >= 0).all()


# ══════════════════════════════════════════════════════════════════════════════
# EORCAM — Execução Orçamentária (peso 20)
# ══════════════════════════════════════════════════════════════════════════════

class TestEorcam:

    # ── Regras da curva ───────────────────────────────────────────────────────

    def test_zona_saudavel_retorna_maximo(self):
        """90% ≤ execução ≤ 105% → score 1.0 (zona ótima)."""
        assert pontuar_eorcam(90.0)  == 1.0
        assert pontuar_eorcam(100.0) == 1.0
        assert pontuar_eorcam(105.0) == 1.0

    def test_excesso_anomalo_teto_05(self):
        """execução > 120% → teto 0.5 (arrecadação não sustentável)."""
        assert pontuar_eorcam(121.0) == 0.5
        assert pontuar_eorcam(200.0) == 0.5

    def test_excesso_moderado_decai_linearmente(self):
        """105% < execução ≤ 120% → decaimento linear entre 0.5 e 1.0."""
        resultado = pontuar_eorcam(112.5)
        assert 0.5 < resultado < 1.0

    def test_zona_atencao_proporcional(self):
        """70% ≤ execução < 90% → proporcional entre 0 e 1.0."""
        resultado = pontuar_eorcam(80.0)
        assert 0.0 < resultado < 1.0

    def test_colapso_arrecadacao_retorna_zero(self):
        """execução < 70% → 0.0 (colapso de arrecadação)."""
        assert pontuar_eorcam(69.9) == 0.0
        assert pontuar_eorcam(0.0)  == 0.0

    def test_ausente_retorna_none(self):
        assert pontuar_eorcam(None) is None

    # ── Casos reais dos fixtures ──────────────────────────────────────────────

    def test_alagoa_grande_2022_anomalo(self):
        """Alagoa Grande 2022: eorcam=152.71 → teto 0.5."""
        assert pontuar_eorcam(152.71) == 0.5

    def test_alagoinha_2020_colapso(self):
        """Alagoinha 2020: eorcam=69.26 → abaixo de 70%, retorna 0.0."""
        assert pontuar_eorcam(69.26) == 0.0

    def test_aguiar_2024_anomalo(self):
        """Aguiar 2024: eorcam=127.17 → teto 0.5."""
        assert pontuar_eorcam(127.17) == 0.5

    def test_agua_branca_sem_rreo_nao_aparece(self):
        """Água Branca nunca entregou RREO — não deve aparecer no resultado."""
        result = calcular_eorcam(carregar_siconfi())
        assert "2500106" not in result["cod_ibge"].values

    def test_contrib_dentro_do_peso(self):
        """contrib_eorcam ∈ [0, 20] para todos os municípios."""
        result = calcular_eorcam(carregar_siconfi())
        assert (result["contrib_eorcam"] >= 0).all()
        assert (result["contrib_eorcam"] <= PESOS["eorcam"]).all()


# ══════════════════════════════════════════════════════════════════════════════
# CAUC — Risco de Bloqueio Federal (peso 20)
# ══════════════════════════════════════════════════════════════════════════════

class TestCauc:

    # ── Regras de classificação ───────────────────────────────────────────────

    def test_regular_sem_penalidade(self):
        """REGULAR → ccauc = 0.0 → contribuição máxima."""
        assert pontuar_ccauc("REGULAR") == 0.0

    def test_pendencia_grave_zera(self):
        """Pendência grave isolada → ccauc = 1.0 → contrib = 0."""
        assert pontuar_ccauc("Regularidade Fiscal (RFB)") == 1.0
        assert pontuar_ccauc("Adimplência TCU")           == 1.0
        assert pontuar_ccauc("CADIN")                     == 1.0

    def test_grave_misturada_ainda_zera(self):
        """Uma grave entre moderadas → ainda zera (gatilho punitivo)."""
        assert pontuar_ccauc("Regularidade Fiscal (RFB) | Regularidade FGTS") == 1.0

    def test_so_moderadas_penalidade_proporcional(self):
        """Só moderadas → ccauc proporcional, teto 0.5."""
        resultado = pontuar_ccauc("Regularidade FGTS | Regularidade Trabalhista (TST)")
        assert 0.0 < resultado <= 0.5

    def test_ausente_pior_caso(self):
        """Dado ausente → ccauc = 1.0 (município não rastreável = pior caso)."""
        assert pontuar_ccauc(None)         == 1.0
        assert pontuar_ccauc(float("nan")) == 1.0

    # ── Casos reais dos fixtures ──────────────────────────────────────────────

    def test_agua_branca_contrib_zero(self):
        """Água Branca tem RFB + CADIN (graves) → contrib = 0."""
        result = calcular_cauc(carregar_cauc())
        mun = result[result["cod_ibge"] == "2500106"].iloc[0]
        assert mun["contrib_ccauc"] == 0.0

    def test_aguiar_contrib_zero(self):
        """Aguiar também tem RFB → contrib = 0."""
        result = calcular_cauc(carregar_cauc())
        mun = result[result["cod_ibge"] == "2500205"].iloc[0]
        assert mun["contrib_ccauc"] == 0.0

    def test_alagoa_grande_contrib_maxima(self):
        """Alagoa Grande está REGULAR → contrib_ccauc = 20."""
        result = calcular_cauc(carregar_cauc())
        mun = result[result["cod_ibge"] == "2500304"].iloc[0]
        assert mun["contrib_ccauc"] == PESOS["ccauc"]

    def test_alagoinha_contrib_zero(self):
        """Alagoinha tem CADIN nas pendências — CADIN é grave → contrib = 0.
        Nota: o teste original assumiu que eram só moderadas, mas CADIN
        está em PENDENCIAS_GRAVES e aciona o gatilho punitivo."""
        result = calcular_cauc(carregar_cauc())
        mun = result[result["cod_ibge"] == "2500502"].iloc[0]
        assert mun["contrib_ccauc"] == 0.0

    def test_contrib_sempre_dentro_do_peso(self):
        """contrib_ccauc ∈ [0, 20] para todos."""
        result = calcular_cauc(carregar_cauc())
        assert (result["contrib_ccauc"] >= 0).all()
        assert (result["contrib_ccauc"] <= PESOS["ccauc"]).all()


# ══════════════════════════════════════════════════════════════════════════════
# QSICONFI — Qualidade de Transparência (peso 15)
# ══════════════════════════════════════════════════════════════════════════════

class TestQsiconfi:

    def test_entregou_todos_os_anos(self):
        """Alagoa Grande entregou 6/6 anos → qsiconfi = 1.0, contrib = 15."""
        result = calcular_qsiconfi(carregar_siconfi())
        mun = result[result["cod_ibge"] == "2500304"].iloc[0]
        assert mun["anos_entregues"]    == 6
        assert mun["qsiconfi"]          == 1.0
        assert mun["contrib_qsiconfi"]  == PESOS["qsiconfi"]

    def test_sem_rreo_aparece_com_zero_anos(self):
        """Água Branca nunca entregou RREO → aparece no resultado com
        anos_entregues=0 e contrib=0. A exclusão acontece no classifier
        (anos_entregues == 0 → ⚫ Sem Dados), não no scorer."""
        result = calcular_qsiconfi(carregar_siconfi())
        mun = result[result["cod_ibge"] == "2500106"].iloc[0]
        assert mun["anos_entregues"]   == 0
        assert mun["contrib_qsiconfi"] == 0.0

    def test_contrib_dentro_do_peso(self):
        """contrib_qsiconfi ∈ [0, 15]."""
        result = calcular_qsiconfi(carregar_siconfi())
        assert (result["contrib_qsiconfi"] >= 0).all()
        assert (result["contrib_qsiconfi"] <= PESOS["qsiconfi"]).all()


# ══════════════════════════════════════════════════════════════════════════════
# RPROC — Cronicidade de Restos a Pagar (peso 5)
# ══════════════════════════════════════════════════════════════════════════════

class TestRproc:

    # ── Tabela de pontuação ───────────────────────────────────────────────────

    def test_zero_anos_cronicos(self):
        assert pontuar_rproc_cronico(0) == 1.00

    def test_1_ano_cronico(self):
        assert pontuar_rproc_cronico(1) == 0.75

    def test_2_anos_cronicos(self):
        assert pontuar_rproc_cronico(2) == 0.50

    def test_3_anos_cronicos(self):
        assert pontuar_rproc_cronico(3) == 0.30

    def test_4_anos_cronicos(self):
        assert pontuar_rproc_cronico(4) == 0.10

    def test_5_ou_mais_retorna_zero(self):
        """5+ anos crônicos → 0.0 (também ativa cap no classifier)."""
        assert pontuar_rproc_cronico(5) == 0.00
        assert pontuar_rproc_cronico(6) == 0.00

    # ── Casos reais dos fixtures ──────────────────────────────────────────────

    def test_alagoa_grande_3_anos_cronicos(self):
        """Alagoa Grande: rproc_pct > 3% em 2020 (4.08), 2021 (3.89), 2022 (3.13)
        → n_anos_cronicos = 3."""
        result = calcular_rproc(carregar_siconfi())
        mun = result[result["cod_ibge"] == "2500304"].iloc[0]
        assert mun["n_anos_cronicos"] == 3

    def test_alagoinha_1_ano_cronico(self):
        """Alagoinha: só 2022 está acima de 3% (4.86) → n_anos_cronicos = 1."""
        result = calcular_rproc(carregar_siconfi())
        mun = result[result["cod_ibge"] == "2500502"].iloc[0]
        assert mun["n_anos_cronicos"] == 1

    def test_contrib_dentro_do_peso(self):
        """contrib_rproc ∈ [0, 5]."""
        result = calcular_rproc(carregar_siconfi())
        assert (result["contrib_rproc"] >= 0).all()
        assert (result["contrib_rproc"] <= PESOS["rproc"]).all()