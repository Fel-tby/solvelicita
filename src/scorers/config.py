# Sem imports de projeto — seguro importar de qualquer lugar sem sys.path
# Versão: 7.0 | Atualizado: Março/2026

# Pesos dos indicadores
PESOS = {
    "lliq"      : 35,  # v6.2: 30 → +5 (melhor preditor, AUC=0.691)
    "ccauc"     : 10,  # v6.2: 20 → -10 (AUC=0 no backtest; peso excessivo)
    "eorcam"    : 15,  # v6.2: 20 → -5 (AUC sub-aleatório como preditor isolado)
    "qsiconfi"  : 15,  # v6.2: 15 → sem alteração
    "autonomia" : 10,  # v6.2: 10 → sem alteração
    "rproc"     : 15,  # v6.2: 5  → +10 (AUC=0.609; 3× mais preditivo que eorcam)
}
assert sum(PESOS.values()) == 100, "Pesos não somam 100"

# Anos de referência
ANOS_REF = [2020, 2021, 2022, 2023, 2024, 2025]
N_ANOS   = len(ANOS_REF)

# Pesos de recência para Eorcam (2020 reservado como histórico, não entra na média)
PESOS_ANO = {
    2025: 0.40,
    2024: 0.25,
    2023: 0.20,
    2022: 0.10,
    2021: 0.05,
    2020: 0.00,
}

# Limiares de classificação (v7.0)
# v6.2: 75 / 55 / 35 → v7.0: 80 / 60 / 40
# Bandas uniformes de 20 pts. Classifier lê daqui — não hardcode nos scorers.
LIMIARES_SCORE = {
    "baixo" : 80,   # [80–100] 🟢 Risco Baixo
    "medio" : 60,   # [60– 79] 🟡 Risco Médio
    "alto"  : 40,   # [40– 59] 🔴 Risco Alto
                    # [ 0– 39] ⛔ Crítico
}

# Caps duros de classificação
# RPproc: teto rebaixado de ≥5 para ≥4 anos crônicos
N_ANOS_CRONICOS_CAP_MEDIO = 4  # v6.2: 5 → v7.0: 4 (≥4 anos → teto 🟡)

# Qsiconfi: mantidos inalterados da v6.2
# 3 anos entregues → teto 🟡 | ≤2 anos → teto 🔴 | 0 anos → ⚫ Sem Dados

# Limiares operacionais
LIMIAR_RPROC_CRONICO    = 3.0   # % da receita — ano acima = crônico (inalterado)
LIMIAR_AUTONOMIA_CRIT   = 0.08  # < 8% RCL = dependência crítica do FPM
LIMIAR_LLIQ_SUSPEITO    = -0.50 # abaixo = dado_suspeito
JANELA_RGF_BIMESTRAL    = 90    # dias — municípios > 50k hab
JANELA_RGF_SEMESTRAL    = 210   # dias — municípios ≤ 50k hab

# Pendências CAUC por gravidade
PENDENCIAS_GRAVES = {
    "Regularidade Fiscal (RFB)",
    "Regularidade PGFN",
    "CADIN",
    "SISTN (Dívida Consolidada)",
    "LRF - Limite Pessoal Executivo",
    "Adimplência TCU",
    "Adimplência CGU",
}

PENDENCIAS_MODERADAS = {
    "Regularidade FGTS",
    "Regularidade Trabalhista (TST)",
    "SIOPS (Saúde)",
    "SIOPE (Educação)",
    "SICONV/TRANSFEREGOV Prestação de Contas",
    "SISTN (Garantias)",
    "LRF - Limite Pessoal Legislativo",
}

# Mapa de fim de período RGF → mês de referência
# Usado pelo lliq_scorer para calcular dias de atraso do dado.
# Q = quadrimestral (municípios > 50k hab) | S = semestral (≤ 50k hab)
FIM_PERIODO_MES = {
    ("Q", 1): 4,
    ("Q", 2): 8,
    ("Q", 3): 12,
    ("S", 1): 6,
    ("S", 2): 12,
}

# Sigmoid de Autonomia — parâmetros default (calibrados para PB 2020–2024)
# Estrutura: porte → (mu, k)
# mu = mediana empírica do grupo | k = 2 / IQR empírico
# Rever anualmente após nova coleta DCA. Não alterar diretamente para novos
# estados — usar OVERRIDES_UF abaixo.
SIGMOID_AUTONOMIA_DEFAULT = {
    "micro"   : (0.0296, 98.6),
    "pequeno" : (0.0276, 77.9),
    "médio"   : (0.0318, 96.2),
    "grande"  : (0.0228, 306.2),
}

# Overrides por UF
# Estrutura:
# OVERRIDES_UF = {
#     "CE": {
#         "pesos": {"lliq": 33, "rproc": 17},        # substitui apenas o que mudar
#         "sigmoid_autonomia": {                       # substitui apenas o porte alterado
#             "micro": (0.0310, 95.0),
#         },
#     },
# }
# PB não tem override — usa os defaults calibrados acima.
# Novos estados: calibrar sigmoid_autonomia com dados reais antes de adicionar aqui.
OVERRIDES_UF: dict[str, dict] = {}


# Funções de acesso parametrizadas por UF
def get_pesos(uf: str) -> dict:
    """
    Retorna os pesos dos indicadores para a UF informada.
    Aplica override parcial sobre PESOS se definido em OVERRIDES_UF.
    A soma dos pesos resultantes não é validada aqui — overrides devem
    compensar internamente.
    """
    override = OVERRIDES_UF.get(uf.upper(), {}).get("pesos", {})
    return {**PESOS, **override}


def get_sigmoid_autonomia(uf: str, porte: str) -> tuple:
    """
    Retorna (mu, k) da sigmoid de Autonomia para a UF e porte informados.
    Fallback: parâmetros PB (SIGMOID_AUTONOMIA_DEFAULT) quando UF não tem override.
    """
    uf_overrides = OVERRIDES_UF.get(uf.upper(), {}).get("sigmoid_autonomia", {})
    return uf_overrides.get(porte, SIGMOID_AUTONOMIA_DEFAULT[porte])