# Sem imports de projeto — seguro importar de qualquer lugar sem sys.path
# Versão: 7.0 | Atualizado: Março/2026

PESOS = {
    "lliq"      : 35,   # v6.2: 30 → +5  (melhor preditor, AUC=0.691)
    "ccauc"     : 10,   # v6.2: 20 → -10 (AUC=0 no backtest; peso excessivo)
    "eorcam"    : 15,   # v6.2: 20 → -5  (AUC sub-aleatório como preditor isolado)
    "qsiconfi"  : 15,   # v6.2: 15 → sem alteração
    "autonomia" : 10,   # v6.2: 10 → sem alteração
    "rproc"     : 15,   # v6.2:  5 → +10 (AUC=0.609; 3× mais preditivo que eorcam)
}
assert sum(PESOS.values()) == 100, "Pesos não somam 100"

ANOS_REF = [2020, 2021, 2022, 2023, 2024, 2025]
N_ANOS = len(ANOS_REF)

PESOS_ANO = {
    2025: 0.40, 2024: 0.25, 2023: 0.20,
    2022: 0.10, 2021: 0.05, 2020: 0.00,
}

# ── Limiares de classificação (v7.0) ─────────────────────────────────────────
# v6.2: 75 / 55 / 35  →  v7.0: 80 / 60 / 40
# Bandas uniformes de 20 pts. Classifier lê daqui — não hardcode nos scorers.
LIMIARES_SCORE = {
    "baixo" : 80,   # [80–100] 🟢 Risco Baixo
    "medio" : 60,   # [60– 79] 🟡 Risco Médio
    "alto"  : 40,   # [40– 59] 🔴 Risco Alto
                    # [ 0– 39] ⛔ Crítico
}

# ── Caps duros de classificação ───────────────────────────────────────────────
# RPproc: teto rebaixado de ≥5 para ≥4 anos crônicos
N_ANOS_CRONICOS_CAP_MEDIO = 4   # v6.2: 5 → v7.0: 4  (≥4 anos → teto 🟡)

# Qsiconfi: mantidos inalterados da v6.2
# 3 anos entregues → teto 🟡 | ≤2 anos → teto 🔴 | 0 anos → ⚫ Sem Dados

LIMIAR_RPROC_CRONICO   = 3.0    # % da receita — ano acima = crônico (inalterado)
LIMIAR_AUTONOMIA_CRIT  = 0.08   # < 8% RCL = dependência crítica do FPM
LIMIAR_LLIQ_SUSPEITO   = -0.50  # abaixo = dado_suspeito
JANELA_RGF_BIMESTRAL   = 90     # dias — municípios > 50k hab
JANELA_RGF_SEMESTRAL   = 210    # dias — municípios ≤ 50k hab

PENDENCIAS_GRAVES = {
    "Regularidade Fiscal (RFB)", "Regularidade PGFN", "CADIN",
    "SISTN (Dívida Consolidada)", "LRF - Limite Pessoal Executivo",
    "Adimplência TCU", "Adimplência CGU",
}
PENDENCIAS_MODERADAS = {
    "Regularidade FGTS", "Regularidade Trabalhista (TST)",
    "SIOPS (Saúde)", "SIOPE (Educação)",
    "SICONV/TRANSFEREGOV Prestação de Contas",
    "SISTN (Garantias)", "LRF - Limite Pessoal Legislativo",
}

FIM_PERIODO_MES = {
    ("Q", 1): 4, ("Q", 2): 8, ("Q", 3): 12,
    ("S", 1): 6, ("S", 2): 12,
}
