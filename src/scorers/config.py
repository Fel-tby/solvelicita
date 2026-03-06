# Sem imports de projeto — seguro importar de qualquer lugar sem sys.path

PESOS = {
    "lliq"      : 30,
    "ccauc"     : 20,
    "eorcam"    : 20,
    "qsiconfi"  : 15,
    "autonomia" : 10,
    "rproc"     :  5,
}
assert sum(PESOS.values()) == 100, "Pesos não somam 100"

ANOS_REF  = [2020, 2021, 2022, 2023, 2024, 2025]
N_ANOS    = len(ANOS_REF)

PESOS_ANO = {
    2025: 0.40, 2024: 0.25, 2023: 0.20,
    2022: 0.10, 2021: 0.05, 2020: 0.00,
}

LIMIAR_RPROC_CRONICO  = 3.0    # % da receita — anos acima = crônico
LIMIAR_AUTONOMIA_CRIT = 0.08   # < 8% RCL = dependência crítica do FPM
LIMIAR_LLIQ_SUSPEITO  = -0.50  # abaixo = dado_suspeito
JANELA_RGF_BIMESTRAL  = 90     # dias — municípios > 50k hab
JANELA_RGF_SEMESTRAL  = 210    # dias — municípios ≤ 50k hab

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
    ("Q", 1): 4, ("Q", 2): 8,  ("Q", 3): 12,
    ("S", 1): 6, ("S", 2): 12,
}
