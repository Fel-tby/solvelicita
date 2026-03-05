"""
Módulo analítico do SICONFI — v6.2
Processa RREO (Anexos 01 e 07) e RGF (Anexo 05) para gerar
os indicadores de entrada do solvency.py.

Mudanças v6.2 vs v6.1:
- Adiciona rproc_pct: RP Processados / receita_realizada (padrão crônico de calote)
"""

import duckdb
import pandas as pd
from pathlib import Path

BASE_DIR       = Path(__file__).resolve().parent.parent.parent
CSV_RREO       = BASE_DIR / "data" / "processed" / "siconfi_rreo_pb.csv"
CSV_RGF        = BASE_DIR / "data" / "processed" / "siconfi_rgf_pb.csv"
CSV_MUNICIPIOS = BASE_DIR / "data" / "processed" / "municipios_pb_tabela.csv"
OUT            = BASE_DIR / "data" / "processed" / "siconfi_indicadores_pb.csv"

con = duckdb.connect()

print("Executando motor analítico DuckDB...")

query = """
WITH anos AS (
    SELECT DISTINCT exercicio AS ano
    FROM read_csv_auto($csv_rreo)
),
malha_base AS (
    SELECT m.cod_ibge, m.ente AS instituicao, m.populacao, a.ano
    FROM read_csv_auto($csv_municipios) m
    CROSS JOIN anos a
),

ultimo_periodo_rreo AS (
    SELECT cod_ibge, exercicio AS ano, MAX(periodo) AS max_periodo
    FROM read_csv_auto($csv_rreo)
    GROUP BY cod_ibge, exercicio
),

dados_rreo AS (
    SELECT
        s.cod_ibge,
        s.exercicio AS ano,

        MAX(CASE
            WHEN s.anexo     = 'RREO-Anexo 01'
            AND  s.cod_conta = 'ReceitasExcetoIntraOrcamentarias'
            AND  s.coluna    = 'Até o Bimestre (c)'
            THEN s.valor END) AS receita_realizada,

        MAX(CASE
            WHEN s.anexo     = 'RREO-Anexo 01'
            AND  s.cod_conta = 'ReceitasExcetoIntraOrcamentarias'
            AND  s.coluna    = 'PREVISÃO ATUALIZADA (a)'
            THEN s.valor END) AS receita_prevista,

        MAX(CASE
            WHEN s.anexo     = 'RREO-Anexo 01'
            AND  s.cod_conta = 'TotalDespesas'
            AND  s.coluna    = 'DESPESAS LIQUIDADAS ATÉ O BIMESTRE (h)'
            THEN s.valor END) AS despesa_liquidada,

        -- RP Não Processados (Anexo 07) — para situacional e lliq_parcial
        MAX(CASE
            WHEN s.anexo     = 'RREO-Anexo 07'
            AND  s.cod_conta = 'RestosAPagarNaoProcessadosAPagar'
            AND  s.coluna    = 'Saldo k = (f + g) - (i + j)'
            AND  s.conta     = 'TOTAL (III) = (I + II)'
            THEN s.valor END) AS rrestos_nao_processados,

        -- RP Processados (Anexo 07) — padrão crônico de calote
        MAX(CASE
            WHEN s.anexo     = 'RREO-Anexo 07'
            AND  s.cod_conta = 'RestosAPagarProcessadosENaoProcessadosLiquidadosAPagar'
            AND  s.coluna    = 'Saldo e = (a+ b) - (c + d)'
            AND  s.conta     = 'TOTAL (III) = (I + II)'
            THEN s.valor END) AS rrestos_processados

    FROM read_csv_auto($csv_rreo) s
    JOIN ultimo_periodo_rreo up
      ON s.cod_ibge  = up.cod_ibge
     AND s.exercicio = up.ano
     AND s.periodo   = up.max_periodo
    GROUP BY s.cod_ibge, s.exercicio
),

-- RGF Anexo 05: prioridade Q sobre S, período mais recente por ano
ultimo_periodo_rgf AS (
    SELECT cod_ibge, exercicio AS ano, periodicidade, MAX(periodo) AS max_periodo
    FROM read_csv($csv_rgf, quote='"')
    WHERE anexo = 'RGF-Anexo 05'
    GROUP BY cod_ibge, exercicio, periodicidade
),
regime_prioritario AS (
    SELECT
        cod_ibge, ano, periodicidade, max_periodo,
        ROW_NUMBER() OVER (
            PARTITION BY cod_ibge, ano
            ORDER BY CASE periodicidade WHEN 'Q' THEN 1 WHEN 'S' THEN 2 ELSE 3 END
        ) AS prioridade
    FROM ultimo_periodo_rgf
),

dados_rgf AS (
    SELECT
        r.cod_ibge,
        r.exercicio              AS ano,
        rp.periodicidade         AS periodicidade_rgf,
        rp.max_periodo           AS periodo_rgf,

        -- DCL pós-RPNP (primário — v6.1)
        MAX(CASE
            WHEN r.cod_conta = 'DisponibilidadeDeCaixaLiquidaAposRP'
            AND  r.conta     = 'TOTAL (IV) = (I + II + III)'
            THEN r.valor END) AS dcl_apos_rp_total,

        -- Parcela RPPS dentro da DCL pós-RPNP (para exclusão)
        MAX(CASE
            WHEN r.cod_conta = 'DisponibilidadeDeCaixaLiquidaAposRP'
            AND  r.conta     = 'TOTAL DOS RECURSOS VINCULADOS AO RPPS (III)'
            THEN r.valor END) AS dcl_apos_rp_rpps,

        -- DCL pré-RPNP (fallback quando pós-RPNP ausente)
        MAX(CASE
            WHEN r.cod_conta = 'DisponibilidadeDeCaixaLiquida'
            AND  r.conta     = 'TOTAL (IV) = (I + II + III)'
            THEN r.valor END) AS dcl_pre_rp_total,

        -- Parcela RPPS dentro da DCL pré-RPNP
        MAX(CASE
            WHEN r.cod_conta = 'DisponibilidadeDeCaixaLiquida'
            AND  r.conta     = 'TOTAL DOS RECURSOS VINCULADOS AO RPPS (III)'
            THEN r.valor END) AS dcl_pre_rp_rpps

    FROM read_csv($csv_rgf, quote='"') r
    JOIN regime_prioritario rp
      ON r.cod_ibge      = rp.cod_ibge
     AND r.exercicio     = rp.ano
     AND r.periodicidade = rp.periodicidade
     AND r.periodo       = rp.max_periodo
     AND rp.prioridade   = 1
    WHERE r.anexo = 'RGF-Anexo 05'
    GROUP BY r.cod_ibge, r.exercicio, rp.periodicidade, rp.max_periodo
)

SELECT
    mb.cod_ibge, mb.instituicao, mb.ano, mb.populacao,
    dr.receita_prevista, dr.receita_realizada, dr.despesa_liquidada,
    dr.rrestos_nao_processados, dr.rrestos_processados,
    rg.dcl_apos_rp_total, rg.dcl_apos_rp_rpps,
    rg.dcl_pre_rp_total,  rg.dcl_pre_rp_rpps,
    rg.periodicidade_rgf, rg.periodo_rgf

FROM malha_base mb
LEFT JOIN dados_rreo dr ON mb.cod_ibge = dr.cod_ibge AND mb.ano = dr.ano
LEFT JOIN dados_rgf  rg ON mb.cod_ibge = rg.cod_ibge AND mb.ano = rg.ano
ORDER BY mb.instituicao, mb.ano
"""

df = con.execute(query, {
    "csv_rreo":       str(CSV_RREO),
    "csv_rgf":        str(CSV_RGF),
    "csv_municipios": str(CSV_MUNICIPIOS),
}).df()

# ── Se rrestos_processados vier todo NULL, inspecione com: ───────────────────
# con.execute("""
#     SELECT DISTINCT coluna, conta
#     FROM read_csv_auto('data/processed/siconfi_rreo_pb.csv')
#     WHERE anexo = 'RREO-Anexo 07'
#     AND   cod_conta = 'RestosAPagarProcessadosAPagar'
#     LIMIT 20
# """).df()
# ─────────────────────────────────────────────────────────────────────────────

print("Calculando indicadores...")

df["entregou_rreo"] = df["receita_prevista"].notna()

df["eorcam"] = df.apply(
    lambda r: round(r["receita_realizada"] / r["receita_prevista"] * 100, 2)
    if pd.notnull(r["receita_prevista"]) and r["receita_prevista"] > 0 else None,
    axis=1,
)

df["rrestos_nproc_pct"] = df.apply(
    lambda r: round(r["rrestos_nao_processados"] / r["receita_realizada"] * 100, 2)
    if pd.notnull(r["rrestos_nao_processados"]) and pd.notnull(r["receita_realizada"])
       and r["receita_realizada"] > 0 else None,
    axis=1,
)

df["rproc_pct"] = df.apply(
    lambda r: round(r["rrestos_processados"] / r["receita_realizada"] * 100, 2)
    if pd.notnull(r["rrestos_processados"]) and pd.notnull(r["receita_realizada"])
       and r["receita_realizada"] > 0 else None,
    axis=1,
)

df["deficit_pct"] = df.apply(
    lambda r: round((r["despesa_liquidada"] - r["receita_realizada"]) / r["receita_realizada"] * 100, 2)
    if pd.notnull(r["receita_realizada"]) and r["receita_realizada"] > 0 else None,
    axis=1,
)

# ── Lliq v6.1 ────────────────────────────────────────────────────────────────
# Primário : DCL pós-RPNP − RPPS / receita_realizada
# Fallback : DCL pré-RPNP − RPPS / receita_realizada  → lliq_parcial = True
def _calcular_lliq(row):
    rec = row["receita_realizada"]
    if pd.isna(rec) or rec <= 0:
        return None, None, False

    if pd.notnull(row["dcl_apos_rp_total"]):
        rpps = row["dcl_apos_rp_rpps"] if pd.notnull(row["dcl_apos_rp_rpps"]) else 0.0
        lliq_bruta = row["dcl_apos_rp_total"] - rpps
        return round(lliq_bruta / rec, 6), lliq_bruta, False

    if pd.notnull(row["dcl_pre_rp_total"]):
        rpps = row["dcl_pre_rp_rpps"] if pd.notnull(row["dcl_pre_rp_rpps"]) else 0.0
        lliq_bruta = row["dcl_pre_rp_total"] - rpps
        return round(lliq_bruta / rec, 6), lliq_bruta, True

    return None, None, False

resultado = df.apply(_calcular_lliq, axis=1, result_type="expand")
df["lliq"]         = resultado[0]
df["lliq_bruta"]   = resultado[1]
df["lliq_parcial"] = resultado[2]

df.to_csv(OUT, index=False, encoding="utf-8")

print(f"\n✅ Salvo: {OUT.name}")
print(f"   Malha total     : {len(df)} linhas")
print(f"   Municípios      : {df['cod_ibge'].nunique()}")
print(f"   Anos            : {sorted(df['ano'].unique())}")
print(f"   Com RREO        : {df['entregou_rreo'].sum()} linhas")
print(f"   Com lliq        : {df['lliq'].notna().sum()} linhas (primário pós-RPNP)")
print(f"   Com lliq parcial: {df['lliq_parcial'].sum()} linhas (fallback pré-RPNP)")
print(f"   Com rproc_pct   : {df['rproc_pct'].notna().sum()} linhas")
print(f"   Sem lliq        : {df['lliq'].isna().sum()} linhas")

# Diagnóstico rproc: se vier todo NULL, provavelmente o nome de coluna/conta está errado
if df["rproc_pct"].isna().all():
    print("\n⚠️  rproc_pct veio todo NULL — inspecione os nomes exatos com:")
    print("   python -c \"import duckdb, pandas as pd")
    print("   con = duckdb.connect()")
    print("   print(con.execute(\\\"SELECT DISTINCT coluna, conta")
    print("         FROM read_csv_auto('data/processed/siconfi_rreo_pb.csv')")
    print("         WHERE anexo = 'RREO-Anexo 07'")
    print("         AND cod_conta = 'RestosAPagarProcessadosAPagar'")
    print("         LIMIT 20\\\").df())\"")

print("\nAmostra — Patos (2510808) e Sousa (2516201):")
amostra = df[df["cod_ibge"].astype(str).isin(["2510808", "2516201"])]
print(amostra[[
    "ano", "instituicao", "eorcam", "rproc_pct", "rrestos_nproc_pct",
    "lliq", "lliq_parcial", "periodicidade_rgf", "periodo_rgf"
]].to_string(index=False))
