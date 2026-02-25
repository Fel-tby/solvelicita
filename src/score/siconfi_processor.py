"""
Módulo analítico responsável pelo processamento de matrizes financeiras do SICONFI.
Utiliza DuckDB para executar operações de malha (cross join) e agregação SQL, 
seguido por Pandas para cálculo vetorizado de indicadores de saúde fiscal.
"""

import duckdb
import pandas as pd
from pathlib import Path

# ── Configurações de Diretórios ───────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CSV_SICONFI    = BASE_DIR / "data" / "processed" / "siconfi_rreo_pb.csv"
CSV_MUNICIPIOS = BASE_DIR / "data" / "processed" / "municipios_pb_tabela.csv"
OUT            = BASE_DIR / "data" / "processed" / "siconfi_indicadores_pb.csv"

# Inicialização da instância in-memory do DuckDB
con = duckdb.connect()

print("Executando motor analítico DuckDB...")

# ── Processamento SQL (ETL) ───────────────────────────────────────────────────
query = """
    -- 1. Definição da malha cartesiana (Backbone): Todos os Municípios x Todos os Anos
    WITH anos AS (
        SELECT DISTINCT exercicio AS ano
        FROM read_csv_auto($csv_siconfi)
    ),
    malha_base AS (
        SELECT
            m.cod_ibge,
            m.ente AS instituicao,
            m.populacao,
            a.ano
        FROM read_csv_auto($csv_municipios) m
        CROSS JOIN anos a
    ),

    -- 2. Identificação do último período fiscal declarado por ente/ano
    ultimo_periodo AS (
        SELECT cod_ibge, exercicio AS ano, MAX(periodo) AS max_periodo
        FROM read_csv_auto($csv_siconfi)
        GROUP BY cod_ibge, exercicio
    ),

    -- 3. Pivotamento de métricas financeiras baseado no último período entregue
    dados_financeiros AS (
        SELECT
            s.cod_ibge,
            s.exercicio AS ano,

            -- Receitas (Anexo 01)
            MAX(CASE
                WHEN s.anexo = 'RREO-Anexo 01'
                AND s.cod_conta = 'ReceitasExcetoIntraOrcamentarias'
                AND s.coluna = 'Até o Bimestre (c)'
                THEN s.valor END) AS receita_realizada,

            MAX(CASE
                WHEN s.anexo = 'RREO-Anexo 01'
                AND s.cod_conta = 'ReceitasExcetoIntraOrcamentarias'
                AND s.coluna = 'PREVISÃO ATUALIZADA (a)'
                THEN s.valor END) AS receita_prevista,

            -- Despesas (Anexo 01)
            MAX(CASE
                WHEN s.anexo = 'RREO-Anexo 01'
                AND s.cod_conta = 'TotalDespesas'
                AND s.coluna = 'DESPESAS LIQUIDADAS ATÉ O BIMESTRE (h)'
                THEN s.valor END) AS despesa_liquidada,

            MAX(CASE
                WHEN s.anexo = 'RREO-Anexo 01'
                AND s.cod_conta = 'TotalDespesas'
                AND s.coluna = 'DOTAÇÃO ATUALIZADA (d)'
                THEN s.valor END) AS despesa_dotacao,

            -- Restos a Pagar Processados (Anexo 07)
            MAX(CASE
                WHEN s.anexo = 'RREO-Anexo 07'
                AND s.cod_conta = 'RestosAPagarProcessadosENaoProcessadosLiquidadosAPagar'
                AND s.coluna = 'Saldo e = (a+ b) - (c + d)'
                AND s.conta = 'TOTAL (III) = (I + II)'
                THEN s.valor END) AS restos_processados,

            -- Restos a Pagar Não Processados (Anexo 07)
            MAX(CASE
                WHEN s.anexo = 'RREO-Anexo 07'
                AND s.cod_conta = 'RestosAPagarNaoProcessadosAPagar'
                AND s.coluna = 'Saldo k = (f + g) - (i + j)'
                AND s.conta = 'TOTAL (III) = (I + II)'
                THEN s.valor END) AS restos_nao_processados,

            -- Validador de Saldo Total (Anexo 07)
            MAX(CASE
                WHEN s.anexo = 'RREO-Anexo 07'
                AND s.cod_conta = 'SaldoTotal'
                AND s.coluna = 'Saldo Total L = (e + k)'
                AND s.conta = 'TOTAL (III) = (I + II)'
                THEN s.valor END) AS restos_saldo_total

        FROM read_csv_auto($csv_siconfi) s
        JOIN ultimo_periodo up
            ON s.cod_ibge = up.cod_ibge
            AND s.exercicio = up.ano
            AND s.periodo = up.max_periodo
        GROUP BY s.cod_ibge, s.exercicio
    )

    -- 4. Fusão final assegurando a inclusão de entes omissos (NULL entries)
    SELECT
        mb.cod_ibge,
        mb.instituicao,
        mb.ano,
        mb.populacao,
        df.receita_prevista,
        df.receita_realizada,
        df.despesa_dotacao,
        df.despesa_liquidada,
        df.restos_processados,
        df.restos_nao_processados,
        df.restos_saldo_total
    FROM malha_base mb
    LEFT JOIN dados_financeiros df
        ON mb.cod_ibge = df.cod_ibge
        AND mb.ano = df.ano
    ORDER BY mb.instituicao, mb.ano
"""

df = con.execute(query, {
    "csv_siconfi":    str(CSV_SICONFI),
    "csv_municipios": str(CSV_MUNICIPIOS)
}).df()

print("Calculando indicadores...")

# ── Engenharia de Features (KPIs Fiscais) ─────────────────────────────────────

# Flag de conformidade de entrega do RREO
df["entregou_rreo"] = df["receita_prevista"].notna()

# Eficiência de Arrecadação (% de execução da receita prevista)
df["eorcam"] = df.apply(
    lambda r: round(r["receita_realizada"] / r["receita_prevista"] * 100, 2)
    if pd.notnull(r["receita_prevista"]) and r["receita_prevista"] > 0 else None,
    axis=1
)

# Pressão de Dívida de Curto Prazo (Restos Processados / Receita Realizada)
df["rrestos_proc_pct"] = df.apply(
    lambda r: round(r["restos_processados"] / r["receita_realizada"] * 100, 2)
    if pd.notnull(r["receita_realizada"]) and r["receita_realizada"] > 0 else None,
    axis=1
)

# Pressão de Compromissos Futuros (Restos Não Processados / Receita Realizada)
df["rrestos_nproc_pct"] = df.apply(
    lambda r: round(r["restos_nao_processados"] / r["receita_realizada"] * 100, 2)
    if pd.notnull(r["receita_realizada"]) and r["receita_realizada"] > 0 else None,
    axis=1
)

# Déficit de Execução (Despesa Liquidada vs Receita Realizada)
df["deficit_pct"] = df.apply(
    lambda r: round((r["despesa_liquidada"] - r["receita_realizada"]) / r["receita_realizada"] * 100, 2)
    if pd.notnull(r["receita_realizada"]) and r["receita_realizada"] > 0 else None,
    axis=1
)

# ── Exportação Final ──────────────────────────────────────────────────────────
df.to_csv(OUT, index=False, encoding="utf-8")

print(f"\n✅ Salvo: {OUT.name}")
print(f"   Malha total:           {len(df)} linhas")
print(f"   Municípios:            {df['cod_ibge'].nunique()}")
print(f"   Anos:                  {sorted(df['ano'].unique())}")
print(f"   Entregaram RREO:       {df['entregou_rreo'].sum()} linhas")
print(f"   Omissos (penalizar):   {(~df['entregou_rreo']).sum()} linhas")

print("\nAmostra — Sousa (PB):")
sousa = df[df["instituicao"].str.contains("Sousa", na=False)]
print(sousa[[
    "ano", "eorcam", "rrestos_proc_pct", "rrestos_nproc_pct", "deficit_pct", "entregou_rreo"
]].to_string(index=False))