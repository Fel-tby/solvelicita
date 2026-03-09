"""
Módulo analítico do SICONFI — v6.2
Processa RREO (Anexos 01 e 07) e RGF (Anexo 05) para gerar
os indicadores de entrada do solvency.py.

Input:
    raw/siconfi/siconfi_rreo_pb.csv   (produzido por collectors/siconfi.py)
    raw/siconfi/siconfi_rgf_pb.csv    (produzido por collectors/siconfi.py)
    processed/municipios_pb_tabela.csv

Output:
    processed/siconfi_indicadores_pb.csv

Mudanças v6.2 vs v6.1:
- Adiciona rproc_pct: RP Processados / receita_realizada (padrão crônico de calote)
- Filtra anos sem nenhum dado real entregue (evita linhas 100% NULL no score)

Rodar individualmente:
    python src/processors/siconfi_processor.py
"""

import duckdb
import pandas as pd
from pathlib import Path

BASE_DIR       = Path(__file__).resolve().parent.parent.parent
CSV_RREO       = BASE_DIR / "data" / "raw" / "siconfi" / "siconfi_rreo_pb.csv"
CSV_RGF        = BASE_DIR / "data" / "raw" / "siconfi" / "siconfi_rgf_pb.csv"
CSV_MUNICIPIOS = BASE_DIR / "data" / "processed" / "municipios_pb_tabela.csv"
OUT            = BASE_DIR / "data" / "processed" / "siconfi_indicadores_pb.csv"

QUERY = """
WITH
-- Apenas anos que têm pelo menos um registro real no RREO.
-- Isso evita que anos coletados mas sem dados (ex: 2026 ainda sem entregas)
-- gerem linhas 100% NULL no output e contaminem o score.
anos_com_dados AS (
    SELECT DISTINCT exercicio AS ano
    FROM read_csv_auto($csv_rreo)
    WHERE cod_conta IS NOT NULL
      AND valor    IS NOT NULL
),
malha_base AS (
    SELECT m.cod_ibge, m.ente AS instituicao, m.populacao, a.ano
    FROM read_csv_auto($csv_municipios) m
    CROSS JOIN anos_com_dados a
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

        MAX(CASE
            WHEN s.anexo     = 'RREO-Anexo 07'
            AND  s.cod_conta = 'RestosAPagarNaoProcessadosAPagar'
            AND  s.coluna    = 'Saldo k = (f + g) - (i + j)'
            AND  s.conta     = 'TOTAL (III) = (I + II)'
            THEN s.valor END) AS rrestos_nao_processados,

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
        r.exercicio          AS ano,
        rp.periodicidade     AS periodicidade_rgf,
        rp.max_periodo       AS periodo_rgf,

        MAX(CASE
            WHEN r.cod_conta = 'DisponibilidadeDeCaixaLiquidaAposRP'
            AND  r.conta     = 'TOTAL (IV) = (I + II + III)'
            THEN r.valor END) AS dcl_apos_rp_total,

        MAX(CASE
            WHEN r.cod_conta = 'DisponibilidadeDeCaixaLiquidaAposRP'
            AND  r.conta     = 'TOTAL DOS RECURSOS VINCULADOS AO RPPS (III)'
            THEN r.valor END) AS dcl_apos_rp_rpps,

        MAX(CASE
            WHEN r.cod_conta = 'DisponibilidadeDeCaixaLiquida'
            AND  r.conta     = 'TOTAL (IV) = (I + II + III)'
            THEN r.valor END) AS dcl_pre_rp_total,

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


def _filtrar_anos_sem_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove linhas onde o município não entregou absolutamente nenhum dado
    no ano — ou seja, todos os indicadores fiscais são NULL.

    Isso evita que anos coletados mas ainda sem entregas (ex: 2026 recém-iniciado)
    apareçam no output e gerem ruído no cálculo do score.

    Um município que entregou RREO mas não tem RGF ainda é mantido
    (tem receita_prevista, só não tem lliq) — isso é dado real, não ausência.
    """
    colunas_indicadores = [
        "receita_prevista", "receita_realizada", "despesa_liquidada",
        "rrestos_nao_processados", "rrestos_processados",
        "dcl_apos_rp_total", "dcl_pre_rp_total",
    ]
    cols_presentes = [c for c in colunas_indicadores if c in df.columns]

    # Linha "fantasma": todos os indicadores são NaN → município não entregou nada
    mask_fantasma = df[cols_presentes].isna().all(axis=1)
    n_removidas   = mask_fantasma.sum()

    if n_removidas:
        anos_afetados = sorted(df.loc[mask_fantasma, "ano"].unique())
        print(f"\n  🧹 {n_removidas} linhas sem dados removidas "
              f"(anos sem entregas: {anos_afetados})")

    return df[~mask_fantasma].reset_index(drop=True)


def run() -> pd.DataFrame:
    """
    Executa o motor analítico DuckDB e salva processed/siconfi_indicadores_pb.csv.
    Retorna o DataFrame com todos os indicadores calculados.
    """
    for path, label in [
        (CSV_RREO,       "RREO"),
        (CSV_RGF,        "RGF"),
        (CSV_MUNICIPIOS, "Municípios"),
    ]:
        if not path.exists():
            raise FileNotFoundError(
                f"Arquivo {label} não encontrado: {path}\n"
                "Execute primeiro: python src/collectors/siconfi.py"
            )

    print("Executando motor analítico DuckDB...")
    con = duckdb.connect()

    df = con.execute(QUERY, {
        "csv_rreo":       str(CSV_RREO),
        "csv_rgf":        str(CSV_RGF),
        "csv_municipios": str(CSV_MUNICIPIOS),
    }).df()

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

    # ── Lliq v6.1 ─────────────────────────────────────────────────────────────
    def _calcular_lliq(row):
        rec = row["receita_realizada"]
        if pd.isna(rec) or rec <= 0:
            return None, None, False

        if pd.notnull(row["dcl_apos_rp_total"]):
            rpps       = row["dcl_apos_rp_rpps"] if pd.notnull(row["dcl_apos_rp_rpps"]) else 0.0
            lliq_bruta = row["dcl_apos_rp_total"] - rpps
            return round(lliq_bruta / rec, 6), lliq_bruta, False

        if pd.notnull(row["dcl_pre_rp_total"]):
            rpps       = row["dcl_pre_rp_rpps"] if pd.notnull(row["dcl_pre_rp_rpps"]) else 0.0
            lliq_bruta = row["dcl_pre_rp_total"] - rpps
            return round(lliq_bruta / rec, 6), lliq_bruta, True

        return None, None, False

    resultado          = df.apply(_calcular_lliq, axis=1, result_type="expand")
    df["lliq"]         = resultado[0]
    df["lliq_bruta"]   = resultado[1]
    df["lliq_parcial"] = resultado[2]

    # ── Remove anos completamente sem dados ───────────────────────────────────
    df = _filtrar_anos_sem_dados(df)

    # ── Exportação ────────────────────────────────────────────────────────────
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

    if df["rproc_pct"].isna().all():
        print("\n⚠️  rproc_pct veio todo NULL — inspecione os nomes exatos com:")
        print("   python -c \"import duckdb")
        print("   con = duckdb.connect()")
        print("   print(con.execute(\\\"SELECT DISTINCT coluna, conta")
        print("         FROM read_csv_auto('data/raw/siconfi/siconfi_rreo_pb.csv')")
        print("         WHERE anexo = 'RREO-Anexo 07'")
        print("         AND cod_conta = 'RestosAPagarProcessadosAPagar'")
        print("         LIMIT 20\\\").df())\"")

    print("\nAmostra — Patos (2510808) e Sousa (2516201):")
    amostra = df[df["cod_ibge"].astype(str).isin(["2510808", "2516201"])]
    print(amostra[[
        "ano", "instituicao", "eorcam", "rproc_pct", "rrestos_nproc_pct",
        "lliq", "lliq_parcial", "periodicidade_rgf", "periodo_rgf"
    ]].to_string(index=False))

    return df


if __name__ == "__main__":
    run()