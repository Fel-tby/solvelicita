WITH source AS (SELECT * FROM {{ source('raw', 'dca') }}),
renamed AS (
    SELECT
        CAST(cod_ibge       AS INT64)   AS cod_ibge,
        uf,
        ente                            AS municipio,
        CAST(ano            AS INT64)   AS ano,
        CAST(rec_tributaria AS FLOAT64) AS rec_tributaria,
        CAST(rec_corrente   AS FLOAT64) AS rec_corrente,
        CAST(ativo_financeiro  AS FLOAT64) AS ativo_financeiro,
        CAST(passivo_financeiro AS FLOAT64) AS passivo_financeiro,
        CAST(bp_disponivel  AS FLOAT64) AS bp_disponivel,
        CAST(rec_disponivel AS FLOAT64) AS rec_disponivel,
        CAST(populacao      AS INT64)   AS populacao
    FROM source
)
SELECT * FROM renamed