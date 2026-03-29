WITH source AS (SELECT * FROM {{ source('raw', 'siconfi_rgf') }}),
renamed AS (
    SELECT
        CAST(cod_ibge       AS INT64)  AS cod_ibge,
        uf,
        instituicao,
        CAST(exercicio      AS INT64)  AS ano,
        periodo,
        periodicidade,
        co_poder,
        anexo,
        esfera,
        rotulo,
        coluna,
        cod_conta,
        conta,
        CAST(valor          AS FLOAT64) AS valor,
        CAST(populacao      AS INT64)   AS populacao
    FROM source
)
SELECT * FROM renamed