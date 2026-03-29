WITH source AS (SELECT * FROM {{ source('raw', 'pncp') }}),
renamed AS (
    SELECT
        numerocontrolepncp                          AS numero_controle_pncp,
        uf,
        CAST(anocompra      AS INT64)               AS ano_compra,
        CAST(mes            AS INT64)               AS mes,
        modalidadenome                              AS modalidade_nome,
        mododisputanome                             AS modo_disputa_nome,
        orgaoentidade                               AS orgao_entidade,
        unidadeorgao                                AS unidade_orgao,
        objetocompra                                AS objeto_compra,
        situacaocompranome                          AS situacao_compra_nome,
        CAST(valortotalestimado   AS FLOAT64)       AS valor_total_estimado,
        CAST(valortotalhomologado AS FLOAT64)       AS valor_total_homologado,
        srp,
        PARSE_DATE('%Y-%m-%d',
            SUBSTR(datapublicacaopncp, 1, 10))      AS data_publicacao_pncp,
        PARSE_DATE('%Y-%m-%d',
            SUBSTR(dataatualizacaoglobal, 1, 10))   AS data_atualizacao,
        sem_dados
    FROM source
    WHERE sem_dados IS NULL OR sem_dados != 'True'
)
SELECT * FROM renamed