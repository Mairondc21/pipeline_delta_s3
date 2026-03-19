WITH cte_base AS (
    SELECT
        cidade_transacao,
        tipo_transacao,
        COUNT(tipo_transacao) AS quantidade_transacao
    FROM
        {{ source('s3_silver', 'ft_transacao') }}
    WHERE status_transacao = 'CONCLUIDA'
    GROUP BY
        cidade_transacao,
        tipo_transacao
)

SELECT *
FROM
    cte_base
