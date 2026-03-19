WITH cte_base AS (
    SELECT
        cartao_id,
        limite_total
    FROM
        {{ source('s3_silver', 'ft_transacao') }}
),

cte_cartao_limite AS (
    SELECT
        bs.bandeira,
        bs.limite_total,
        RANK() OVER (
            PARTITION BY bs.bandeira ORDER BY bs.limite_total DESC
        ) AS rn
    FROM
        (
            SELECT DISTINCT
                cartao_id,
                status_cartao,
                bandeira
            FROM {{ source('s3_silver', 'dim_cartao') }}
        ) AS ct
    INNER JOIN cte_base AS bs ON ct.cartao_id = bs.cartao_id
    WHERE ct.status_cartao = 'ATIVO'
)

SELECT *
FROM
    cte_cartao_limite
WHERE rn IN (1, 2)
