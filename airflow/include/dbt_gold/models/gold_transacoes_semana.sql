WITH cte_base AS (
    SELECT
        cliente_id,
        sk_date,
        valor
    FROM
        {{ source('s3_silver', 'ft_transacao') }}
),

cte_cliente_total_por_mes AS (
    SELECT
        cl.nome,
        SUM(bs.valor) AS total_transacao,
        CASE
            WHEN
                flag_fim_semana = true::text
                THEN 'Mais transações aos finais de semanas'
            ELSE 'Mais transações em dias de semana'
        END AS flag_fim_semana_2
    FROM
        cte_base AS bs
    INNER JOIN {{ source('s3_silver', 'dim_cliente') }} AS cl
        ON bs.cliente_id = cl.cliente_id AND flag_atual = true
    LEFT JOIN {{ source('s3_silver','dim_data') }} AS dt
        ON bs.sk_date = dt.sk_date
    GROUP BY
        cl.nome,
        flag_fim_semana
)

SELECT * FROM cte_cliente_total_por_mes
