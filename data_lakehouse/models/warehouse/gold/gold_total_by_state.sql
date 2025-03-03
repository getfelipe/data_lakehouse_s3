WITH total_by_state AS (
    SELECT 
        state,
        ROUND(SUM(so.total_value::numeric), 2)

    FROM {{ref("silver_registers")}} sr
    RIGHT JOIN {{ref("silver_orders")}} so
    USING(cpf)
    GROUP BY state

) SELECT * FROM total_by_state