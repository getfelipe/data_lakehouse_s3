WITH group_by_state_status AS (
    SELECT
       sr.state,
        so.order_status,
        COUNT(so.order_status)
    FROM {{ref("silver_orders")}} so
    LEFT JOIN {{ref("silver_registers")}} sr 
    USING(cpf)

    GROUP BY (state, order_status)
) SELECT * FROM group_by_state_status