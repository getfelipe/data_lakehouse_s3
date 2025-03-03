WITH status_by_region AS (
    SELECT
        sr.region,
        so.order_status,
        COUNT(order_status) AS status_by_region 
    
    FROM {{ref("silver_orders")}} AS so
    INNER JOIN {{ref("silver_registers")}} AS sr
        USING (cpf)
    GROUP BY (sr.region, so.order_status)
    ) SELECT * FROM status_by_region