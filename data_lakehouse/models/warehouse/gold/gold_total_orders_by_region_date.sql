WITH total_orders_by_region_date AS (
    SELECT 
        sr.region,
        so.order_date,
        ROUND(SUM(total_value::numeric), 2),
        COUNT(order_id)
    FROM {{ref("silver_orders")}} AS so
    LEFT JOIN {{ref("silver_registers")}} AS sr
        USING(cpf)
    GROUP BY (sr.region, so.order_date)
)  SELECT * FROM total_orders_by_region_date