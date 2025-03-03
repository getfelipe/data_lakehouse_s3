WITH total_by_date AS (
    SELECT 
        order_date,
        COUNT(order_date) as qntde_orders,
        ROUND(SUM(total_value::numeric), 2) AS total_date
    FROM {{ref("silver_orders")}}
    GROUP BY order_date
    ORDER BY order_date
) SELECT * FROM total_by_date