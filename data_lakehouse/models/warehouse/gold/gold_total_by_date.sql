WITH total_by_date AS (
    SELECT 
        order_date,
        COUNT(order_date) as qntde_orders,
        ROUND(SUM(total_value::numeric), 2) AS total_date
    FROM {{ref("silver_orders")}}
    WHERE order_status = 'invoiced'
    GROUP BY order_date
    ORDER BY order_date DESC
) SELECT * FROM total_by_date