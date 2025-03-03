WITH total_by_region AS ( 
    SELECT 
        sr.region,
        ROUND(SUM(so.total_value::numeric), 2) AS total_orders_region,
        count(so.order_id) AS qntde_customers_region
        FROM {{ ref("silver_registers") }} AS sr
        RIGHT JOIN {{ref("silver_orders")}} AS so 
            USING(cpf)
        GROUP BY region
) 
SELECT * FROM total_by_region
