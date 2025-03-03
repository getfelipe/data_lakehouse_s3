{{
    config(
        materialized='table'
    )
}}


WITH raw_orders_query AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) AS n_id
    FROM {{ref('raw_orders')}}
    
),

filtered_raw_orders AS (
    SELECT
        order_id,
        cpf,
        CAST(order_value AS FLOAT) AS order_value,
        CAST(charges AS FLOAT) AS charges,
        CAST(discount_value AS FLOAT) AS discount_value,
        (order_value + charges - discount_value) AS total_value,
        CASE 
            WHEN voucher IS NOT NULL THEN 1
            ELSE 0
        END AS used_voucher,

        order_status,
        CAST(order_date AS DATE) AS order_date
    FROM raw_orders_query
    WHERE n_id = 1
)
SELECT * FROM filtered_raw_orders
