WITH total_orders_by_customer AS (
    SELECT 
        sr.name,
        so.order_id,
        so.cpf,
        so.order_value,
        so.charges,
        so.discount_value,
        so.used_voucher,
        so.total_value,
        so.order_status,
        so.order_date
    FROM {{ref("silver_orders")}} AS so
    LEFT JOIN {{ref("silver_registers")}} AS sr
    ON sr.cpf = so.cpf
),


total_invoice_by_customer AS (
    SELECT 
        cpf, 
        COUNT(cpf) as qntde_purchased, 
        ROUND(SUM(total_value::numeric), 2) AS total_spent,
        ROUND((SUM(total_value::numeric) / COUNT(cpf)), 2) AS avg_ticket
    FROM total_orders_by_customer
    GROUP BY cpf
) 

SELECT * FROM total_invoice_by_customer