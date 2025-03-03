WITH raw_orders AS (
    SELECT * 
    FROM {{source('transactions_dvj2', 'orders')}}
)

SELECT 
    order_id,
    cpf,
    order_value,
    charges,
    discount_value,
    voucher,
    order_status,
    order_date

FROM raw_orders