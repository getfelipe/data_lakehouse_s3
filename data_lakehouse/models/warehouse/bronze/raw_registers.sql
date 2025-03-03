WITH raw_registers AS (
    SELECT * 
    FROM {{ source('transactions_dvj2', 'registers') }}  -- Correct macro 'source'
)

SELECT 

    id,
    name,
    birth_date,
    cpf,
    postal_code,
    country,
    city,
    state,
    address_street,
    address_number,
    gender,
    marital_status,
    phone,
    email,
    register_date

FROM raw_registers
