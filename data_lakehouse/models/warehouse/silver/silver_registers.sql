{{
    config(
        materialized='table'
    )
}}


WITH raw_registers_unique AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf) AS n_cpf,
    ROW_NUMBER() OVER (PARTITION BY email ORDER BY email) AS n_email,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS n_id
    FROM {{ref('raw_registers')}}
),

filtered_registers AS (
    SELECT *
    FROM raw_registers_unique
    WHERE n_cpf = 1 AND n_email = 1 AND n_id = 1
),


concat_address_cast_date AS (
    SELECT
        id,
        name,
        CAST(birth_date AS DATE) AS birth_date,
        cpf,
        postal_code,
        country,
        city,
        state,
        CONCAT(CAST(address_street AS VARCHAR), ' ', CAST(address_number AS VARCHAR)) as full_address,
        gender,
        marital_status,
        phone,
        email,
        CAST(register_date AS DATE) AS register_date
    FROM filtered_registers
)
SELECT * FROM concat_address_cast_date