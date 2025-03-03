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
),

abbr_region_states AS (
        SELECT 
            *,
            CASE 
                -- Região Norte
                WHEN state = 'Acre' THEN 'AC'
                WHEN state = 'Amapá' THEN 'AP'
                WHEN state = 'Amazonas' THEN 'AM'
                WHEN state = 'Pará' THEN 'PA'
                WHEN state = 'Rondônia' THEN 'RO'
                WHEN state = 'Roraima' THEN 'RR'
                WHEN state = 'Tocantins' THEN 'TO'
                
                -- Região Nordeste
                WHEN state = 'Alagoas' THEN 'AL'
                WHEN state = 'Bahia' THEN 'BA'
                WHEN state = 'Ceará' THEN 'CE'
                WHEN state = 'Maranhão' THEN 'MA'
                WHEN state = 'Paraíba' THEN 'PB'
                WHEN state = 'Pernambuco' THEN 'PE'
                WHEN state = 'Piauí' THEN 'PI'
                WHEN state = 'Rio Grande do Norte' THEN 'RN'
                WHEN state = 'Sergipe' THEN 'SE'

                -- Região Centro-Oeste
                WHEN state = 'Distrito Federal' THEN 'DF'
                WHEN state = 'Goiás' THEN 'GO'
                WHEN state = 'Mato Grosso' THEN 'MT'
                WHEN state = 'Mato Grosso do Sul' THEN 'MS'

                -- Região Sudeste
                WHEN state = 'Espírito Santo' THEN 'ES'
                WHEN state = 'Minas Gerais' THEN 'MG'
                WHEN state = 'Rio de Janeiro' THEN 'RJ'
                WHEN state = 'São Paulo' THEN 'SP'

                -- Região Sul
                WHEN state = 'Paraná' THEN 'PR'
                WHEN state = 'Rio Grande do Sul' THEN 'RS'
                WHEN state = 'Santa Catarina' THEN 'SC'

                ELSE 'Unknown'
            END AS state_abbr,

            CASE 
                WHEN state IN ('Acre', 'Amapá', 'Amazonas', 'Pará', 'Rondônia', 'Roraima', 'Tocantins') THEN 'Norte'
                WHEN state IN ('Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba', 'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe') THEN 'Nordeste'
                WHEN state IN ('Distrito Federal', 'Goiás', 'Mato Grosso', 'Mato Grosso do Sul') THEN 'Centro-Oeste'
                WHEN state IN ('Espírito Santo', 'Minas Gerais', 'Rio de Janeiro', 'São Paulo') THEN 'Sudeste'
                WHEN state IN ('Paraná', 'Rio Grande do Sul', 'Santa Catarina') THEN 'Sul'
                ELSE 'Unknown'
            END AS region
        FROM concat_address_cast_date
) SELECT * FROM abbr_region_states
