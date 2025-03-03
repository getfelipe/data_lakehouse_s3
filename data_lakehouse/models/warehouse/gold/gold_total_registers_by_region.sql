WITH total_registers_by_region AS (
    SELECT 
        region,
        COUNT(cpf) as qntde_registers_region
    FROM {{ref("silver_registers")}}
    GROUP by region
) SELECT * FROM total_registers_by_region