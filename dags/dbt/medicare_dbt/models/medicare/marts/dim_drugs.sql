-- Dimension table for drugs
{{ config(
    materialized='table'
) }}

WITH stg_spending AS (
    SELECT * FROM {{ ref('stg_medicare_spending') }}
),

-- Get unique drug combinations
unique_drugs AS (
    SELECT DISTINCT
        brand_name,
        generic_name,
        manufacturer_name,
        total_manufacturer,
        -- Get the latest data for each drug
        FIRST_VALUE(total_spending) OVER (
            PARTITION BY brand_name, generic_name, manufacturer_name 
            ORDER BY year DESC
        ) as latest_total_spending,
        FIRST_VALUE(year) OVER (
            PARTITION BY brand_name, generic_name, manufacturer_name 
            ORDER BY year DESC
        ) as latest_year
    FROM stg_spending
),

final AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY brand_name, generic_name) as drug_id,
        brand_name,
        generic_name,
        manufacturer_name,
        total_manufacturer,
        latest_total_spending,
        latest_year,
        CASE 
            WHEN brand_name = generic_name THEN TRUE 
            ELSE FALSE 
        END as is_generic,
        CURRENT_TIMESTAMP() as created_at
    FROM unique_drugs
)

SELECT * FROM final 