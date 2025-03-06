-- Staging model to clean and standardize raw data
{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('medicare', 'ext_raw_spending') }}
),

cleaned AS (
    SELECT
        -- Standardize text fields
        TRIM(UPPER(brand_name)) as brand_name,
        TRIM(UPPER(generic_name)) as generic_name,
        TRIM(UPPER(manufacturer_name)) as manufacturer_name,
        TRIM(UPPER(total_manufacturer)) as total_manufacturer,
        
        -- Convert year to integer
        CAST(year AS INTEGER) as year,
        
        -- Clean numeric fields (handle nulls and negatives)
        NULLIF(total_spending, 0) as total_spending,
        NULLIF(total_dosage_units, 0) as total_dosage_units,
        NULLIF(total_claims, 0) as total_claims,
        NULLIF(total_beneficiaries, 0) as total_beneficiaries,
        
        -- Clean average metrics
        CASE 
            WHEN avg_spend_per_dosage_unit < 0 THEN NULL 
            ELSE avg_spend_per_dosage_unit 
        END as avg_spend_per_dosage_unit,
        
        CASE 
            WHEN avg_spend_per_claim < 0 THEN NULL 
            ELSE avg_spend_per_claim 
        END as avg_spend_per_claim,
        
        CASE 
            WHEN avg_spend_per_beneficiary < 0 THEN NULL 
            ELSE avg_spend_per_beneficiary 
        END as avg_spend_per_beneficiary,
        
        -- Growth metrics
        cagr_avg_spend_per_dosage_unit_18_22,
        change_avg_spend_per_dosage_unit_21_22,
        
        -- Flag fields
        outlier_flag,
        
        -- Add metadata
        CURRENT_TIMESTAMP() as loaded_at
    FROM source
)

SELECT * FROM cleaned 