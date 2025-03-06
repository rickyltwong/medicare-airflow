-- Fact table for medicare spending analysis
{{ config(
    materialized='table'
) }}

WITH stg_spending AS (
    SELECT * FROM {{ ref('stg_medicare_spending') }}
),

-- Add year-over-year calculations
spending_with_yoy AS (
    SELECT 
        *,
        -- Calculate year over year changes
        LAG(total_spending) OVER (
            PARTITION BY brand_name, generic_name, manufacturer_name 
            ORDER BY year
        ) as prev_year_spending,
        
        LAG(avg_spend_per_dosage_unit) OVER (
            PARTITION BY brand_name, generic_name, manufacturer_name 
            ORDER BY year
        ) as prev_year_avg_spend_per_unit
    FROM stg_spending
),

final AS (
    SELECT 
        -- Dimensions
        brand_name,
        generic_name,
        manufacturer_name,
        total_manufacturer,
        year,
        
        -- Metrics
        total_spending,
        total_dosage_units,
        total_claims,
        total_beneficiaries,
        avg_spend_per_dosage_unit,
        avg_spend_per_claim,
        avg_spend_per_beneficiary,
        
        -- Calculated fields
        CASE 
            WHEN prev_year_spending IS NULL THEN NULL
            ELSE (total_spending - prev_year_spending) / prev_year_spending 
        END as yoy_spending_change,
        
        CASE 
            WHEN prev_year_avg_spend_per_unit IS NULL THEN NULL
            ELSE (avg_spend_per_dosage_unit - prev_year_avg_spend_per_unit) / prev_year_avg_spend_per_unit 
        END as yoy_unit_price_change,
        
        -- Growth metrics
        cagr_avg_spend_per_dosage_unit_18_22,
        change_avg_spend_per_dosage_unit_21_22,
        
        -- Flags and metadata
        outlier_flag,
        loaded_at,
        
        -- Add derived flags
        CASE 
            WHEN total_spending > 1000000 THEN TRUE 
            ELSE FALSE 
        END as is_high_spend_drug,
        
        CASE 
            WHEN yoy_unit_price_change > 0.1 THEN TRUE 
            ELSE FALSE 
        END as significant_price_increase
    FROM spending_with_yoy
)

SELECT * FROM final 