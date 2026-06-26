{{ config(materialized='table', tags=['dimensions']) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_atm_master') }}
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['atm_id']) }}  AS atm_key,
        atm_id,
        region,
        atm_type,
        provider,
        location_type,
        installation_date,
        cash_limit_mad,
        is_cash_deposit_enabled,
        atm_age_years,
        CASE
            WHEN atm_age_years <= 2  THEN 'NEW'
            WHEN atm_age_years <= 5  THEN 'ESTABLISHED'
            ELSE 'MATURE'
        END                                                 AS atm_maturity,
        CASE
            WHEN cash_limit_mad >= 1000000 THEN 'HIGH_CAPACITY'
            WHEN cash_limit_mad >= 800000  THEN 'MEDIUM_CAPACITY'
            ELSE 'LOW_CAPACITY'
        END                                                 AS capacity_tier,
        country,
        bank_name,
        currency,
        _silver_loaded_at
    FROM stg
)

SELECT * FROM enriched
