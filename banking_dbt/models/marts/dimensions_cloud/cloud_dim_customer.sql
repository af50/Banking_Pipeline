{{ config(materialized='table', tags=['dimensions', 'cloud']) }}

with stg as (
    select * from {{ ref('cloud_stg_customers') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['client_id']) }}   as customer_key,
        client_id,
        current_age,
        retirement_age,
        birth_year,
        birth_month,
        gender,
        address,
        latitude,
        longitude,
        per_capita_income_mad,
        yearly_income_mad,
        total_debt_mad,
        credit_score,
        num_credit_cards,
        case
            when credit_score >= 750 then 'EXCELLENT'
            when credit_score >= 700 then 'GOOD'
            when credit_score >= 650 then 'FAIR'
            else 'POOR'
        end                                                     as credit_tier,
        case
            when num_credit_cards >= 5 then 'HEAVY_USER'
            when num_credit_cards >= 3 then 'MODERATE_USER'
            else 'LIGHT_USER'
        end                                                     as credit_usage_tier,
        _silver_loaded_at
    from stg
)

select * from enriched