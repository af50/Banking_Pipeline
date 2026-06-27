{{ config(materialized='table', tags=['dimensions', 'cloud']) }}

with stg as (
    select * from {{ ref('cloud_stg_atm_master') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['atm_id']) }}  as atm_key,
        atm_id,
        region,
        atm_type,
        provider,
        location_type,
        installation_date,
        cash_limit_mad,
        case
            when cash_limit_mad >= 5000000 then 'HIGH_CAPACITY'
            when cash_limit_mad >= 4000000 then 'MEDIUM_CAPACITY'
            else 'LOW_CAPACITY'
        end                                                 as capacity_tier,
        _silver_loaded_at
    from stg
)

select * from enriched 