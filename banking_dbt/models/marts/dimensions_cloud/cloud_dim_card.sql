{{ config(materialized='table', tags=['dimensions', 'cloud']) }}

with stg as (
    select * from {{ ref('cloud_stg_cards') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['card_id']) }}     as card_key,
        card_id,
        client_id,
        {{ dbt_utils.generate_surrogate_key(['client_id']) }}   as customer_key,
        card_brand,
        card_type,
        has_chip,
        credit_limit_mad,
        acct_open_date,
        year_pin_last_changed,
        card_on_dark_web,
        case
            when card_on_dark_web then 'CRITICAL'
            else 'LOW'
        end                                                     as card_risk_level,
        _silver_loaded_at
    from stg
)

select * from enriched 