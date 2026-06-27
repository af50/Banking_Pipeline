{{ config(materialized='table', tags=['dimensions', 'cloud']) }}

with stg as (
    select distinct
        merchant_id,
        merchant_city,
        merchant_region,
        mcc
    from {{ ref('cloud_stg_transactions') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['merchant_id']) }} as merchant_key,
        merchant_id,
        merchant_city,
        merchant_region,
        cast(mcc as string)                                     as mcc_code,
        _silver_loaded_at
    from {{ ref('cloud_stg_transactions') }}
    qualify row_number() over (partition by merchant_id order by merchant_id) = 1
)

select * from enriched