{{ config(
    materialized='incremental',
    unique_key='transaction_key',
    incremental_strategy='merge',
    tags=['facts', 'cloud']
) }}

with stg as (
    select * from {{ ref('cloud_stg_transactions') }}
    {% if is_incremental() %}
    where transaction_date > (select max(transaction_date) from {{ this }})
    {% endif %}
),

dim_customer as (
    select customer_key, client_id from {{ ref('cloud_dim_customer') }}
),

dim_card as (
    select card_key, card_id from {{ ref('cloud_dim_card') }}
),

dim_merchant as (
    select merchant_key, merchant_id from {{ ref('cloud_dim_merchant') }}
),

dim_date as (
    select date_key, full_date from {{ ref('cloud_dim_date') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['stg.transaction_id']) }} as transaction_key,
        coalesce(dd.date_key, 0)                                       as date_key,
        coalesce(dc.customer_key, 'unknown')                           as customer_key,
        coalesce(dcard.card_key, 'unknown')                            as card_key,
        coalesce(dm.merchant_key, 'unknown')                           as merchant_key,
        stg.transaction_id,
        stg.amount_mad,
        stg.channel,
        stg.merchant_city,
        stg.merchant_region,
        stg.mcc,
        stg.transaction_date,
        stg.transaction_datetime,
        stg.errors,
        stg._silver_loaded_at,
        current_timestamp()                                            as _loaded_at
    from stg
    left join dim_customer dc
        on stg.client_id = dc.client_id
    left join dim_card dcard
        on stg.card_id = dcard.card_id
    left join dim_merchant dm
        on stg.merchant_id = dm.merchant_id
    left join dim_date dd
        on stg.transaction_date = dd.full_date
)

select * from joined 