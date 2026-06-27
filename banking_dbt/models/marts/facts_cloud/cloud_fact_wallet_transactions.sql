{{ config(
    materialized='incremental',
    unique_key='transaction_key',
    incremental_strategy='merge',
    tags=['facts', 'cloud']
) }}

with stg as (
    select * from {{ ref('cloud_stg_wallet_transactions') }}
    {% if is_incremental() %}
    where transaction_datetime > (select max(transaction_datetime) from {{ this }})
    {% endif %}
),

dim_atm as (
    select atm_key, atm_id from {{ ref('cloud_dim_atm') }}
),

dim_date as (
    select date_key, full_date from {{ ref('cloud_dim_date') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['stg.transaction_id']) }} as transaction_key,
        coalesce(dd.date_key, 0)                                       as date_key,
        coalesce(da.atm_key, 'unknown')                                as atm_key,
        stg.transaction_id,
        stg.mobile_number,
        stg.atm_id,
        stg.transaction_type,
        stg.amount_mad,
        stg.status_code,
        stg.status_description,
        stg.transaction_datetime,
        stg._silver_loaded_at,
        current_timestamp()                                            as _loaded_at
    from stg
    left join dim_atm da
        on stg.atm_id = da.atm_id
    left join dim_date dd
        on cast(stg.transaction_datetime as date) = dd.full_date
)

select * from joined