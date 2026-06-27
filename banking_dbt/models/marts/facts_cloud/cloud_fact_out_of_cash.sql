{{ config(
    materialized='incremental',
    unique_key='event_key',
    incremental_strategy='merge',
    tags=['facts', 'cloud']
) }}

with stg as (
    select * from {{ ref('cloud_stg_out_of_cash') }}
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
        {{ dbt_utils.generate_surrogate_key(['stg.refnum']) }}  as event_key,
        coalesce(dd.date_key, 0)                                as date_key,
        coalesce(da.atm_key, 'unknown')                         as atm_key,
        stg.refnum,
        stg.atm_id,
        stg.requested_amount_mad,
        stg.transaction_datetime,
        stg.resp_code,
        stg._silver_loaded_at,
        current_timestamp()                                     as _loaded_at
    from stg
    left join dim_atm da
        on stg.atm_id = da.atm_id
    left join dim_date dd
        on cast(stg.transaction_datetime as date) = dd.full_date
)

select * from joined