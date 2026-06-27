{{ config(
    materialized='incremental',
    unique_key='atm_transaction_key',
    incremental_strategy='merge',
    tags=['facts', 'cloud']
) }}

with stg as (
    select * from {{ ref('cloud_stg_atm_transactions') }}
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

dim_error as (
    select response_code, error_description, error_category
    from {{ ref('cloud_dim_error_type') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['stg.refnum']) }}  as atm_transaction_key,
        coalesce(dd.date_key, 0)                                as date_key,
        coalesce(da.atm_key, 'unknown')                         as atm_key,
        stg.refnum,
        stg.atm_id,
        stg.amount_mad,
        stg.transaction_datetime,
        stg.resp_code,
        coalesce(de.error_description, 'Unknown')               as error_description,
        coalesce(de.error_category, 'Unknown')                  as error_category,
        stg.is_successful,
        stg._silver_loaded_at,
        current_timestamp()                                     as _loaded_at
    from stg
    left join dim_atm da
        on stg.atm_id = da.atm_id
    left join dim_date dd
        on cast(stg.transaction_datetime as date) = dd.full_date
    left join dim_error de
        on cast(stg.resp_code as string) = de.response_code
)

select * from joined 