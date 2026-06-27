{{ config(materialized='view', tags=['staging', 'cloud']) }}

with source as (
    select * from {{ source('silver_cloud', 'kaggle_transactions') }}
),

cleaned as (
    select
        cast(id as string)              as transaction_id,
        cast(client_id as string)       as client_id,
        cast(card_id as string)         as card_id,
        cast(date as timestamp)         as transaction_datetime,
        cast(date as date)              as transaction_date,
        cast(amount as double)          as amount_mad,
        trim(use_chip)                  as channel,
        cast(merchant_id as string)     as merchant_id,
        trim(merchant_city)             as merchant_city,
        trim(merchant_state)            as merchant_region,
        cast(mcc as string)             as mcc,
        cast(errors as string)          as errors,
        _ingested_at                    as _silver_loaded_at
    from source
    where id is not null
      and client_id is not null
)

select * from cleaned 