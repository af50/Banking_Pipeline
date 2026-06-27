{{ config(materialized='view', tags=['staging', 'cloud']) }}

with source as (
    select * from {{ source('silver_cloud', 'out_of_cash') }}
),

cleaned as (
    select
        cast(pan as string)             as card_pan,
        cast(refnum as string)          as refnum,
        cast(termid as string)          as atm_id,
        cast(transaction_timestamp as timestamp) as transaction_datetime,
        cast(amount as double)          as requested_amount_mad,
        cast(respcode as int)           as resp_code,
        _ingested_at                    as _silver_loaded_at
    from source
    where refnum is not null
)

select * from cleaned 