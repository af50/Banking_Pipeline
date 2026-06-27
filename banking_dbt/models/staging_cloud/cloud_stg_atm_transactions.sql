{{ config(materialized='view', tags=['staging', 'cloud']) }}

with source as (
    select * from {{ source('silver_cloud', 'cards') }}
),

cleaned as (
    select
        cast(refnum as string)          as refnum,
        cast(termid as string)          as atm_id,
        cast(amount as double)          as amount_mad,
        cast(transaction_timestamp as timestamp) as transaction_datetime,
        cast(respcode as int)           as resp_code,
        case when cast(respcode as int) = 0
             then true else false end   as is_successful,
        _ingested_at                    as _silver_loaded_at
    from source
    where refnum is not null
      and cast(amount as double) >= 0
)

select * from cleaned 