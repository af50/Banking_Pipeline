{{ config(materialized='view', tags=['staging', 'cloud']) }}

with source as (
    select * from {{ source('silver_cloud', 'wallet') }}
),

cleaned as (
    select
        cast(transaction_id as string)      as transaction_id,
        cast(mobile_number as string)       as mobile_number,
        cast(term_id as string)             as atm_id,
        cast(transaction_date as timestamp) as transaction_datetime,
        trim(transaction_type)              as transaction_type,
        cast(transaction_amount as double)  as amount_mad,
        trim(status_code)                   as status_code,
        trim(status_description)            as status_description,
        _ingested_at                        as _silver_loaded_at
    from source
    where transaction_id is not null
)

select * from cleaned 