{{ config(materialized='view', tags=['staging', 'cloud']) }}

with source as (
    select * from {{ source('silver_cloud', 'cards_data') }}
),

cleaned as (
    select
        cast(id as string)             as card_id,
        cast(client_id as string)      as client_id,
        upper(trim(card_brand))        as card_brand,
        upper(trim(card_type))         as card_type,
        coalesce(has_chip, false)      as has_chip,
        cast(credit_limit as double)   as credit_limit_mad,
        acct_open_date,
        year_pin_last_changed,
        coalesce(card_on_dark_web, false) as card_on_dark_web,
        _ingested_at                   as _silver_loaded_at
    from source
    where id is not null
      and client_id is not null
)

select * from cleaned 