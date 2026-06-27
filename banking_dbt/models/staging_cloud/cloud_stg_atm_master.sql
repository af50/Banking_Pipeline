{{ config(materialized='view', tags=['staging', 'cloud']) }}

with source as (
    select * from {{ source('silver_cloud', 'atm_master') }}
),

cleaned as (
    select
        terminal_id        as atm_id,
        governorate        as region,
        type               as atm_type,
        replenished_by     as provider,
        replenished_from   as location_type,
        installation_date,
        cast(limits as double) as cash_limit_mad,
        _ingested_at       as _silver_loaded_at
    from source
    where terminal_id is not null
)

select * from cleaned