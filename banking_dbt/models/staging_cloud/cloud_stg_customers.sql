{{ config(materialized='view', tags=['staging', 'cloud']) }}

with source as (
    select * from {{ source('silver_cloud', 'users_data') }}
),

cleaned as (
    select
        cast(id as string)                  as client_id,
        cast(current_age as int)            as current_age,
        cast(retirement_age as int)         as retirement_age,
        cast(birth_year as int)             as birth_year,
        cast(birth_month as int)            as birth_month,
        upper(trim(gender))                 as gender,
        trim(address)                       as address,
        cast(latitude as double)            as latitude,
        cast(longitude as double)           as longitude,
        cast(per_capita_income as double)   as per_capita_income_mad,
        cast(yearly_income as double)       as yearly_income_mad,
        cast(total_debt as double)          as total_debt_mad,
        cast(credit_score as int)           as credit_score,
        cast(num_credit_cards as int)       as num_credit_cards,
        _ingested_at                        as _silver_loaded_at
    from source
    where id is not null
)

select * from cleaned