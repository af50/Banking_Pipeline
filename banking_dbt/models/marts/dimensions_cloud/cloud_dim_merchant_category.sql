{{ config(materialized='table', tags=['dimensions', 'cloud']) }}

with stg as (
    select distinct
        cast(mcc as string) as mcc_code
    from {{ ref('cloud_stg_transactions') }}
    where mcc is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['mcc_code']) }} as merchant_category_key,
    mcc_code,
    mcc_code                                              as mcc_description
from stg 