{{ config(materialized='table', tags=['dimensions', 'cloud']) }}

with date_spine as (
    select explode(sequence(
        to_date('2010-01-01'),
        to_date('2027-12-31'),
        interval 1 day
    )) as full_date
)

select
    cast(date_format(full_date, 'yyyyMMdd') as int) as date_key,
    full_date,
    day(full_date)                                   as day,
    month(full_date)                                 as month,
    date_format(full_date, 'MMMM')                  as month_name,
    quarter(full_date)                               as quarter,
    year(full_date)                                  as year,
    weekofyear(full_date)                            as week_of_year,
    date_format(full_date, 'EEEE')                  as day_of_week,
    case when dayofweek(full_date) in (1,7)
         then true else false end                    as is_weekend
from date_spine 