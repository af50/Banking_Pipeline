{{ config(materialized='table', tags=['dimensions', 'cloud']) }}

select 'ATM Card'      as channel_name, 'Physical card at ATM'   as channel_description union all
select 'Mobile Wallet' as channel_name, 'Mobile wallet cash out' as channel_description union all
select 'Swipe'         as channel_name, 'Card swipe at merchant' as channel_description union all
select 'Chip'          as channel_name, 'Chip card at merchant'  as channel_description union all
select 'Online'        as channel_name, 'Online transaction'     as channel_description