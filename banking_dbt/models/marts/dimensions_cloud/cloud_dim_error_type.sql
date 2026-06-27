{{ config(materialized='table', tags=['dimensions', 'cloud']) }}

select '0'  as response_code, 'Approved'           as error_description, 'Success'    as error_category union all
select '96' as response_code, 'Out of Cash'        as error_description, 'ATM Error'  as error_category union all
select '51' as response_code, 'Insufficient Funds' as error_description, 'User Error' as error_category union all
select '14' as response_code, 'Invalid Card'       as error_description, 'User Error' as error_category union all
select '05' as response_code, 'Do Not Honor'       as error_description, 'Bank Error' as error_category