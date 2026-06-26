-- fact_atm_transactions.sql 1st trail meds going to be used for incremental load of fact table atm transactions .
{{ config(
    materialized='incremental',
    unique_key='atm_transaction_key',
    incremental_strategy='merge',
    tags=['facts']
) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_atm_transactions') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

pan_map AS (
    SELECT pan_masked, customer_key, card_key
    FROM {{ ref('pan_customer_map') }}
),

dim_atm AS (
    SELECT atm_key, atm_id FROM {{ ref('dim_atm') }}
),

dim_ch AS (
    SELECT channel_key, channel_code FROM {{ ref('dim_channel') }}
),

dim_err AS (
    SELECT error_type_key, CAST(error_code AS VARCHAR) AS error_code
    FROM {{ ref('dim_error_type') }}
),

dim_dt AS (
    SELECT date_key, full_date FROM {{ ref('dim_date') }}
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['stg.refnum']) }}
                                                AS atm_transaction_key,
        dt.date_key,
        pm.customer_key,
        pm.card_key,
        da.atm_key,
        dch.channel_key,
        derr.error_type_key,
        stg.refnum,
        stg.amount_mad,
        stg.transaction_date,
        stg.transaction_hour,
        stg.transaction_type,
        stg.msg_type,
        stg.resp_code,
        stg.is_successful,
        stg.is_reversal,
        stg.is_out_of_cash,
        stg.is_deposit,
        stg.client_id,
        stg.currency,
        stg._silver_loaded_at,
        CURRENT_TIMESTAMP                       AS _loaded_at
    FROM stg
    LEFT JOIN pan_map pm
        ON stg.refnum = pm.pan_masked
    LEFT JOIN dim_atm da
        ON stg.atm_id = da.atm_id
    LEFT JOIN dim_ch dch
        ON stg.channel = dch.channel_code
    LEFT JOIN dim_err derr
        ON CAST(stg.resp_code AS VARCHAR) = derr.error_code
    LEFT JOIN dim_dt dt
        ON stg.transaction_date = dt.full_date
)

SELECT * FROM joined
