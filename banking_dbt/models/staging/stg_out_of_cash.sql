
-- staging/stg_out_of_cash.sql

{{ config(materialized='view', tags=['staging']) }}

WITH raw AS (
    SELECT * FROM read_parquet(
        'D:/NTI INTERNSHIP/Airflow/Banking_pipeline/local_warehouse/delta/silver/out_of_cash/**/*.parquet',
        hive_partitioning = true
    )
)

SELECT
    refnum                                      AS refnum,
    pan                                         AS pan_masked,
    termid                                      AS atm_id,
    CAST(transaction_date AS DATE)              AS transaction_date,
    CAST(transaction_hour AS INTEGER)           AS transaction_hour,
    CAST(attempted_amount_mad AS DOUBLE)        AS attempted_amount_mad,
    CAST(resp_code AS INTEGER)                  AS resp_code,
    COALESCE(failure_reason, 'GUICHET VIDE')    AS failure_reason,
    CAST(is_confirmed_ooc AS BOOLEAN)           AS is_confirmed_ooc,
    currency                                    AS currency,
    _silver_loaded_at
FROM raw
WHERE termid IS NOT NULL
