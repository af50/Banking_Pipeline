
{{ config(materialized='view', tags=['staging']) }}

WITH raw AS (
    SELECT * FROM read_parquet(
        'D:/NTI INTERNSHIP/Airflow/Banking_pipeline/local_warehouse/delta/silver/atm_master/**/*.parquet',
        hive_partitioning = true
    )
),

cleaned AS (
    SELECT
        terminal_id                             AS atm_id,
        region                                  AS region,
        atm_type                                AS atm_type,
        provider                                AS provider,
        location_type                           AS location_type,
        installation_date                       AS installation_date,
        CAST(cash_limit AS DOUBLE)              AS cash_limit_mad,
        is_cash_deposit_enabled                 AS is_cash_deposit_enabled,
        atm_age_years                           AS atm_age_years,
        country                                 AS country,
        bank_name                               AS bank_name,
        currency                                AS currency,
        _silver_loaded_at                       AS _silver_loaded_at
    FROM raw
    WHERE terminal_id IS NOT NULL
)

SELECT * FROM cleaned
