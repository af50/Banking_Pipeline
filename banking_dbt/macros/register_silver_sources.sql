
-- macros/register_silver_sources.sql
-- Registers Silver Delta/Parquet files as DuckDB views
-- Called once before dbt run via on-run-start hook

{% macro register_silver_sources() %}

  {% set silver_base = 'D:/NTI INTERNSHIP/Airflow/Banking_pipeline/local_warehouse/delta/silver' %}

  {% set tables = [
    'atm_master',
    'customers',
    'cards',
    'card_transactions',
    'wallet_transactions',
    'out_of_cash',
    'kaggle_transactions',
    'pan_customer_map',
  ] %}

  {% for table in tables %}
    {% set path = silver_base ~ '/' ~ table ~ '/**/*.parquet' %}
    {% call statement('register_' ~ table) %}
      CREATE OR REPLACE VIEW {{ schema }}.{{ table }} AS
      SELECT * FROM read_parquet('{{ path }}', hive_partitioning=true);
    {% endcall %}
    {{ log("Registered silver view: " ~ table, info=True) }}
  {% endfor %}

{% endmacro %}
