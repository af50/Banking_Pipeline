version: 2

models:
  - name: fact_atm_transactions
    description: "ATM card transaction fact table — incremental merge on transaction_date"
    columns:
      - name: atm_transaction_key
        description: "Surrogate key (MD5 of refnum)"
        tests: [not_null, unique]
      - name: refnum
        tests: [not_null]
      - name: atm_key
        tests: [not_null]
      - name: date_key
        tests: [not_null]
      - name: amount_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_successful
        tests: [not_null]
      - name: is_reversal
        tests: [not_null]
      - name: is_deposit
        tests: [not_null]
      - name: is_out_of_cash
        tests: [not_null]
      - name: transaction_date
        tests: [not_null]

  - name: fact_card_transactions
    description: "Kaggle card transaction fact table with fraud labels — incremental merge on transaction_date"
    columns:
      - name: card_transaction_key
        description: "Surrogate key (MD5 of transaction_id)"
        tests: [not_null, unique]
      - name: transaction_id
        tests: [not_null]
      - name: customer_key
        tests: [not_null]
      - name: card_key
        tests: [not_null]
      - name: date_key
        tests: [not_null]
      - name: amount_mad
        tests:
          - not_null
      - name: amount_abs
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_fraud
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      - name: transaction_date
        tests: [not_null]

  - name: fact_wallet_transactions
    description: "Mobile wallet transaction fact table — incremental merge on transaction_date"
    columns:
      - name: wallet_transaction_key
        description: "Surrogate key (MD5 of transaction_id)"
        tests: [not_null, unique]
      - name: transaction_id
        tests: [not_null]
      - name: date_key
        tests: [not_null]
      - name: amount_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_successful
        tests: [not_null]
      - name: transaction_date
        tests: [not_null]

  - name: fact_out_of_cash_events
    description: "ATM out-of-cash failure event fact table — incremental merge on transaction_date"
    columns:
      - name: out_of_cash_key
        description: "Surrogate key (MD5 of refnum)"
        tests: [not_null, unique]
      - name: refnum
        tests: [not_null]
      - name: atm_key
        tests: [not_null]
      - name: date_key
        tests: [not_null]
      - name: attempted_amount_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_confirmed_ooc
        tests: [not_null]
      - name: transaction_date
        tests: [not_null]
