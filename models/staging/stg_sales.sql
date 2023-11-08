{{
    config (
      engine='MergeTree()',
      order_by=['sale_date', 'user_id'],
      partition_by='toYear(sale_date)'
    )
}}

SELECT
        program_id,
        user_id,
        money_received,
        amount_rub,
        cashback_rub,
        commission_rub,
        sale_id,
        sale_date,
        review_date
FROM {{ source('dbgen', 'sales') }}
