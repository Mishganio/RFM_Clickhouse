{{
    config (
      engine='MergeTree()',
      order_by=['rdate', 'user_id'],
      partition_by='toYear(rdate)'
    )
}}

SELECT
user_id,toDate32('2023-10-01')as rdate,
DATEDIFF('day',MAX(sale_date),toDate32('2023-10-01')) AS recency,
COUNT(DISTINCT sale_id) AS frequency,
SUM(amount_rub) AS monetary_value,
CASE
WHEN DATEDIFF('day',MAX(sale_date),toDate32('2023-10-01')) <=180 THEN 'Active'
        WHEN DATEDIFF('day',MAX(sale_date),toDate32('2023-10-01')) <= 360 THEN 'Potential'
        ELSE 'Inactive'
    END AS segment
FROM {{ ref('stg_sales') }}
   where 
     sale_date between '2022-10-01' and '2023-10-01'
GROUP BY
    user_id   

    
