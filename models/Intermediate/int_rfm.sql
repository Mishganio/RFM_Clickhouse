{{
    config (
      engine='MergeTree()',
      order_by=['rdate', 'user_id'],
      partition_by='toYear(rdate)'
    )
}}

SELECT
user_id,toDate('{{ var('dt') }}')as rdate,
DATEDIFF(day,MAX(sale_date),toDate('{{ var('dt') }}')) AS _recency,
COUNT(DISTINCT program_id) as _shops,
COUNT(sale_id) AS _frequency,
SUM(amount_rub) AS _monetary_value,
0 as _rj_frequency,0 as _rj_monetary_value,0 as _ali_monetary_value
FROM {{ ref('stg_sales') }}
   where money_received!=2 and amount_rub>0 and  user_id>0 and
     sale_date between DATEADD(day,-365,toDate('{{ var('dt') }}')) AND toDate('{{ var('dt') }}')
GROUP BY
    user_id   

UNION ALL
    
SELECT
user_id, toDate('{{ var('dt') }}') as rdate,
0,0,0,0,
COUNT(sale_id) AS _rj_frequency,
SUM(amount_rub) AS _rj_monetary_value,0
FROM {{ ref('stg_sales') }}
   where money_received=2 and amount_rub>0 and user_id>0 and
     sale_date between  DATEADD(day,-365,toDate('{{ var('dt') }}')) AND toDate('{{ var('dt') }}')
GROUP BY
    user_id

UNION ALL    

SELECT
user_id, toDate('{{ var('dt') }}') as rdate,
0,0,0,0,0,0,
SUM(amount_rub) AS _ali_monetary_value
FROM {{ ref('stg_sales') }}
   where money_received!=2 and amount_rub>0 and program_id in (72,12439) and user_id>0 and
     sale_date between  DATEADD(day,-365,toDate('{{ var('dt') }}')) AND toDate('{{ var('dt') }}')
GROUP BY
    user_id       