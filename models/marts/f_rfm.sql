{{
    config (
      engine='MergeTree()',
      order_by=['rdate', 'user_id'],
      partition_by='toYear(rdate)'
    )
}}

 SELECT
         user_id,rdate,
         MAX(_recency) AS recency,
            CASE
			WHEN MAX(_recency) <=30 THEN 'месяц'
        	WHEN MAX(_recency) between 31 AND 61   THEN '2 месяца'
        	WHEN MAX(_recency) between 62 AND 92   THEN 'квартал'
        	WHEN MAX(_recency) between 93 AND 183  THEN 'полгода'
        	WHEN MAX(_recency) between 184 AND 365 THEN 'год'
        	WHEN MAX(_recency) between 366 AND 732 THEN '2 года'
        ELSE 'более 3х лет'
            END AS recency_segment,
        SUM(_monetary_value) AS monetary_value, 
            CASE
			WHEN SUM(_monetary_value) <=1000 THEN 'до 1000'
        	WHEN SUM(_monetary_value) between 1001 AND 5000   THEN '1-5 тыс'
        	WHEN SUM(_monetary_value) between 5001 AND 10000   THEN '5-10 тыс'
        	WHEN SUM(_monetary_value) between 10001 AND 30000  THEN '10-30 тыс'
        	WHEN SUM(_monetary_value) between 30001 AND 100000 THEN '30-100 тыс'
        	WHEN SUM(_monetary_value) between 100001 AND 500000 THEN '100-500 тыс'
        ELSE 'более 500 тыс'
        	END AS monetary_segment,
        SUM(_frequency) AS frequency,  
            CASE
			WHEN SUM(_frequency) <=1 THEN '1 чек'
        	WHEN SUM(_frequency) between 2 AND 5   THEN '5 чеков'
        	WHEN SUM(_frequency) between 6 AND 10   THEN '10 чеков'
        	WHEN SUM(_frequency) between 11 AND 30  THEN '30 чеков'
        	WHEN SUM(_frequency) between 31 AND 50 THEN '50 чеков'
        	WHEN SUM(_frequency) between 51 AND 100 THEN '100 чеков'
        ELSE 'более 100 чеков'
        	END AS frequency_segment,
        SUM(_shops) as shops,
        	  CASE
			WHEN SUM(_shops) = 1 THEN '1 магазин'
        	WHEN SUM(_shops) = 2  THEN '5 магазинов'
        	WHEN SUM(_shops) = 3  THEN '10 магазинов'
        	WHEN SUM(_shops) between 4 AND 6  THEN '30 магазинов'
        	WHEN SUM(_shops) between 7 AND 10 THEN '50 магазинов'
        	WHEN SUM(_shops) between 11 AND 15 THEN '100 магазинов'
        ELSE 'более 15 магазинов'
        	END AS shops_segment,
        SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value))  as rj_percent,
        	  CASE
			WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value))<=0.05 THEN 'до 5%'
        	WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value)) between 0.051 AND 0.1   THEN '5-10%'
        	WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value)) between 0.11 AND 0.25   THEN '10-25%'
        	WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value)) between 0.26 AND 0.5    THEN '25-50%'
        	WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value)) between 0.51 AND 0.75   THEN '50-75%'
        	WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value)) between 0.76 AND 0.9    THEN '75-90%'
        ELSE 'более 90%'
        	END AS rj_percent_segment,
           SUM(_ali_monetary_value)/SUM(_monetary_value) as ali_percent,
        	  CASE
			WHEN SUM(_ali_monetary_value)/SUM(_monetary_value)<=0.05 THEN 'до 5%'
        	WHEN SUM(_ali_monetary_value)/SUM(_monetary_value) between 0.051 AND 0.1   THEN '5-10%'
        	WHEN SUM(_ali_monetary_value)/SUM(_monetary_value) between 0.11 AND 0.25   THEN '10-25%'
        	WHEN SUM(_ali_monetary_value)/SUM(_monetary_value) between 0.26 AND 0.5    THEN '25-50%'
        	WHEN SUM(_ali_monetary_value)/SUM(_monetary_value) between 0.51 AND 0.75   THEN '50-75%'
        	WHEN SUM(_ali_monetary_value)/SUM(_monetary_value) between 0.76 AND 0.9    THEN '75-90%'
        ELSE 'более 90%'
        	END AS ali_percent_segment
          from int_rfm 
 group by user_id,rdate