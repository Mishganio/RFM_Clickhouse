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
			WHEN MAX(_recency) <=31 THEN '1| до 1 месяца'
        	WHEN MAX(_recency) between 31 AND 61    THEN '2| 1-2 месяца'
        	WHEN MAX(_recency) between 62 AND 122   THEN '3| 2-4 месяца'
        	WHEN MAX(_recency) between 123 AND 183  THEN '4| 4-6 месяцев'
        	WHEN MAX(_recency) between 184 AND 244  THEN '5| 6-8 месяцев'
        	WHEN MAX(_recency) between 245 AND 307  THEN '6| 8-10 месяцев'
        ELSE '7| более 10 месяцев'
            END AS recency_segment,
        SUM(_monetary_value) AS monetary_value, 
            CASE
			WHEN SUM(_monetary_value) <=1000 THEN '1| до 1000'
        	WHEN SUM(_monetary_value) between 1001 AND 5000   THEN '2| 1-5 тыс'
        	WHEN SUM(_monetary_value) between 5001 AND 10000   THEN '3| 5-10 тыс'
        	WHEN SUM(_monetary_value) between 10001 AND 30000  THEN '4| 10-30 тыс'
        	WHEN SUM(_monetary_value) between 30001 AND 100000 THEN '5| 30-100 тыс'
        	WHEN SUM(_monetary_value) between 100001 AND 500000 THEN '6| 100-500 тыс'
       	ELSE '7| более 500 тыс'
        	END AS monetary_segment,
        SUM(_frequency) AS frequency,  
            CASE
			WHEN SUM(_frequency) =1 THEN '1| 1 заказ'
        	WHEN SUM(_frequency) between 2 AND 5    THEN '2| 2-5 заказов'
        	WHEN SUM(_frequency) between 6 AND 10   THEN '3| 6-10 заказов'
        	WHEN SUM(_frequency) between 11 AND 30  THEN '4| 11-30 заказов'
        	WHEN SUM(_frequency) between 31 AND 50  THEN '5| 31-50 закозов'
        	WHEN SUM(_frequency) between 51 AND 100 THEN '6| 51-100 заказов'
        ELSE '7| более 100 заказов'
        	END AS frequency_segment,
        SUM(_shops) as shops,
        	  CASE
			WHEN SUM(_shops) = 1  THEN '1| 1 магазин'
        	WHEN SUM(_shops) = 2  THEN '2| 2 магазина'
        	WHEN SUM(_shops) = 3  THEN '3| 3 магазина'
        	WHEN SUM(_shops) between 4 AND 6  THEN '4| 4-6 магазинов'
        	WHEN SUM(_shops) between 7 AND 10 THEN '5| 7-10 магазинов'
        	WHEN SUM(_shops) between 11 AND 15 THEN '6| 11-15 магазинов'
        ELSE '7| более 15 магазинов'
        	END AS shops_segment,
        SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value))  as rj_percent,
        	  CASE
			WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value))<=0.05 THEN '1| до 5%'
        	WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value)) between 0.05001 AND 0.1    THEN '2| 5-10%'
        	WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value)) between 0.10001 AND 0.25   THEN '3| 10-25%'
        	WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value)) between 0.25001 AND 0.5    THEN '4| 25-50%'
        	WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value)) between 0.50001 AND 0.75   THEN '5| 50-75%'
        	WHEN SUM(_rj_monetary_value)/(SUM(_monetary_value)+SUM(_rj_monetary_value)) between 0.75001 AND 0.9    THEN '6| 75-90%'
        ELSE '7| более 90%'
        	END AS rj_percent_segment,
        SUM(_ali_monetary_value)/if(SUM(_monetary_value)>0,SUM(_monetary_value),1) as ali_percent,
        	  CASE
			WHEN SUM(_ali_monetary_value)/if(SUM(_monetary_value)>0,SUM(_monetary_value),1)<=0.05 THEN '1| до 5%'
        	WHEN SUM(_ali_monetary_value)/if(SUM(_monetary_value)>0,SUM(_monetary_value),1) between 0.05001 AND 0.1    THEN '2| 5-10%'
        	WHEN SUM(_ali_monetary_value)/if(SUM(_monetary_value)>0,SUM(_monetary_value),1) between 0.10001 AND 0.25   THEN '3| 10-25%'
        	WHEN SUM(_ali_monetary_value)/if(SUM(_monetary_value)>0,SUM(_monetary_value),1) between 0.25001 AND 0.5    THEN '4| 25-50%'
        	WHEN SUM(_ali_monetary_value)/if(SUM(_monetary_value)>0,SUM(_monetary_value),1) between 0.50001 AND 0.75   THEN '5| 50-75%'
        	WHEN SUM(_ali_monetary_value)/if(SUM(_monetary_value)>0,SUM(_monetary_value),1) between 0.75001 AND 0.9    THEN '6| 75-90%'
        ELSE '7| более 90%'
        	END AS ali_percent_segment
          from {{ ref('int_rfm') }}
 group by user_id,rdate
 having SUM(_monetary_value)>0 
    