import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName('Generating order_item Fact Table') \
                    .getOrCreate()

RAW_DATA_BUCKET = sys.argv[1]
STAGING_DATA_BUCKET = sys.argv[2]


orders_df = spark.read.csv(RAW_DATA_BUCKET + '/olist_orders_dataset.csv',
                           inferSchema=True, header=True)
order_items_df = spark.read.csv(RAW_DATA_BUCKET + '/olist_order_items_dataset.csv',
                                inferSchema=True, header=True)
payments_df = spark.read.csv(RAW_DATA_BUCKET + '/olist_order_payments_dataset.csv',
                             inferSchema=True, header=True)
reviews_df = spark.read.csv(RAW_DATA_BUCKET + '/olist_order_reviews_dataset.csv',
                            inferSchema=True, header=True)

orders_df.createOrReplaceTempView('order')
order_items_df.createOrReplaceTempView('order_item')
payments_df.createOrReplaceTempView('payment')
reviews_df.createOrReplaceTempView('review')

order_item_fact = spark.sql("""SELECT monotonically_increasing_id() AS order_item_id,
                                      o.order_id,                                           ----> Retaining Business Key for analysis
                                      o.customer_id, i.product_id, i.seller_id,
                                      CAST(o.order_purchase_timestamp AS DATE)              AS purchase_date,
                                      CAST(year(o.order_purchase_timestamp) AS SMALLINT)    AS purchase_year,
                                      CAST(month(o.order_purchase_timestamp) AS SMALLINT)   AS purchase_month,
                                      o.order_purchase_timestamp,
                                      o.order_approved_at                                   AS order_approved_timestamp,
                                      o.order_delivered_carrier_date                        AS order_delivered_carrier_timestamp,
                                      o.order_delivered_customer_date                       AS order_delivered_customer_timestamp,
                                      o.order_estimated_delivery_date                       AS order_estimated_delivery_timestamp,
                                      o.order_status,
                                      i.shipping_limit_date,
                                      
                                      ----------------------------- MEASURES -----------------------------
                                      
                                      CAST(i.freight_value AS DECIMAL(5, 2))                AS freight_value,
                                      CAST(i.price AS DECIMAL(10, 2))                       AS unit_price,
                                      CAST(i.qty AS SMALLINT)                               AS qty,
                                      CAST(ROUND(i.qty * i.price, 2) AS DECIMAL(10, 2))     AS total_product_price,
                                      
                                      (SELECT CAST(SUM(p.payment_value) AS DECIMAL(10, 2))
                                       FROM payment AS p
                                       WHERE p.order_id = o.order_id)                       AS total_order_price,
                                       
                                      (SELECT CAST(ROUND(AVG(r.review_score), 1) AS DECIMAL(2, 1))
                                       FROM review AS r
                                       WHERE r.order_id = o.order_id)                       AS avg_review_score
                               FROM order AS o
                               INNER JOIN (SELECT order_id, product_id, seller_id,
                                                  shipping_limit_date, price, freight_value,
                                                  COUNT(order_item_id) AS qty
                                           FROM order_item
                                           GROUP BY order_id, product_id, seller_id,
                                                    shipping_limit_date, price, freight_value) AS i
                                       ON o.order_id = i.order_id
                          """)


order_item_fact.repartition(10) \
               .write \
               .parquet(STAGING_DATA_BUCKET + '/order_item',
                        mode='overwrite')