import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName('Generating Calender Dimension') \
                    .getOrCreate()

RAW_DATA_BUCKET = sys.argv[1]
STAGING_DATA_BUCKET = sys.argv[2]

df = spark.read.csv(RAW_DATA_BUCKET + '/olist_orders_dataset.csv',
                    inferSchema=True, header=True)
df.createOrReplaceTempView('order')


calender_dim = spark.sql("""SELECT DISTINCT CAST(order_purchase_timestamp AS DATE) as date
                            FROM order
                        """)

calender_dim.createOrReplaceTempView('calender')
calender_dim = spark.sql("""SELECT date,
                                   CAST(year(date) AS SMALLINT) AS year,
                                   CAST(month(date) AS SMALLINT) AS month,
                                   CAST(day(date) AS SMALLINT) AS day,
                                   CAST(dayofweek(date) AS SMALLINT) AS day_of_week
                            FROM calender
                            WHERE date IS NOT NULL
                         """)

calender_dim.write.parquet(STAGING_DATA_BUCKET + '/calender',
                           mode='overwrite')