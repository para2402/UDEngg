import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName('Processing Seller Table') \
                    .getOrCreate()

RAW_DATA_BUCKET = sys.argv[1]
STAGING_DATA_BUCKET = sys.argv[2]

df = spark.read.csv(RAW_DATA_BUCKET + '/olist_sellers_dataset.csv',
                    inferSchema=True, header=True)

df.repartition(5) \
  .write \
  .parquet(STAGING_DATA_BUCKET + '/seller', mode='overwrite')