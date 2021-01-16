import sys, logging

from pyspark.sql import SparkSession


STAGING_DATA_BUCKET = sys.argv[2]
TABLE = sys.argv[3]

spark = SparkSession.builder \
                    .appName(f'Checking {TABLE.title()} Table') \
                    .getOrCreate()

df = spark.read.parquet(f'{STAGING_DATA_BUCKET}/{TABLE}')

if df.count() <=0:
    err_msg = f'{TABLE.title()} table is EMPTY!!'
    logging.error(err_msg)
    raise ValueError(err_msg)

logging.info(f'{TABLE.title()} table has {df.count()} rows.')