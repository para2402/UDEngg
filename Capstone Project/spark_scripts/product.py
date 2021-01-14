import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName('Processing Product Table') \
                    .getOrCreate()

RAW_DATA_BUCKET = sys.argv[1]
STAGING_DATA_BUCKET = sys.argv[2]

df = spark.read.csv(RAW_DATA_BUCKET + '/olist_products_dataset.csv',
                    inferSchema=True, header=True)
category_translation = spark.read.csv(RAW_DATA_BUCKET + '/product_category_name_translation.csv',
                                      inferSchema=True, header=True)

df.createOrReplaceTempView('product')
category_translation.createOrReplaceTempView('translation')

product_dim = spark.sql("""SELECT p.*, t.product_category_name_english AS product_category
                           FROM product p
                           INNER JOIN translation t
                                   ON p.product_category_name = t.product_category_name
                        """) \
                   .drop('product_category_name', 'product_name_lenght') \
                   .withColumnRenamed('product_description_lenght',
                                      'product_description_length') ## correct the column name "lenght" to "length"
product_dim.createOrReplaceTempView('product')

product_dim.repartition(5) \
           .write \
           .parquet(STAGING_DATA_BUCKET + '/product', mode='overwrite')