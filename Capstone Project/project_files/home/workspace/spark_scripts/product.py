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

product_dim = spark.sql("""SELECT product_id,
                                  product_category_name_english                AS product_category,
                                  CAST(product_description_lenght AS SMALLINT) AS product_description_length,
                                  CAST(product_photos_qty AS SMALLINT)         AS product_photos_qty,
                                  CAST(product_weight_g AS SMALLINT)           AS product_weight_g,
                                  CAST(product_length_cm AS SMALLINT)          AS product_length_cm,
                                  CAST(product_height_cm AS SMALLINT)          AS product_height_cm,
                                  CAST(product_width_cm AS SMALLINT)           AS product_width_cm                                  
                           FROM product p
                           LEFT JOIN translation t
                                   ON p.product_category_name = t.product_category_name
                        """)

product_dim.repartition(5) \
           .write \
           .parquet(STAGING_DATA_BUCKET + '/product', mode='overwrite')