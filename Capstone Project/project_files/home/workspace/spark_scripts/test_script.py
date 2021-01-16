from pyspark.sql import SparkSession


# spark = SparkSession.builder \
#                     .master('yarn') \
#                     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
#                     .getOrCreate()

spark = SparkSession.builder \
                    .getOrCreate()



customers_df = spark.read.csv('s3://jaipara-udacity-capstone/raw_data/olist_customers_dataset.csv',
                              inferSchema=True, header=True)



customers_df.write.parquet('s3://jaipara-udacity-capstone/staging_data/customer', mode='overwrite')