import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description:
        Process the songs metadata files and create `songs` and `artists` tables.
    
    :param spark: a spark session instance
    :param input_data: data files base path
    :param output_data: output files base path
    """
    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data).drop_duplicates()
    
    # extract columns to create songs table
    select_list = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(select_list).drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(path=output_data + 'songs/',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    select_list = ['artist_id', 'artist_name', 'artist_location',
                   'artist_latitude', 'artist_longitude']
    artists_table = df.select(select_list).drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(path=output_data + 'artists/',
                                mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Description:
            Process the event log files and create `time` , `users` and `songplays` tables.
    
    :param spark: a spark session instance
    :param input_data: data files base path
    :param output_data: output files base path
    """
    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    # read log data file
    df = spark.read.json(log_data).drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table
    select_list = ['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = df.select(select_list).drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(path=output_data + 'users/',
                              mode='overwrite')
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.utcfromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select("start_time") \
                   .drop_duplicates() \
                   .withColumn("hour", hour("start_time")) \
                   .withColumn("day", dayofmonth("start_time")) \
                   .withColumn("week", weekofyear("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .withColumn("year", year("start_time")) \
                   .withColumn("weekday", dayofweek("start_time"))
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(path=output_data + 'time/',
                             mode='overwrite',
                             partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/")
    
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner') \
                        .select("song_id", "artist_id", "start_time",
                                col("userId").alias("user_id"),
                                col("sessionId").alias("session_id"),
                                col("userAgent").alias("user_agent"),
                                "level", "location") \
                        .withColumn("songplay_id", monotonically_increasing_id()) \
                        .join(time_table, df.start_time == time_table.start_time, how='inner') \
                        .select("song_id", "artist_id", time_table.start_time,
                                "user_id", "session_id", "user_agent", "level",
                                "location", "year", "month") \
                        .drop_duplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays/",
                                  mode='overwrite',
                                  partitionBy=['year','month'])


def main():    
    spark = create_spark_session()
    input_data = "s3://udacity-c4-project/input/"
    output_data = "s3://udacity-c4-project/output/"
        
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
