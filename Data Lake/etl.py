import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofweek, dayofmonth, hour, weekofyear, date_format 
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType


'''
Get AWS login information from dl.cfg file
'''
    
config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    '''
    Extract data from source to destination, make songs and artists table.
    
    spark: Start spark session
    input_data: Load the data from source S3 Bucket. For short data loading time, only using a part of the dataset:   s3://udacity-  dend/song_data/A/A/A
    output_data: Save the data to destination S3 Bucket.
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(path=output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(path=output_data + "artist")



def process_log_data(spark, input_data, output_data):
    '''
    Extract data from source to destination, make songplays, users, time tables 
    spark: Start spark session
    input_data: Load the data from source S3 Bucket.
    output_data: Save the data to destination S3 Bucket.
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    users_table = log_df.select("userId","firstName","lastName","gender","level").distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(path=output_data + "users")

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x/1000), TimestampType())
    log_df = log_df.withColumn("start_time", get_datetime(log_df.ts))
    
    # extract columns to create time table
    time_table =  log_df.withColumn("hour", hour("start_time")) \
                    .withColumn("day", dayofmonth("start_time")) \
                    .withColumn("week", weekofyear("start_time")) \
                    .withColumn("month", month("start_time")) \
                    .withColumn("year", year("start_time")) \
                    .withColumn("weekday", dayofweek("start_time")) \
                    .select("ts","start_time","hour","day","month","week","year","weekday").distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(path=output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data,"songs/*/*/"))
    artist_df = spark.read.parquet(os.path.join(output_data, "artist"))

    #join 4 dim table to make fact table
    song_log = log_df.join(song_df, log_df.song == song_df.title, how = "inner")
    artist_song = song_log.join(artist_df, artist_df.artist_name == song_log.artist)
    songplays_table = artist_song.join(time_table,artist_song.ts == time_table.ts, how = "inner")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_table.select(
                        monotonically_increasing_id().alias("songplay_id"),
                        col("start_time"),
                        col("userID").alias("user_id"),
                        "level",
                        "song_id",
                        "artist_id",
                        col("sessionID").alias("session_id"),
                        "location",
                        col("userAgent").alias("user_agent"),
                        col("year"),
                        col("month")
                                               )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data, 'songplays')


def main():
    '''
    Create new session for Spark cluster
    var input_data: Source of data
    var output_data: Destination of data
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
