import configparser
from datetime import datetime as DateTime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """ creates the spark session using aws package to load data from s3 and use apache spark in the code """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ gets data path as input_data and output path as output_data to create song and artists tables
        from song_data and saves them in the output path """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id' ,
                            'title',
                            'artist_id' ,
                            'year',
                            'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').format("parquet").save(output_data +                         'data/song.parquet')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                              'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').format("parquet").save(output_data + 'data/artist.parquet')


def process_log_data(spark, input_data, output_data):
    """ gets data path as input_data and output path as output_data to create users ,time and songplays tables
        from log_data and saves them in the output path """
    # get filepath to log data file
    log_data = os.path.join(input_data , 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.select('userId',
                             'firstName',
                             'lastName',
                             'gender',
                             'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'data/users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x : DateTime.fromtimestamp(x/1000),T.TimestampType())
    df = df.withColumn("timestamp",get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x: DateTime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("datetime",get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                            hour('timestamp').alias('hour'),
                            dayofmonth('timestamp').alias('dayofmonth'),
                            weekofyear('timestamp').alias('week'),
                            month('timestamp').alias('month'),
                            year('timestamp').alias('year'),
                            date_format(col("timestamp"), "E").alias('dayofweek')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + 'data/time.parquet')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data)
    song_log = song_df.join(df, (df.song == song_df.title) & 
                                (df.artist == song_df.artist_name) & 
                                (df.length == song_df.duration),"left")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_log.select(F.monotonically_increasing_id().alias('songplay_id'),                                                                           col('timestamp').alias('start_time'),
                                      col('userId').alias('user_id'),
                                      'level',
                                      'song_id',
                                      'artist_id',
                                      col('sessionId').alias('session_id'),
                                      'location',
                                      col('userAgent').alias('user_agent'),
                                      month('start_time').alias('month'),
                                      year('start_time').alias('year')).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').format("parquet").save(output_data + 'data/songplays.parquet')


def main():
    """ main function to load spark session, song_data and log_data """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacityprojectbucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
