import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Initializes global spark session object with relevant packages and AWS configs

    '''
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.5') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Extracts log data from S3 into separate songs and artists
    dataframes, transforms, and loads back into a different S3 bucket
    Args:
        spark : the current spark session
        input_data : input S3 bucket path containing .json files
        output_data : output S3 bucket path containing .parquet files
    '''
    song_data = input_data + 'song_data/*/*/*/*.json'
    df = spark.read.json(song_data)

    # songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).where(df['song_id'].isNotNull()).dropDuplicates()
            
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs-parquet/')

    # artists table
    artists_table = df.selectExpr(['artist_id', 'artist_name AS name', 'artist_location AS location', \
                           'artist_latitude AS latitude', 'artist_longitude AS longitude']).where(df['artist_id'].isNotNull())  
         
    artists_table.write.mode('overwrite').parquet(output_data + 'artists-parquet/') 

    
def process_log_data(spark, input_data, output_data):
    '''
    Extracts log data from S3 into separate users, time, and songplays
    dataframes, transforms, and loads back into a different S3 bucket
    Args:
        spark : the current spark session
        input_data : input S3 bucket path containing .json files
        output_data : output S3 bucket path containing .parquet files
    '''
    log_data = input_data + 'log_data/*/*/*.json'
    df = spark.read.json(log_data)
    df = df.filter(df.page == 'NextSong')

    # users table   
    users_table = df.selectExpr(['userId as user_id', 'firstName AS first_name', 
                             'lastName AS last_name', 'gender', 'level']).where(df['userId'].isNotNull()).dropDuplicates()
        
    users_table.write.mode('overwrite').parquet(output_data + 'users-parquet/')
    
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn('ts', get_timestamp(df.ts))

    get_datetime = udf(lambda x: str(x))
    df = df.withColumn('start_time', get_datetime(df.ts))
    
    # time table
    time_table = df.select('start_time') \
                    .withColumn('hour', hour(df.start_time)) \
                    .withColumn('day', dayofmonth(df.start_time)) \
                    .withColumn('week', weekofyear(df.start_time)) \
                    .withColumn('month', month(df.start_time)) \
                    .withColumn('year', year(df.start_time)) \
                    .withColumn('weekday', dayofweek(df.start_time))

    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time-parquet/')                   
                     
    # songplays table
    song_df = spark.read.json(song_data)
     
    songplays_table = (df.join(song_df, df.song == song_df.title, how="left")
                     .withColumn("songplay_id", monotonically_increasing_id())
                     .withColumn("year", year(df.start_time))
                     .withColumn("month", month(df.start_time))
                     .selectExpr("songplay_id", "start_time", "year", "month", "userId as user_id", "level", 
                                 "song_id", "artist_id", "sessionId as session_id", "location", 
                                 "userAgent as user_agent")) 
    
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays-parquet/')


def main():
    '''
    run create_spark_session(), process_song_data, process_log_data
    '''
    spark = create_spark_session()
    
    input_data = config['S3']['INPUT_DATA']
    output_data = config['S3']['OUTPUT_DATA']
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
    
