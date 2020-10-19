import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, IntegerType
import pandas as pd 
pd.set_option('max_colwidth',50)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['IAM_USER']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['IAM_USER']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """create spark session that connects to aws-s3"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """process song_data from s3 (udacity) and load it back to my s3 bucket
    * spark: spark session that connects to aws-s3
    * input_data: all json files in song_data directory (udacity) on s3
    * output_data: parquet files in my own s3 bucket
    """ 
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create artists table
    # remove duplicated rows
    # remove song_id ==null
    songs_table = df.select(['song_id','title','artist_id','year','duration'])\
                    .drop_duplicates(subset='song_id').dropna(subset='song_id')
    print ('songs_table:',songs_table.count(),'rows')
    songs_table.show(1)
   
    # write songs table to parquet files partitioned by year and artist
    # parquet: columnar format
    # 1. partitional by year and artist_id
    # 2. set output as a path + table name, overwrite if exists"""
    
    songs_table.write.partitionBy(['year','artist_id'])\
                .parquet(os.path.join(output_data,'songs'),'overwrite')

    # extract columns to create artists table
    # remove duplicated rows
    # remove artist_id ==null""" 
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])\
                    .drop_duplicates(subset='artist_id').dropna(subset='artist_id')
    print ('artists_table:',artists_table.count(),'rows')
    artists_table.show(1)
    
    # write artists table to parquet files
    # parquet: columnar format
    # 1. partitional by year and artist_id
    # 2. set output as a path + table name, overwrite if exists"""
    artists_table.write.parquet(os.path.join(output_data,'artists'),'overwrite')


def process_log_data(spark, input_data, output_data):
    """process log_data from s3 (udacity) and load it back to my s3 bucket
    * spark: spark session that connects to aws-s3
    * input_data: all json files in log_data directory (udacity) on s3
    * output_data: parquet files in my own s3 bucket
    """

    # get filepath to log data file
    log_data = output_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page']=='NextSong')

    # extract columns for users table    
    users_table = df.select(['userID', 'firstName', 'lastName', 'gender', 'level'])\
                .drop_duplicates(subset='userID').dropna(subset='userID')
    
    # write users table to parquet files
    users_table.write\
        .parquet(os.path.join(output_data,'users'),'overwrite')

    
    # convert millisecond to second
    # // is the integer division
    # IntegerType() makes sure it returns an int
    
    get_timestamp = udf(lambda x: x//1000, IntegerType())
    df = df.withColumn('timestamp',get_timestamp(df['ts']))
    
    
    # convert second, a big integer to timestamp type
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn('start_time',get_datetime(df['timestamp']))
    
    
    # convert the timestamp type to hour, day, ...
    # * ts = original data from log table, a big integer 
    #    so that we can join it with log table
    # * start_time = TIMESTAMP
    # * date_format(colname, 'EEEE') return weekday
    time_table = df.select(col('ts'),
                            col('start_time'),
                            hour('start_time').alias('hour'),
                            dayofmonth('start_time').alias('day'),
                            weekofyear('start_time').alias('week'),
                            month('start_time').alias('month'),
                            year('start_time').alias('year'),
                            date_format('start_time','EEEE').alias('weekday'))\
                    .drop_duplicates(subset='start_time').dropna(subset='start_time')

   
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month'])\
        .parquet(os.path.join(output_data,'time'),'overwrite')
    print ('time_table:',time_table.count(),'rows')
    time_table.show(1)

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs')
    print ('song_df:',song_df.count(),'rows')
    song_df.show(1)

    # create temp view of LOG and SONG and TIME tables to be joined
    df.createOrReplaceTempView('log')
    song_df.createOrReplaceTempView('song')
    time_table.createOrReplaceTempView('time')

    # extract columns from joined song and log datasets to create songplays table 
    # 1. left join LOG and SONG table to append artist and song to LOG
    # 2. left join LOG to time to append year and month to LOG
    songplays_table = spark.sql("""
                                SELECT l.userId AS user_id, l.level, s.song_id,s.artist_id,
                                l.sessionId, l.location, l.userAgent,
                                t.year, t.month
                                FROM log AS l
                                LEFT JOIN song AS s
                                ON (l.song=s.title) AND (l.artist=s.artist_id) AND (l.length=s.duration) AND (l.ts IS NOT NULL)
                                LEFT JOIN time AS t
                                ON l.ts=t.ts
                                """)
    
    # set songplay_id as serial
    songplays_table=songplays_table.withColumn('songplay_id',monotonically_increasing_id())
    
    print ('songplays_table:',songplays_table.count(),'rows')
    print (songplays_table.limit(3).toPandas())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year','month'])\
        .parquet(os.path.join(output_data,'songplays'),'overwrite')

def main():
    """process song and log data using the spark session
    spark, input_data, ouput_data needs to be defined
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-emr-resources-327442518701-us-west-2/datalakes-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
