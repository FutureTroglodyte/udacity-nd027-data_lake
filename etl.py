import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Fill Dimension tables `songs` and `artists` with S3 songs data.
    
    1. Raw data from S3 is loaded in staging table `staging_songs` in Spark.
    2. Data from `staging_songs` is directly written into the dimension tables `songs` and `artists` and written to parquet files.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    df.createOrReplaceTempView("staging_songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
    """)

    # write songs table to parquet files partitioned by year and artist
    # Thanks to Prateek G's answer in https://knowledge.udacity.com/questions/195077
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(
        output_data + "songs.parquet"
    )

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT
            artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
    """)

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Fill Dimension tables `users` and `time` and fact_table `songplays`
    with S3 events data.
    
    1. Raw data from S3 is loaded in staging tables `staging_events` and
        `staging_songs` in Spark.
    2. Data from `staging_events` is directly written into the dimension tables `users`
        and `time` and written to parquet files.
    3. For the fact table `songplays` the staging_events are filtered by `page == 'Nextsong'`.
        Then both parquet files for `songs` and `artists` are read and joined by artist id, to
        get the artist name. After that, this dataframe is joined by `staging_events` on song title,
        artist name and song duration. The is joined by `time` table on `start_time` to append `year`
        and `month` before it eventually is written to a parquet file as well. 
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    df.createOrReplaceTempView("staging_events")

    # filter by actions for song plays
    df = spark.sql("""
        SELECT DISTINCT *
        FROM staging_events
        WHERE page = 'NextSong'
    """)

    # extract columns for users table
    users_table = spark.sql("""
        SELECT DISTINCT
            userId AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender,
            level
        FROM staging_events
        WHERE userId IS NOT NULL
    """)

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000.0, FloatType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    # Thanks to Eugenico C's answer in https://knowledge.udacity.com/questions/40684
    get_datetime = udf(lambda x: datetime.fromtimestamp((x / 1000.0)), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
        df.timestamp.alias("start_time"),
        "datetime",
        hour(df.datetime).alias("hour"),
        dayofmonth(df.datetime).alias("day"),
        weekofyear(df.datetime).alias("week_of_year"),
        month(df.datetime).alias("month"),
        year(df.datetime).alias("year"),
        dayofweek(df.datetime).alias("weekday"),
    ).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + "songs.parquet")
    artists_df = spark.read.parquet(output_data + "artists.parquet")
    songs_artists_df = songs_df.join(
        other=artists_df, on="artist_id", how="inner"
    ).select(
        col("artist_id"),
        col("song_id"),
        col("title"),
        col("name"),
        col("duration"),
    )

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(
        other=songs_artists_df,
        on=[
            df.song == songs_artists_df.title,
            df.artist == songs_artists_df.name,
            df.length == songs_artists_df.duration,
        ],
        how="inner",
    ).select(
        col("timestamp").alias("start_time"),
        col("userId").alias("user_id"),
        col("level"),
        col("song_id"),
        col("artist_id"),
        col("sessionId").alias("session_id"),
        col("location"),
        col("userAgent").alias("user_agent"),
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.join(
        other=time_table, on="start_time", how="inner"
    ).drop(*("datetime", "hour", "day", "week_of_year", "weekday"))
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(
        output_data + "songplays.parquet"
    )


def main():
    """
    Fill Dimension tables `songs`, `artists`, `users`, and `time` and fact_table
    `songplays` with S3 songs and events data.
    
    1. Raw data from S3 is loaded in staging table `staging_songs` in Spark.
    2. Data from `staging_songs` is directly written into the dimension tables `songs` and `artists` and written to parquet files.
    3. Raw data from S3 is loaded in staging tables `staging_events` and
        `staging_songs` in Spark.
    4. Data from `staging_events` is directly written into the dimension tables `users`
        and `time` and written to parquet files.
    5. For the fact table `songplays` the staging_events are filtered by `page == 'Nextsong'`.
        Then both parquet files for `songs` and `artists` are read and joined by artist id, to
        get the artist name. After that, this dataframe is joined by `staging_events` on song title,
        artist name and song duration. The is joined by `time` table on `start_time` to append `year`
        and `month` before it eventually is written to a parquet file as well. 
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    print("\nProcessing songs data")
    process_song_data(spark, input_data, output_data)
    print("Done!\n")
    print("\nProcessing log data")
    process_log_data(spark, input_data, output_data)
    print("Done!\n")


if __name__ == "__main__":
    main()
