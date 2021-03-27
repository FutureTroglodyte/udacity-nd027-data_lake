# udacity-nd027-data_lake
Udacity Data Engeneering Nanodegree Program - My Submission of Project: Data Lake

## Summary

The goal of this project is to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from sets in Amazon S3 into five S3 parquet files with Apache Spark (via pyspark).

### Raw Data

The raw data is in Amazon S3 and contains

1. \*song_data\*.jsons ('artist_id', 'artist_latitude', 'artist_location',
       'artist_longitude', 'artist_name', 'duration', 'num_songs',
       'song_id', 'title', 'year') - a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/).
2. \*log_data\*.jsons ('artist', 'auth', 'firstName', 'gender', 'itemInSession',
       'lastName', 'length', 'level', 'location', 'method', 'page',
       'registration', 'sessionId', 'song', 'status', 'ts', 'userAgent',
       'userId') - [simulated](https://github.com/Interana/eventsim) activity logs from a music streaming app based on specified configurations.

### preliminaries

Make sure you have an AWS secret and access key and an own S3-Bucket as well. Then edit the `dl.cfg` file and replace the following `XXX` entries:

```
[AWS]
KEY=YOUR_AWS_KEY
SECRET=YOUR_AWS_SECRET
```

Afterwards edit `etl.py` and let `output_data` point to your S3 Bucket.

### Run ETL Pipeline - Define Fact and Dimension Tables for the Raw Data

Run `python3 etl.py` in a terminal to import the raw sets from Udacity's S3 above and store data in the following star schema:

#### A Fact Table

1. songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) - records in log data associated with song plays

#### Four Dimension Tables

1. users (user_id, first_name, last_name, gender, level) - users in the app
1. songs (song_id, title, artist_id, year, duration) - songs in music database
1. artists (artist_id, name, location, latitude, longitude) - artists in music database
1. time (start_time, hour, day, week, month, year, weekday) - timestamps of records in songplays broken down into specific units
        
## 1. Discuss the Purpose of this Database in the Context of the Startup, Sparkify, and their Analytical Goals.

The startup Sparkify is an audio streaming services provider. "As a freemium service, basic features are free with advertisements and limited control, while additional features, such as offline listening and commercial-free listening, are offered via paid subscriptions. Users can search for music based on artist, album, or genre, and can create, edit, and share playlists." (Taken from [https://en.wikipedia.org/wiki/Spotify](https://en.wikipedia.org/wiki/Spotify))

So the commercial goal is to get as many users as long as possible to use sparkify. In order to achieve this, sparkify must meet/exceed the users expectations and satisfy them. Ways of doing this are
- providing a huge database of artists and songs
- a fancy and usable (Browser, Mobile-App, Desktop-App) GUI
- a good search engine (Analytical goal!)
- a good recommendation system (Analytical goal!)

This database serves the needs of a good recommendation system: The favourite songs of user `X` can easily be extracted of our fact_table given their number_of_plays (by `X`). So we can put users in clusters based on their favourite songs. And if `X` likes the songs `a`, `b`, & `c` and there are other Users in (one of) his cluster(-s) which likes the songs `a`, `b`, `c`, & `d` he also might like song `d`. Let's recommend this song to him. He supposably enjoys that and thus enjoys sparkify.

## 2. State and Justify your Database Schema Design and ETL Pipeline.

### Database Schema Design

The denormalized fact table `songplays` provides most information sparkify needs for its basic analytical goals. Reading and aggregation is very fast and if we need additional information from the dimension tables
- `users`
- `artists`
- `songs` (missing genre here 'tho)
- `time`

the joins are very simple -> high readability.

### ETL Pipeline

The ETL Pipeline is very simple,
1. Raw data from S3 is loaded in staging tables `staging_events` and `staging_songs` in Spark.
1. Data from `staging_songs` is directly written into the dimension tables `songs` and `artists` and written to parquet files.
1. Data from `staging_events` is directly written into the dimension tables `users` and `time` and written to parquet files.
1. For the fact table `songplays` the staging_events are filtered by `page == 'Nextsong'`. Then both parquet files for `songs` and `artists` are read and joined by artist id, to get the artist name. After that, this dataframe is joined by `staging_events` on song title, artist name and song duration. The is joined by `time` table on `start_time` to append `year` and `month` before it eventually is written to a parquet file as well.
