import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

log = config['S3']['LOG_DATA']
path =  log_json_path=config['S3']['LOG_JSONPATH']
song = config['S3']['SONG_DATA']
IAM_ROLE = ole_arn=config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS log_times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR(50),
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR(50),
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR(50),
    page VARCHAR(50),
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            VARCHAR,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time           TIMESTAMP sortkey ,
user_id              INTEGER,
level                VARCHAR,
song_id              VARCHAR,
artist_id            VARCHAR,
session_id           INTEGER,
location             VARCHAR,
user_agent           VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id INTEGER PRIMARY KEY distkey,
first_name      VARCHAR,
last_name       VARCHAR,
gender          VARCHAR,
level           VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id     VARCHAR PRIMARY KEY sortkey,
title       VARCHAR,
artist_id   VARCHAR distkey,
year        INTEGER,
duration    FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id          VARCHAR PRIMARY KEY distkey,
name               VARCHAR,
location           VARCHAR,
latitude           FLOAT,
longitude          FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS log_times
(
start_time    TIMESTAMP PRIMARY KEY sortkey distkey,
minute        INTEGER,
hour          INTEGER,
day           INTEGER,
week          INTEGER,
month         INTEGER,
year          INTEGER,
weekday       INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(log, IAM_ROLE, path)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(song,IAM_ROLE)

# inert Queries into TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(event.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                event.userId as user_id,
                event.level as level,
                songs.song_id as song_id,
                songs.artist_id as artist_id,
                event.sessionId as session_id,
                event.location as location,
                event.userAgent as user_agent
FROM staging_events event
JOIN staging_songs songs ON event.song = songs.title AND event.artist = songs.artist_name;
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
FROM staging_events
where userId IS NOT NULL; 
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM staging_songs 
WHERE song_id IS NOT NULL;;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs 
where artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO log_times(start_time,minute, hour, day, week, month, year, weekday)
SELECT DISTINCT(ts) as start_time,
                EXTRACT(minute from ts),
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
FROM staging_events 
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
