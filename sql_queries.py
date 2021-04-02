import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist varchar,
        auth varchar,
        first_name varchar,
        gender varchar,
        item_in_session int,
        last_name varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        session_id int,
        song varchar,
        status int, 
        ts bigint,
        user_agent varchar,
        user_id int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar not null,
        title varchar,
        duration float,
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id int IDENTITY(1,1) primary key, 
        start_time  timestamp not null,
        user_id varchar not null,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int not null,
        location varchar,
        user_agent text 
        )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id int primary key,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar primary key,
        title varchar not null,
        artist_id varchar not null,
        year int,
        duration float
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar primary key,
        name varchar not null,
        location varchar,
        latitude float,
        longitude float
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp primary key,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    region 'us-west-2'
    format as json {};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role {}
    region 'us-west-2'
    json 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second' as start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e
    JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT user_id,first_name,last_name,gender, level
    FROM staging_events
    WHERE user_id IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
        
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time 
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(dow FROM start_time) AS weekday
    FROM staging_events;
 """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
