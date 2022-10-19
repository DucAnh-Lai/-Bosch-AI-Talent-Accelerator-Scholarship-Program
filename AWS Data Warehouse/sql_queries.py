import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender CHAR(1),
        item_in_session INTEGER,
        last_name VARCHAR,
        length DECIMAL,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        session_id INTEGER,
        song VARCHAR,
        status INTEGER,
        ts VARCHAR,
        user_agent VARCHAR,
        user_id INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INTEGER,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INTEGER,
        artist_id VARCHAR,
        artist_name VARCHAR,
        location VARCHAR ,
        latitude FLOAT ,
        longitude FLOAT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INTEGER IDENTITY(0,1) NOT NULL, 
        start_time TIMESTAMP              NOT NULL,
        user_id INTEGER                   NOT NULL,
        level VARCHAR                     NULL,
        song_id VARCHAR                   NOT NULL,
        artist_id VARCHAR                 NOT NULL,
        session_id INTEGER                NOT NULL,
        location VARCHAR                  NULL,
        user_agent VARCHAR                NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER               PRIMARY KEY,
        first_name VARCHAR            NULL,
        last_name VARCHAR             NULL,
        gender VARCHAR                NULL,
        level VARCHAR                 NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR               PRIMARY KEY,
        title VARCHAR                 NOT NULL,
        artist_id VARCHAR             NOT NULL,
        year INTEGER                  NOT NULL,
        duration FLOAT                NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist(
        artist_id VARCHAR             PRIMARY KEY,
        name VARCHAR                  NOT NULL,
        location VARCHAR              NULL,
        latitude FLOAT                NULL,
        longitude FLOAT               NULL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP         NOT NULL,
        hour NUMERIC                     NULL,
        day NUMERIC                      NULL,
        week NUMERIC                     NULL,
        month NUMERIC                    NULL,
        year NUMERIC                     NULL,
        weekday NUMERIC                  NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    iam_role {}
    REGION 'us-west-2'
    JSON {};
    """).format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role {}
    region 'us-west-2'
    JSON 'auto';
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
        se.user_id         AS user_id,
        se.level           AS level,
        ss.song_id         AS song_id,
        ss.artist_id       AS artist_id,
        se.session_id      AS session_id,
        se.location        AS location,
        se.user_agent      AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss ON (se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration)
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT(user_id) AS user_id,
        first_name AS first_name,
        last_name  AS last_name,
        gender     AS gender,
        level      AS level
    FROM staging_events
    WHERE page = 'NextSong' AND user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration)
    SELECT  DISTINCT song_id AS song_id,
            title            AS title,
            artist_id        AS artist_id,
            year             AS year,
            duration         AS duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artist (
        artist_id,
        name,
        location,
        latitude,
        longitude)
    SELECT  DISTINCT artist_id   AS artist_id,
        artist_name              AS name,
        location                 AS location,
        latitude                 AS latitude,
        longitude                AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  start_time AS start_time,
            EXTRACT(hour FROM start_time)  AS hour,
            EXTRACT(day FROM start_time)   AS day,
            EXTRACT(week FROM start_time)  AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time)  AS year,
            EXTRACT(weekday FROM start_time) as weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
