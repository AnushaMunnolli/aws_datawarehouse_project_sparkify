import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN" )
KEY=config.get('AWS','KEY')
SECRET=config.get('AWS', 'SECRET')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE stage_events (
        se_artist VARCHAR,
        se_auth VARCHAR, 
        se_firstName VARCHAR,
        se_gender VARCHAR,
        se_itemInSession INTEGER,
        se_lastName VARCHAR,
        se_length FLOAT,
        se_level VARCHAR,
        se_location VARCHAR,
        se_method VARCHAR,
        se_page VARCHAR,
        se_registration FLOAT,
        se_sessionId INTEGER,
        se_song VARCHAR,
        se_status INTEGER,
        se_ts BIGINT,
        se_userAgent VARCHAR,
        se_userId INTEGER     
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE stage_songs (
        ss_num_of_songs INTEGER, 
        ss_artist_id VARCHAR,
        ss_artist_latitude VARCHAR,
        ss_artist_longitude VARCHAR,
        ss_artist_location VARCHAR,
        ss_artist_name VARCHAR,
        ss_song_id VARCHAR,
        ss_title VARCHAR,
        ss_duration FLOAT,
        ss_year INTEGER
            
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplay(
        p_songplay_id BIGINT IDENTITY(1,1) NOT NULL sortkey distkey,
        p_start_time TIMESTAMP NOT NULL,
        p_user_id INTEGER NOT NULL,
        p_level VARCHAR,
        p_song_id VARCHAR NOT NULL,
        p_artist_id VARCHAR NOT NULL,
        p_session_id INTEGER NOT NULL,
        p_location VARCHAR,
        p_user_agent VARCHAR,
        PRIMARY KEY (p_songplay_id)
    );
""")

user_table_create = ("""
    CREATE TABLE users(
        u_user_id INTEGER NOT NULL sortkey,
        u_first_name VARCHAR,
        u_last_name VARCHAR,
        u_gender VARCHAR,
        u_level VARCHAR,
        PRIMARY KEY (u_user_id)
    );
""")

song_table_create = ("""
    CREATE TABLE songs(
        s_song_id VARCHAR NOT NULL sortkey,
        s_title VARCHAR,
        s_artist_id VARCHAR NOT NULL,
        s_year INTEGER,
        s_duration FLOAT,
        PRIMARY KEY (s_song_id)
    ) diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE artists(
        a_artist_id VARCHAR NOT NULL sortkey,
        a_name VARCHAR,
        a_location VARCHAR,
        a_latitude VARCHAR,
        a_longitude VARCHAR,
        PRIMARY KEY (a_artist_id)
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE time_table(
        t_start_time TIMESTAMP NOT NULL sortkey,
        t_hour INTEGER NOT NULL,
        t_day INTEGER NOT NULL,
        t_week INTEGER NOT NULL,
        t_month INTEGER NOT NULL,
        t_year INTEGER NOT NULL,
        t_weekday INTEGER NOT NULL,
        PRIMARY KEY (t_start_time)
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""

    copy stage_events from 's3://udacity-dend/log_data'
    iam_role {}
    region 'us-west-2'
    format as JSON 's3://udacity-dend/log_json_path.json'
  
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""

    copy stage_songs from 's3://udacity-dend/song_data'
    iam_role {}
    region 'us-west-2' 
    format as JSON 'auto'
    
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(p_start_time, p_user_id, p_level, p_song_id, p_artist_id, p_session_id, p_location, p_user_agent)
    SELECT GETDATE() as p_start_time
        , se.se_userId as p_user_id
        , se.se_level as p_level
        , ss.ss_song_id as p_song_id
        , ss.ss_artist_id as p_artist_id
        , se.se_sessionId as p_session_id
        , se.se_location as p_location
        , se.se_userAgent as p_user_agent
    FROM stage_events se 
    JOIN stage_songs ss 
    ON se.se_artist = ss.ss_artist_name
    and se.se_song = ss.ss_title;    
""")

user_table_insert = ("""
    INSERT INTO users(u_user_id, u_first_name, u_last_name, u_gender, u_level)
    SELECT se.se_userId as u_user_id
        , se.se_firstName as u_first_name
        , se.se_lastName as u_last_name
        , se.se_gender as u_gender
        , se.se_level as u_level
    FROM stage_events se;
""")

song_table_insert = ("""
    INSERT INTO songs(s_song_id, s_title, s_artist_id, s_year, s_duration)
    SELECT ss.ss_song_id as s_song_id
        , ss.ss_title as s_title
        , ss.ss_artist_id as s_artist_id
        , ss.ss_year as s_year
        , ss.ss_duration as s_duration
    FROM stage_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artists(a_artist_id, a_name, a_location, a_latitude, a_longitude)
    SELECT ss.ss_artist_id as a_artist_id
        , ss.ss_artist as a_name
        , ss.ss_artist_location as a_location
        , ss.ss_artist_latitude as a_latitude
        , ss.ss_artist_longitude as a_longitude
    FROM stage_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time_table(t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday)
    SELECT GETDATE() as t_start_time
        , EXTRACT(HOUR FROM GETDATE()) AS t_hour
        , EXTRACT(DAY FROM GETDATE()) AS t_day
        , EXTRACT(WEEK FROM GETDATE()) AS t_week
        , EXTRACT(MONTH FROM GETDATE()) AS t_month
        , EXTRACT(YEAR FROM GETDATE()) AS t_year
        , EXTRACT(DOW FROM GETDATE()) AS t_weekday;
""")

#TEST QUERIES

most_played_song = ("""
    SELECT s.s_titles
        , COUNT(p.p_songplay_id)
        FROM songplay p JOIN songs s
        ON p.p_song_id = s.s_title
        GROUP BY s.s_titles
        ORDER BY COUNT(p.p_songplay_id) DESC LIMIT 1;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

