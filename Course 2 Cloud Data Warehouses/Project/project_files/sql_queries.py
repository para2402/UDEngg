import configparser

"""
WHAT ABOUT PARTITION STRATAGIES?
"""



# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                    artist VARCHAR,
                                    auth VARCHAR,
                                    firstName VARCHAR,
                                    gender CHAR(1),
                                    itemInSession INT,
                                    lastName VARCHAR,
                                    length FLOAT,
                                    level VARCHAR,
                                    location VARCHAR,
                                    method VARCHAR,
                                    page VARCHAR,
                                    registration FLOAT,
                                    sessionId INT,
                                    song VARCHAR,
                                    status INT,
                                    ts BIGINT,
                                    userAgent VARCHAR,
                                    userId INT
                                    );
                              """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs         INTEGER,
                                    artist_id         VARCHAR,
                                    artist_latitude   FLOAT,
                                    artist_longitude  FLOAT,
                                    artist_location   VARCHAR,
                                    artist_name       VARCHAR,
                                    song_id           VARCHAR,
                                    title             VARCHAR,
                                    duration          FLOAT,
                                    year              FLOAT
                                    );
                                """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id     INT IDENTITY(0, 1) PRIMARY KEY,
                                user_id         INT,
                                song_id         VARCHAR,
                                artist_id       VARCHAR,
                                start_time      TIMESTAMP SORTKEY DISTKEY,
                                session_id      INT,
                                level           VARCHAR,
                                location        VARCHAR,
                                user_agent      VARCHAR
                                );
                            """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                            user_id    INT PRIMARY KEY SORTKEY,
                            firstname  VARCHAR,
                            lastname  VARCHAR,
                            gender    CHAR(1),
                            level     VARCHAR
                            );
                     """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                            song_id    VARCHAR PRIMARY KEY SORTKEY,
                            title      VARCHAR,
                            artist_id  VARCHAR,
                            year       INT,
                            duration   FLOAT
                            );
                     """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id  VARCHAR PRIMARY KEY SORTKEY,
                                name       VARCHAR,
                                location   VARCHAR,
                                latitude   FLOAT,
                                longitude  FLOAT
                                );
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                            start_time  TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
                            hour        INT,
                            day         INT,
                            week        INT,
                            month       INT,
                            year        INT,
                            weekday     INT
                            );
                        """)

# STAGING TABLES
staging_events_copy = ("""COPY staging_events
                          FROM {}
                          iam_role {}
                          json {}
                          REGION 'us-west-2';
                       """).format(config.get('S3', 'LOG_DATA'),
                                   config.get("IAM_ROLE","ARN"),
                                   config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs
                         FROM {}
                         iam_role {}
                         json 'auto'
                         REGION 'us-west-2';
                      """).format(config.get('S3', 'SONG_DATA'), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, artist_id,
                                                   song_id, session_id, location, user_agent)
                            SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                                   se.userId AS user_id,
                                   se.level,
                                   ss.artist_id,
                                   ss.song_id,
                                   se.sessionId AS session_id,
                                   se.location,
                                   se.userAgent AS user_agent
                            FROM staging_songs AS ss
                            INNER JOIN staging_events AS se
                                    ON (ss.title = se.song AND se.artist = ss.artist_name)
                            WHERE se.page = 'NextSong'
                         """)

user_table_insert = ("""INSERT INTO users (user_id, firstname, lastname, gender, level)
                        SELECT DISTINCT userId, firstName, lastName, gender, level
                        FROM staging_events
                        WHERE page = 'NextSong';
                     """)

song_table_insert = ("""INSERT INTO songs
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs;
                     """)

artist_table_insert = ("""INSERT INTO artists
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs;
                       """)

time_table_insert = ("""INSERT INTO time
                        SELECT DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' as start_time,
                                        EXTRACT(HOUR FROM start_time) AS hour,
                                        EXTRACT(DAY FROM start_time) AS day,
                                        EXTRACT(WEEKS FROM start_time) AS week,
                                        EXTRACT(MONTH FROM start_time) AS month,
                                        EXTRACT(YEAR FROM start_time) AS year,
                                        EXTRACT(DOW FROM start_time) AS weekday
                        FROM staging_events;
                     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
