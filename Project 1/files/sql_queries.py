# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id INT PRIMARY KEY,
                                user_id INT NOT NULL,
                                song_id TEXT,
                                artist_id TEXT,
                                start_time TEXT NOT NULL,
                                session_id INT NOT NULL,
                                level TEXT NOT NULL CHECK(level IN ('free', 'paid')),
                                location TEXT NOT NULL,
                                user_agent TEXT NOT NULL);
                            """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                user_id INT PRIMARY KEY,
                                first_name TEXT NOT NULL,
                                last_name TEXT NOT NULL,
                                gender CHAR(1) NOT NULL,
                                level TEXT NOT NULL);
                        """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                song_id TEXT PRIMARY KEY,
                                title TEXT NOT NULL,
                                artist_id TEXT NOT NULL,
                                year INT NOT NULL,
                                duration DECIMAL(8, 5));
                        """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id TEXT PRIMARY KEY,
                                name TEXT NOT NULL,
                                location TEXT NOT NULL,
                                latitude DOUBLE PRECISION NOT NULL,
                                longitude DOUBLE PRECISION NOT NULL);
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                start_time TEXT PRIMARY KEY,
                                hour INT NOT NULL,
                                day INT NOT NULL,
                                week INT NOT NULL,
                                month INT NOT NULL,
                                year INT NOT NULL,
                                weekday INT NOT NULL);
                        """)

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level,
                                                   song_id, artist_id, session_id, location, user_agent)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          VALUES (%s, %s, %s, %s, %s)
                          ON CONFLICT DO NOTHING;""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;""")

# FIND SONGS

song_select = ("""SELECT song_id, artists.artist_id
                  FROM songs
                  INNER JOIN artists
                  ON songs.artist_id = artists.artist_id
                  WHERE songs.title = %s
                    AND artists.name = %s
                    AND songs.duration = %s;""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]