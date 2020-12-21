# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id SERIAL PRIMARY KEY,
                                user_id INT NOT NULL,
                                song_id TEXT,
                                artist_id TEXT,
                                start_time BIGINT NOT NULL,
                                session_id INT,
                                level TEXT CHECK(level IN ('free', 'paid')),
                                location TEXT,
                                user_agent TEXT,
                                CONSTRAINT songplays_users_fk FOREIGN KEY (user_id) REFERENCES users (user_id),
                                CONSTRAINT songplays_songs_fk FOREIGN KEY (song_id) REFERENCES songs (song_id),
                                CONSTRAINT songplays_artists_fk FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
                                CONSTRAINT songplays_time_fk FOREIGN KEY (start_time) REFERENCES time (start_time));
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
                                duration REAL);
                        """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id TEXT PRIMARY KEY,
                                name TEXT NOT NULL,
                                location TEXT NOT NULL,
                                latitude DECIMAL(9,6) NOT NULL,
                                longitude DECIMAL(9,6) NOT NULL);
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                start_time BIGINT PRIMARY KEY,
                                hour INT NOT NULL,
                                day INT NOT NULL,
                                week INT NOT NULL,
                                month INT NOT NULL,
                                year INT NOT NULL,
                                weekday INT NOT NULL);
                        """)

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id,
                                                   artist_id, session_id, location, user_agent)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id)
                        DO
                            UPDATE SET level = EXCLUDED.level;""")

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