import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists log_data_staging"
staging_songs_table_drop = "drop table if exists song_data_staging"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create = ("""
create temp table if not exists log_data_staging (
num int,
artist text,
auth text,
firstName text,
gender text,
itemInSession int,
lastName text,
length text,
level text,
location text,
method text,
page text,
registration text,
sessionId int,
song text,
status int,
ts bigint,
userAgent text,
userId text
);
""")

staging_songs_table_create = ("""
create temp table if not exists song_data_staging (
num int,
artist_id text,
artist_latitude int,
artist_longitude int,
artist_location text,
artist_name text,
song_id text,
title text,
duration numeric,
year int
);
""")

songplay_table_create = ("""
create table if not exists songplays (
start_time bigint not null sortkey, 
user_id int not null distkey, 
level text not null, 
song_id text null, 
artist_id text null, 
session_id int not null, 
location text not null, 
user_agent text not null
);
""")

user_table_create = ("""
create table if not exists users (
user_id int sortkey, 
first_name text not null, 
last_name text not null, 
gender text not null, 
level text not null
) diststyle all;
""")

song_table_create = ("""
create table if not exists songs (
song_id text sortkey, 
title text not null, 
artist_id text not null, 
year int not null, 
duration numeric not null
) diststyle all;
""")

artist_table_create = ("""
create table if not exists artists (
artist_id text sortkey, 
name text not null, 
location text not null, 
latitude numeric not null, 
longitude numeric not null
) diststyle all;
""")

time_table_create = ("""
create table if not exists time 
(
start_time bigint sortkey, 
hour int not null, 
day int not null, 
week int not null, 
month int not null, 
year int not null, 
weekday int not null
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [
	staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create,
	song_table_create, artist_table_create, time_table_create
]

drop_table_queries = [
	staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
	song_table_drop, artist_table_drop, time_table_drop
]

copy_table_queries = []
# staging_events_copy, staging_songs_copy]
insert_table_queries = []
# songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
