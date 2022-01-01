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

# staging table for log data
staging_events_table_create = ("""
create table if not exists log_data_staging (
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
userid text
);
""")

# staging table for song dataset
staging_songs_table_create = ("""
create table if not exists song_data_staging (
artist_id text,
artist_latitude numeric,
artist_longitude numeric,
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
songplay_id int identity (0, 1) not null,
start_time bigint not null distkey sortkey, 
user_id int null, 
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
location text null, 
latitude numeric null, 
longitude numeric null
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

# Extraction into staging tables using redshift copy command

staging_events_copy = (f"""
copy log_data_staging from 's3://udacity-dend/log_data'
credentials 'aws_iam_role={config.get('CLUSTER', 'DWH_ROLE_ARN')}'
format as json 's3://udacity-dend/log_json_path.json'
region 'us-west-2';
""")

staging_songs_copy = (f"""
copy song_data_staging from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={config.get('CLUSTER', 'DWH_ROLE_ARN')}'
format json 'auto'
region 'us-west-2';
""")

# FINAL TABLES

# Copy into final tables from staging

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select st.ts, cast(st.userId as integer), st.level, s.song_id, a.artist_id, st.sessionid,
       st.location, st.useragent
from log_data_staging st
left outer join songs s
on st.song = s.title and cast(st.length as numeric(10,2)) = cast(s.duration as numeric(10,2))
left outer join artists a
on st.artist = a.name
where st.userId != ''
and page = 'NextSong';
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct cast(userId as integer), firstName, lastName, gender, level
from log_data_staging
where userId != ''
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration) 
select distinct song_id, title, artist_id, year, duration
from song_data_staging
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude) 
select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from song_data_staging
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select distinct ts,
date_part('hour', dateadd("ms", ts, '1970-01-01')) as hour,
date_part('day', dateadd("ms", ts, '1970-01-01')) as day,
date_part('week', dateadd("ms", ts, '1970-01-01')) as week,
date_part('month', dateadd("ms", ts, '1970-01-01')) as month,
date_part('year', dateadd("ms", ts, '1970-01-01')) as year,
date_part('dow', dateadd("ms", ts, '1970-01-01')) as weekday
from log_data_staging
where page = 'NextSong'
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

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    song_table_insert, artist_table_insert, user_table_insert, artist_table_insert, time_table_insert,
    songplay_table_insert
]
