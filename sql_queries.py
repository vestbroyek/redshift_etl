import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA=config["S3"]["LOG_DATA"]
SONG_DATA=config["S3"]["SONG_DATA"]
LOG_JSONPATH=config["S3"]["LOG_JSONPATH"]
ROLE_ARN=config.get("IAM_ROLE", "DWH_IAM_ROLE_ARN")

# DROP TABLES
staging_events_table_drop = "drop table if exists stg_events"
staging_songs_table_drop = "drop table if exists stg_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists song"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES
staging_events_table_create= ("""

create table if not exists stg_events (

    artist          varchar,    
    auth            varchar,
    firstName       varchar,  
    gender          varchar,
    itemInSession   integer,
    lastName       	varchar,
    length        	numeric,
    level        	varchar,
    location        varchar,
    method      	varchar,
    page            varchar,
    registration    varchar,
    sessionId       integer,
    song            varchar,    
    status        	integer,
    ts              bigint,
    userAgent      	varchar,
    userId       	integer

);

""")

staging_songs_table_create = ("""

create table if not exists stg_songs (

	num_songs           integer,
	artist_id           varchar,
	artist_name         varchar,
	artist_latitude     numeric,
	artist_longitude    numeric,
	artist_location     varchar,
	song_id             varchar,
	title               varchar,
	duration            numeric,
	"year"              integer

);

""")

songplay_table_create = ("""

create table if not exists songplay (

    songplay_id         varchar     primary key distkey,
    user_id             integer,
    song_id             varchar,
    artist_id           varchar,
    session_id          smallint,
    start_time          timestamp   not null,
    level               varchar,
    location            varchar,
    user_agent          varchar

);

""")

user_table_create = ("""

create table if not exists users (

    user_id             smallint        primary key distkey,
    first_name          varchar,
    last_name           varchar,
    gender              varchar,
    level               varchar

);

""")

song_table_create = ("""

create table if not exists song (

    song_id             varchar   primary key distkey,
    title               varchar,
    artist_id           varchar,
    year                smallint,
    duration            numeric

);

""")

artist_table_create = ("""

create table if not exists artist (

    artist_id           varchar   primary key distkey,
    name                varchar,
    location            varchar,
    latitude            varchar,
    longitude           varchar

);

""")

time_table_create = ("""

create table if not exists time (

    start_time          timestamp       not null,
    hour                smallint,
    day                 smallint,
    week                smallint,
    month               varchar,
    year                smallint,
    weekday             varchar

)

diststyle all;

""")

# STAGING TABLES
staging_events_copy = (f"""

    COPY stg_events
    FROM '{LOG_DATA}'
    IAM_ROLE '{ROLE_ARN}'
    JSON '{LOG_JSONPATH}'
    REGION 'us-west-2'

""")

staging_songs_copy = (f"""

    COPY stg_songs
    FROM '{SONG_DATA}'
    IAM_ROLE '{ROLE_ARN}'
    REGION 'us-west-2'
    JSON 'auto'

""")

# FINAL TABLES
songplay_table_insert = ("""

insert into songplay (

    songplay_id,
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent

)

select
    md5(events.sessionid || events.start_time) songplay_id,
    events.start_time, 
    events.userid, 
    events.level, 
    songs.song_id, 
    songs.artist_id, 
    events.sessionid, 
    events.location, 
    events.useragent
    from (select timestamp 'epoch' + ts/1000 * interval '1 second' as start_time, *
from stg_events
where page='NextSong') events
left join stg_songs songs
on events.song = songs.title
and events.artist = songs.artist_name
and events.length = songs.duration


""")

user_table_insert = ("""

insert into users (

    user_id,
    first_name,
    last_name,
    gender,
    level

)

select 
    distinct userid, 
    firstname, 
    lastname, 
    gender, 
    level
from stg_events
where page='NextSong'

""")

song_table_insert = ("""

insert into song (

    song_id,
    title,
    artist_id,
    year,
    duration

)

select
    distinct song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM stg_songs
        
""")

artist_table_insert = ("""

insert into artist (

    artist_id,
    name,
    location,
    latitude,
    longitude

)

select 
    distinct artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM stg_songs

""")

time_table_insert = ("""

insert into time (

    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday

)

select 
    start_time, 
    extract(hour from start_time), 
    extract(day from start_time), 
    extract(week from start_time), 
    extract(month from start_time), 
    extract(year from start_time),
    extract(dayofweek from start_time)
FROM songplay

""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
