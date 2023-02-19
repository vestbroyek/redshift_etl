import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists stg_events"
staging_songs_table_drop = "drop table if exists stg_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists user"
song_table_drop = "drop table if exists song"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""

create table if not exists stg_events (

    artist          varchar(30),    
    auth            varchar(15),
    firstName       varchar(20),  
    gender          char,
    itemInSession   smallint,
    lastName       	varchar(20),
    length        	numeric(8, 5),
    level        	varchar(10),
    location        varchar(30),
    method      	varchar(5),
    page            varchar(20),
    registration    varchar(20),
    sessionId       smallint,
    song            varchar(50),    
    status        	smallint,
    ts              bigint,
    userAgent      	varchar(50),
    userId       	smallint            not null,

    primary key(sessionId, itemInSession)
);

""")

staging_songs_table_create = ("""

create table if not exists stg_songs (

    num_songs           smallint,
    artist_id           varchar(18),
    artist_latitude     varchar(20),
    artist_longitude    varchar(20),
    artist_location     varchar(30),
    artist_name         varchar(30),
    song_id             varchar(18),
    title               varchar(30),
    duration            numeric(8, 5)
    year                smallint,

    primary key(song_id, artist_id)

);

""")

songplay_table_create = ("""

create table if not exists songplay (

    songplay_id         varchar(18)     primary key,
    user_id             smallint,
    song_id             varchar(18),
    artist_id           varchar(18),
    session_id          smallint,
    start_time          timestamp,
    level               varchar(10),
    location            varchar(30),
    user_agent          varchar(50),

);

""")

user_table_create = ("""

create table if not exists user (

    user_id             smallint        primary key,
    first_name          varchar(25),
    last_name           varchar(25),
    gender              char,
    level               varchar(10),

);

""")

song_table_create = ("""

create table if not exists song (

    song_id             varchar(18)     primary key,
    title               varchar(50),
    artist              varchar(30),
    year                smallint,
    duration            numeric(8, 5),

);

""")

artist_table_create = ("""

create table if not exists artist (

    artist_id           varchar(18)     primary key,
    name                varchar(50),
    location            varchar(30),
    latitude            varchar(20),
    longitude           varchar(20)

);

""")

time_table_create = ("""

create table if not exists time (

    start_time          timestamp       primary key,
    hour                smallint,
    day                 smallint,
    week                smallint,
    month               smallint,
    year                smallint,
    weekday             smallint

);

""")

# STAGING TABLES
staging_events_copy = (f"""
COPY stg_events
FROM {LOG_DATA}
CREDENTIALS 'aws_iam_role=
JSON {LOG_JSONPATH}


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

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
