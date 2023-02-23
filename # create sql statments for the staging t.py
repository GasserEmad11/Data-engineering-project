# create sql statments for the staging tables
# add an id column to the immigration_data 
create_column_query="""ALTER TABLE immigration_data
ADD cicid TEXT NULL ;"""


create_staging_table1=("""CREATE TABLE IF NOT EXISTS staging_table1
(   id VARCHAR (256),
    city VARCHAR (256),
    state VARCHAR (256),
    visa_id      BIGINT,
    gender       TEXT,
    occupation   TEXT,
    visa_type    TEXT,
    ci_number    BIGINT,
    date         TIMESTAMP,
    airport_code TEXT,
    average_age  INT,
    population   BIGINT,
    foreign_born BIGINT,
    race         TEXT,
    origin       TEXT,
    visa_post    TEXT);
    """)

    staging_table1_insert=(""" INSERT INTO TABLE staging_table1( id ,city,state,visa_id,gender,
    occupation,visa_type,ci_number,date,airport_code,average_age,population,foreign_born,
    race,origin,visa_post)
    SELECT i.cicid AS id,
    d.city AS city,
    d.state AS state,
    i.visa_id AS visa_id,
    i.gender AS gender,
    











)

