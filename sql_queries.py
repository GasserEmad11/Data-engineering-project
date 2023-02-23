"""
this module contains the sql queries intended to create the tables for the star schema
"""
#     #create the  dimension tables
['create_statments']
create_immigrant_fact="""CREATE TABLE IF NOT EXISTS immigrant_fact(
    id BIGINT PRIMARY KEY,
    visa_id INT,
    gender VARCHAR(50),
    admission_number VARCHAR(50),
    date DATETIME,
    airport_code VARCHAR(50),
    state_code VARCHAR(50),
    City VARCHAR(50))
"""


create_visa_table=""" CREATE TABLE IF NOT EXISTS visa_dim(
    visa_id INT PRIMARY KEY,
    i94visa INT,
    visa_type VARCHAR(50),
    visa_post VARCHAR(50)
);"""
create_demo_table=""" CREATE TABLE IF NOT EXISTS demo_dim(
    City VARCHAR(50),
    State VARCHAR(50),
    average_age FLOAT,
    population BIGINT,
    foreign_born BIGINT,
    race VARCHAR(50),
    state_code VARCHAR(50),
    race_count BIGINT);
"""
create_airport_table=""" CREATE TABLE IF NOT EXISTS airport_dim(
    airport_code VARCHAR(50),
    airport_type VARCHAR(50),
    airport_name VARCHAR(255),
    region VARCHAR(50))
"""
create_time_table=""" CREATE TABLE IF NOT EXISTS time_dim(
    date DATETIME PRIMARY KEY,
    year INT,
    month INT,
    day INT)
"""
create_temperture_table=""" CREATE TABLE IF NOT EXISTS temp_dim(
    City VARCHAR(50),
    AverageTemperature FLOAT,
    Country VARCHAR(50),
    Latitude FLOAT,
    Longitude FLOAT)
"""
create_visa_id="""CREATE INDEX IF NOT EXISTS visa_id ON visa_dim(visa_id);"""




#insert statments for the schema
['insert_statments']
insert_visa_table="""INSERT INTO visa_dim(visa_id,i94visa,visa_type,visa_post)
SELECT i94visa AS i94visa,
       visa_type AS visa_type,
       visa_post AS visa_post)
FROM staging_table1
;
"""
insert_demo_table=""" INSERT INTO TABLE demo_dim(City,State,average_age,population,foriegn_born,race,state_code,race_count)
SELECT City AS City,
       State AS State,
       median age AS average_age,
       Total Population AS population,
       Foreign-born AS foreign_born,
       Race as race,
       state_code AS state_code,
       Count as race_count)
FROM staging_table1
;
"""
insert_airport_table=""" INSERT INTO TABLE airport_dim(airport_code,airport_type,airport_name,region)
    SELECT ident AS airport_code,
           type AS airport_type,
           name AS airport_name,
           REPLACE(iso_region,'us-','') AS region))
    FROM staging_table2
    ;
"""
insert_time_table=""" INSERT INTO TABLE time_dim(date,year,month,day)
 SELECT DATEADD(day,arrdate,CAST('1960-01-01' AS DATE)) AS date,
        Extract(year from date) AS year,
        Extract(month from date) AS month,
        Extract(day from date) AS day)
    FROM staging_table1
    ;
"""
insert_temperture_table=""" INSERT INTO TABLE temp_dim(City,AverageTemperature,Country,Latitude,Longitude)
    SELECT City AS City,
    AverageTemperature AS AverageTemperature,
    Country AS Country,
    Latitude AS Latitude,
    Longitude AS Longitude)
    FROM staging_table2
    ;
"""
insert_immigrant_fact_table=""" INSERT INTO TABLE immigrant_fact(id,visa_id,gender,admission_number,date,airport_code,state_code,City)
    SELECT stage1.ciciid AS id ,
           v.visa_id AS visa_id ,
           stage1.gender as gender,
           stage1.admnum AS admission_number,
           DATEADD(day,stage1.arrdate,CAST('1960-01-01' AS DATE)) AS date,
           air.airport_code AS airport_code,
           stage1.state_code AS state_code,
           stage1.city AS City)
    FROM staging_table1 AS stage1
    JOIN visa_dim AS v ON stage1.i94visa = v.i94visa
    JOIN airport_dim AS air ON stage1.state_code = air.region
    ;
"""

#drop statments for the schema
['drop_statments']
time_drop = "DROP TABLE IF EXISTS time_dim"
visa_drop = "DROP TABLE IF EXISTS visa_dim"
demo_drop = "DROP TABLE IF EXISTS demo_dim"
airport_drop = "DROP TABLE IF EXISTS airport_dim"
temperture_drop = "DROP TABLE IF EXISTS temp_dim"
immigrant_drop ="DROP TABLE IF EXISTS immigrant_fact"






