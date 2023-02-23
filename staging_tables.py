import pandas as pd
import matplotlib.pyplot as plt
import os
import configparser
import glob 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, isnan, when, count, desc, asc, sum, avg, min, max, countDistinct, count, mean, stddev, lit, split, regexp_extract, regexp_replace, date_format, to_date, to_timestamp, from_unixtime, unix_timestamp, year, month, dayofmonth, weekofyear, dayofweek, hour, minute, second, date_add, date_sub, datediff, months_between, to_date, to_timestamp, from_unixtime, unix_timestamp, year, month, dayofmonth, weekofyear, dayofweek, hour, minute, second, date_add, date_sub, datediff, months_between
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import collect_list
"""
this script creates the staging tables needed for the star schema
"""
def create_staging_table1(spark,immigration_data,demo_data):
    """
    creates the first staging table
    args:
        spark:instance of a spark session
        immigration_data: immigration dataset
        demo_data: demographic dataset
    returns:
        staging_table1: first staging table
    """
    demo_data=demo_data.withColumnRenamed('State Code','state_code')
    #rename the count column in the immigration dataset 
    immigration_data=immigration_data.withColumnRenamed('count','i94cnt')
    #join the two datasets as the staging table
    staging_table1=immigration_data.join(demo_data,immigration_data.i94addr==demo_data.state_code,'left')
    #remove the immigration data count and i94addr columns
    staging_table1=staging_table1.drop('i94cnt','i94addr')
    #drop the null values
    staging_table1=staging_table1.dropna()
    #return the staging table
    return staging_table1

def create_staging_table2(spark,temp_data,airport_data):
    """
    creates the second staging table needed for the star schema
    args:
        spark: instance of a spark session
        state_code_map: state code map dataset
        temp_data: temperature dataset
        airport_data: airport dataset
    returns:
        staging_table2: second staging table
    """
    #get a list of the municipalities in the airport data
    municipalities=sorted(list(airport_data.select(collect_list('municipality')).first()[0]))
    #get a list of the cities in the temperature data
    cities=sorted(list(temp_data.select(collect_list('City')).first()[0]))
    #get the data for us cities only in airport data
    airport_data=airport_data.filter(col('iso_country')=='US')
    #get the data for temperatures for every city at 2013 or later
    temp_data=temp_data.filter(col('dt')>='2013-01-01')
    #filter the temerature data to only include the cities in the municipalities dictionary
    temp_data=temp_data.filter(col('City').isin(municipalities))
    #join the airport data and the temperature data
    staging_table2=airport_data.join(temp_data,airport_data.municipality==temp_data.City,'full')
    #drop the null values
    staging_table2=staging_table2.dropna()
    #return the staging table
    return staging_table2