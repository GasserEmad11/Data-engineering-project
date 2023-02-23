import pandas as pd
import matplotlib.pyplot as plt
import os
import configparser
import glob 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import numpy as np
import datetime

"""
    This script creates the tables for the star schema
"""
def create_visa_dim(spark,staging_table1,staging_table2):
    """
    creates the visa dimension table
    args:
        spark: instance of a spark session
        staging_table1: first staging table
        staging_table2: second staging table
    returns:
        visa_dim: visa dimension table
    """
    #create the visa dimension table
    visa_dim=staging_table1.select('i94visa','visatype','visapost')\
        .withColumn('visa_id',monotonically_increasing_id())
    #drop the duplicates
    visa_dim=visa_dim.dropDuplicates()
    #return the visa dimension table
    return visa_dim

def create_airport_dim(spark,staging_table1,staging_table2):
        """
        creates the airport dimension table
        args:
            spark: instance of a spark session
            staging_table1: first staging table
            staging_table2: second staging table
        returns:
        """
        #create the airport dimension table
        airport_dim=staging_table2.select('ident','type','name','iso_region')\
            .withColumnRenamed('ident','airport_id')\
            .withColumnRenamed('type','airport_type')\
            .withColumnRenamed('name','airport_name')\
            .withColumn('iso_region',regexp_replace('iso_region','US-',''))\
            .withColumnRenamed('iso_region','region')
        #drop the duplicates
        airport_dim=airport_dim.dropDuplicates()
        #return the airport dimension table
        return airport_dim

def create_demographic_dim(spark,staging_table1,staging_table2):
    """
    creates the demographic dimension table
    args:
        spark: instance of a spark session
        staging_table1: first staging table
        staging_table2: second staging table
    returns:
        demographic_dim: demographic dimension table
    """
    demo_dim=staging_table1.select('City','State','median age','Total Population','Foreign-born','Race','state_code','Count')\
        .withColumnRenamed('median age','average_age')\
        .withColumnRenamed('Total Population','population')\
        .withColumnRenamed('Foriegn-born','foreign_born')\
        .withColumnRenamed('Count','Race_count')
    #drop the duplicates
    demo_dim=demo_dim.dropDuplicates()
    #return the demographic dimension table
    return demo_dim

def convert_days_to_date(days):
        return datetime.datetime(1960,1,1)+datetime.timedelta(int(float((days))))
    #create a udf to convert the arrival date to a date format

def create_time_dim(spark,staging_table1,staging_table2):
    """
    creates the time dimension table
    args:
        spark: instance of a spark session
        staging_table1: first staging table
        staging_table2: second staging table
    returns:
        time_dim: time dimension table
    """
    #create a udf to convert the arrival date to a date format
    convert_days_to_date_udf=udf(lambda x:convert_days_to_date(x),DateType())
    #create the time dimension table
    time_dim=staging_table1.select('arrdate')\
        .withColumn('date',convert_days_to_date_udf('arrdate'))\
        .withColumn('year',year('date'))\
        .withColumn('month',month('date'))\
        .withColumn('day',dayofmonth('date'))
    #drop the duplicates
    time_dim=time_dim.dropDuplicates()
    #return the time dimension table
    return time_dim

def create_immigrant_fact(spark,staging_table1,staging_table2):
    """
    creates the immigrant fact table
    args:
        spark: instance of a spark session
        staging_table1: first staging table
        staging_table2: second staging table
    returns:
        immigrant_fact
    """
    #create udf to convert the arrival date to a date format
    convert_days_to_date_udf=udf(lambda x:convert_days_to_date(x),DateType())
    #create the immigrant fact table
    immigrant_fact=staging_table1.select('cicid','gender','i94visa','City','admnum','Race','arrdate','state_code','depdate','median age')\
            .withColumnRenamed('cicid','id')\
            .withColumnRenamed('admnnum','admission_number')\
            .withColumn('arrival_date',convert_days_to_date_udf('arrdate'))\
            .withColumn('departure_date',convert_days_to_date_udf('depdate'))\
            .withColumnRenamed('median age','average_age')
    #drop the duplicates
    immigrant_fact.dropDuplicates()
    #return the immigrant fact table
    return immigrant_fact
   
    

def create_temp_dim(spark,staging_table2,state_to_code):
    """
    creates the temperature dimension table
    args:
        spark: instance of a spark session
        staging_table2: second staging table
    returns:
        temp_dim: temperature dimension table
    """
    #create the temperature dimension table
    temp_dim=staging_table2.select('dt','AverageTemperature','City','Country','Latitude','Longitude','iso_region')\
        .withColumnRenamed('dt','date')\
        .withColumnRenamed('AverageTemperature','average_temperature')\
        .withColumn('iso_region',regexp_replace('iso_region','US-',''))\
        .withColumnRenamed('iso_region','state_code')
        
    #drop the duplicates
    temp_dim=temp_dim.dropDuplicates()
    #return the temperature dimension table
    return temp_dim
    

