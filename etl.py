"""
this script perform automates the etl processes for the project 
"""
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
import boto3
import eda
import staging_tables
import create_tables






def cleaning_data(spark,df):
    """
    performs data cleaning tasks on the dataframe
    args:
        spark: spark session
        df: dataframe to be cleaned
    """
    #clean the dataframe using the cleaning_tasks function from eda.py
    df=eda.cleaning_tasks(df)
    #return the cleaned dataframe
    return df


def staging_tables_task(spark,df1,df2,df3,df4):
    """
    creates the staging tables needed for the etl processes
    args:
        spark: spark session
        df1: first dataframe (immigration_data)
        df2: second dataframe (demographics_data)
        df3: third dataframe (airport_data)
        df4: fourth dataframe (temperature_data)
    returns:
        staging_table1: first staging table
    """
    #create the first staging table using the create_staging_table function from staging_tables.py
    staging_table1=staging_tables.create_staging_table1(spark,df1,df2)
    #create the second staging table using the create_staging_table2 function from staging_tables.py
    staging_table2=staging_tables.create_staging_table2(spark,df3,df4)
    #return the two staging tables
    return staging_table1,staging_table2

def create_tables_task(spark,staging_table1,staging_table2):
    """
    automates the creation of the fact and dimension tables
    args:
        spark: spark session
        staging_table1: first staging table
        staging_table2: second staging table
    returns :
        immigrant_fact:the immigration fact table
        time_dim: the date dimension table
        temp_dim: the temperature dimension table
        airport_dim: the airport dimension table
        demographic_dim: the demographic dimension table
        visa_dim: the visa dimension table
    """ 
    #create the visa dimension table using the create_visa_dim function from create_tables.py
    visa_dim=create_tables.create_visa_dim(spark,staging_table1)
    #create the airport dimension table using the create_airport_dim function from create_tables.py
    airport_dim=create_tables.create_airport_dim(spark,staging_table2)
    #create the demographic dimension table using the create_demographic_dim function from create_tables.py
    demo_dim=create_tables.create_demographic_dim(spark,staging_table1)
    #create the time dimension table using the create_time_dim function from create_tables.py
    time_dim=create_tables.create_time_dim(spark,staging_table1)
    #create the temperature dimension table using the create_temp_dim function from create_tables.py
    temp_dim=create_tables.create_temp_dim(spark,staging_table2)
    #create the immigrant fact table using the create_immigrant_fact function from create_tables.py
    immigrant_fact=create_tables.create_immigrant_fact(spark,staging_table1,visa_dim,airport_dim,time_dim,demo_dim,temp_dim)
    #return the fact and dimension tables
    return visa_dim,airport_dim,demo_dim,time_dim,temp_dim,immigrant_fact


def data_quality_checks(spark,df):
    """
    performs data quality checks on the dataframe
    args:
        spark: spark session
        df: dataframe to be checked
    """
    #perform data quality checks on the dataframe using the data_quality_checks function from eda.py
    eda.data_quality_checks(df)
    #return the dataframe







def write_to_parquet_to_s3(spark,df):
    """
    writes the dataframe to parquet format and saves it to s3
    args:
        spark: spark session
        df: dataframe to be written
    """

    #write the dataframe to parquet format
    df.write.parquet('./data/parquet_data')
    #store the path in a variable
    path='./data/parquet_data'
    #establish a connection to s3
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    #establish a connection to s3
    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
                        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY']
                       )
    #create a bucket
    bucket=s3.create_bucket(Bucket=config['AWS']['BUCKET_NAME'])
    #upload the parquet files to s3
    for root, dirs, files in os.walk(path):
        for file in files:
        #get the full path
             path=os.join(root,file)
             with open(path,'rb') as parquet_file:
            #upload the file to s3
                bucket.put_object(Key=path, Body=parquet_file)


def main():
    #create a spak session
    spark=SparkSession.builder.master("local[*]") \
                    .appName('capstone') \
                    .getOrCreate()
    #read the datasets into dataframes
    immigration_data=spark.read.format('com.github.saurfang.sas.spark').load('./data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    demographics_data=spark.read.format('csv').option('header','true').load('./data/us-cities-demographics.csv',sep=';')
    airport_data=spark.read.format('csv').option('header','true').load('./data/airport-codes_csv.csv')
    temperature_data=spark.read.format('csv').option('header','true').load('./data/GlobalLandTemperaturesByCity.csv')
    #clean the dataframes
    immigration_data=cleaning_data(spark,immigration_data)
    demographics_data=cleaning_data(spark,demographics_data)
    airport_data=cleaning_data(spark,airport_data)
    temperature_data=cleaning_data(spark,temperature_data)
    #create the staging tables
    staging_table1=staging_tables_task(spark,immigration_data,demographics_data)
    staging_table2=staging_tables_task(spark,airport_data,temperature_data)
    #create the fact and dimension tables
    visa_dim,airport_dim,demo_dim,time_dim,temp_dim,immigrant_fact=create_tables_task(spark,staging_table1,staging_table2)
    #perform data quality checks on the fact and dimension tables
    data_quality_checks(spark,visa_dim)
    data_quality_checks(spark,airport_dim)
    data_quality_checks(spark,demo_dim)
    data_quality_checks(spark,time_dim)
    data_quality_checks(spark,temp_dim)
    data_quality_checks(spark,immigrant_fact)
    #write the fact and dimension tables to parquet format and save them to s3
    write_to_parquet_to_s3(spark,visa_dim)
    write_to_parquet_to_s3(spark,airport_dim)
    write_to_parquet_to_s3(spark,demo_dim)
    write_to_parquet_to_s3(spark,time_dim)
    write_to_parquet_to_s3(spark,temp_dim)
    write_to_parquet_to_s3(spark,immigrant_fact)
    
if __name__=='__main__':
    main()



