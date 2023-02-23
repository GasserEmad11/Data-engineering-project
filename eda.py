
import pandas as pd
import matplotlib.pyplot as plt
import os
import configparser
import glob 

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, isnan, when, count, desc, asc, sum, avg, min, max, countDistinct, count, mean, stddev, lit, split, regexp_extract, regexp_replace, date_format, to_date, to_timestamp, from_unixtime, unix_timestamp, year, month, dayofmonth, weekofyear, dayofweek, hour, minute, second, date_add, date_sub, datediff, months_between, to_date, to_timestamp, from_unixtime, unix_timestamp, year, month, dayofmonth, weekofyear, dayofweek, hour, minute, second, date_add, date_sub, datediff, months_between
from pyspark.sql.types import *

def cleaning_tasks(dataframe):
    #first we calculate the missiing values in each column
    removed_col=list()
    total_values=dict()
    for col in dataframe.columns:
        val=dataframe.filter(dataframe[col].isNull()).count()
        total_val=100*(val/dataframe.count())
        if total_val>70:
            removed_col.append(col)
        total_values.update({col:total_val})
    #visualize the missing values
    print('visualizing the missing values')
    fig,ax=plt.subplots(figsize=(25,10))
    ax.bar(total_values.keys(),total_values.values())
    ax.set(title='percentage of missing values ',ylabel='percentage')
    plt.show()
    #remove the columns with more than 70% of missing values
    print(f'removing columns with 70% of missing values {removed_col}')
    dataframe=dataframe.drop(*removed_col)
    #remove redundancies from dataframes and calculate the numbers of rows beofre and after removing the redundancies 
    print('removing redundancies from dataframes')
    count_before=dataframe.count()
    dataframe=dataframe.dropDuplicates()
    count_after=dataframe.count()
    print('the number of rows before removing the redundancies is {} and after removing the redundancies is {}'.format(count_before,count_after))
    #remove rows with 50% of missing values
#     print('removing 50% rows with  missing values')
    thresh=int (0.5*len(dataframe.columns))
    dataframe=dataframe.dropna(thresh=thresh)
    print('done !!!')
    return dataframe