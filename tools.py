from pyspark.sql.functions import collect_list
import numpy as np
import pandas as pd
def create_state_codes(spark,stage_table1):
    """
    creates a list of state codes
    args:
        spark: instance of a spark session
        stage_table1: first staging table
    returns:    
        state_codes: list of state codes
    """
    state_codes=stage_table1.select(collect_list('i94addr')).first()[0]
    state_codes=sorted(list(np.unique(state_codes)))
    return state_codes

def create_state_names(spark,stage_table1):
    """
    creates a list of state names
    args:
        spark: instance of a spark session
        stage_table1: first staging table
    returns:    
        state_names: list of state names
    """
    state_names=stage_table1.select(collect_list('state')).first()[0]
    state_names=sorted(list(np.unique(state_names)))
    return state_names



def state_to_code_map(state_codes,state_names):
    """
    creates a dictionary to map the state name into the state code
    args:
        state_codes: list of state codes
        states: list of state names
    returns:
        state_code_dict: dictionary to map the state name into the state code
    """
    #create a count to assign a number to each state (FIPS code)
    state_names_dict={}
    for i in range(len(state_names)):
        state_names_dict=state_names_dict.update({state_names[i]:i})
    #create a count to assign a number to each state code (FIPS code) in the correct order
    state_codes_dict={}
    for i in range(len(state_codes)):
        state_codes_dict=state_codes_dict.update({state_codes[i]:i})
    

        

    
