import datetime as dt
import pandas as pd
import numpy as np
import sqlite3
import json
import os

print(os.getcwd())

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 150)

from db_crud import CRUD
from db_reporting import Reporting
#from logger import Logger


def main():
    with open(f"{os.getcwd()}/solution/config.json", mode="r") as f:
        config = json.load(f)
    
    try:
        # CRUD
        crud = CRUD(config=config)
        crud.pipeline()
        
        # Reporting
        ## instantiate
        rep = Reporting(config=config)
        
        ## generic sql query
        df = rep.read_generic_sql_query()
        print(df)
        
        # ## Fetch a list of participants who are co project directors who worked on projects within a certain state, the state will be provided as a parameter input
        # df_participants = rep.participants(df_sql=df)
        
        # ## Aggregate of the total number of supplements given per year
        # df_supplements = rep.supplements(df_sql=df)

        # ## Count of each project per state with aggregated grants for each state
        # df_projects = rep.projects(df_sql=df)
    
    except:
        pass

if __name__ == '__main__':
    env = "PROD"
    main()




