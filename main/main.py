import pandas as pd
import numpy as np
import sqlite3
import json
import os

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 150)

from db_crud import CRUD
from db_reporting import Reporting


def main():

    with open(f"{os.getcwd()}/config.json", mode="r") as f:
        config = json.load(f)
    
        # CRUD
        crud = CRUD(config=config)
        crud.pipeline()
        
        ## read sql
        df = crud.read(sql_file=config["generic_query"])

        # Reporting
        ## instantiate
        rep = Reporting(config=config)

        ## Fetch a list of participants who are co project directors who worked on projects within a certain state, the state will be provided as a parameter input
        df_participants = rep.participants(state=['NY','CA'], participants="[Co Project Director]")

        ## Aggregate of the total number of supplements given per year
        df_supplements = rep.supplements()

        # Count of each project per state with aggregated grants for each state
        df_projects = rep.projects()



if __name__ == '__main__':
    main()




