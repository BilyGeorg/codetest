import pandas as pd
import json
import os

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 150)

from db_crud import CRUD
from db_reporting import Reporting


def main():
    
    if os.getcwd().endswith("/main"):
        os.chdir("../")

    with open(f"{os.getcwd()}/config.json", mode="r") as f:
        config = json.load(f)

    # CRUD
    crud = CRUD(config=config)
    crud.pipeline()

    # Reporting
    # instantiate
    rep = Reporting(config=config)

    # Fetch a list of participants who are co project directors who worked on projects within a certain state, 
    # the state will be provided as a parameter input
    df_participants, participants = rep.participants(state=['NY','CA'], participants="[Co Project Director]")
    
    # Aggregate of the total number of supplements given per year
    df_supplements, supplements = rep.supplements()

    # Count of each project per state with aggregated grants for each state
    df_projects, projects = rep.projects()
    
    # write to file
    for report,jdata in {
            "Participants":participants,
            "Supplements":supplements,
            "Projects":projects
        }.items():

        with open(f'{os.getcwd()}/output/{config["queries"][report]["name"]}.json', "w") as f:
            json.dump(jdata, f, indent=4)


if __name__ == '__main__':
    main()




