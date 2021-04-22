import pandas as pd
import numpy as np
import sqlite3
import json
import os


class Reporting():
    
    def __init__(self, config):
        self.config = config
        self.db, self.db_cursor = self.db_connector(db_path=self.config["db_file_loc"])


    def db_connector(self, db_path):
        ''' Establish connection with the DB file'''
        conn = sqlite3.connect(f'{os.getcwd()}{db_path}')
        cursor = conn.cursor()
        return conn, cursor

    # Reporting

    def participants(self, state=list, participants=str):
        '''Fetch a list of participants who are co project directors who worked on projects within a certain state, the state will be provided as a parameter input'''

        # # from sql file
        # query = open(f"{os.getcwd()}/db/participants.sql", "r").read()
        # df = pd.read_sql(query,con=self.db)

        states_q_prep = ",".join([f"'{x}'" for x in state])
        query = f"SELECT Participants,InstState FROM GRANTS WHERE Participants LIKE '%{participants}%' AND InstState  IN ({states_q_prep})  "
        df = pd.read_sql(query,con=self.db)

        # first filter making sure conditions match
        df = df.loc[
                    (df["Participants"].notna())
                &   (df["InstState"].isin(state))
            ]
        df = df.loc[df["Participants"].str.contains(participants, regex=False) == True]
        
        # convert string to list
        df["Participants"] = df["Participants"].apply(lambda row: row.split(";"))
        
        # filter for the relevant criteria
        df["Participants"] = df["Participants"].apply(lambda row: [x for x in row if participants in x])
        
        # explode list of participants
        df = df.explode('Participants')
        
        # clean-up
        df["Participants"] = df["Participants"].str.replace(rf'{participants}',"",regex=False).str.replace("[]","", regex=False).str.strip()
        
        # sort
        df.sort_values(["Participants"],ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # json/dict prep
        jdata = {
            "InstState":state,
            "Participants": list(set(df["Participants"].to_list()))
        }
        
        # write to file
        with open(f'{os.getcwd()}/output/{self.config["queries"]["Participants"]["name"]}.json', "w") as f:
            json.dump(jdata, f, indent=4)
        
        return df


    def supplements(self):
        '''Aggregate of the total number of supplements given per year'''

        query = open(f"{os.getcwd()}/db/supplements.sql", "r").read()
        df = pd.read_sql(query,con=self.db)

        df = df["Supplements"].str.split(";", expand=True)

        # transform
        frames= list()
        for col in range(len(list(df.columns))):
            df[col] = df[col].str.strip()
            temp_frame = pd.DataFrame(df[col])
            temp_frame.rename(columns={col:"AmountDate"}, inplace=True)
            temp_frame.dropna(inplace=True)
            frames.append(temp_frame)

        # extract and format
        df = pd.concat(frames, ignore_index=True)
        df[["Amount","Date"]] = df["AmountDate"].str.split(" ", expand=True)
        df["Year"] = df["Date"].str.extract(r"(\d{4})")
        df["Amount"] = df["Amount"].astype(float)

        # aggregate
        df = df.groupby(["Year"]).agg({"Amount": np.sum}).reset_index()

        # json/dict prep
        jdata = {"TotalAmountSupplementsPerYear": pd.Series(df["Amount"].values, index=df["Year"]).to_dict()}

        # write to file
        with open(f'{os.getcwd()}/output/{self.config["queries"]["Supplements"]["name"]}.json', "w") as f:
            json.dump(jdata, f, indent=4)

        return df
   

    def projects(self):
        '''Count of each project per state with aggregated grants for each state'''

        query = open(f"{os.getcwd()}/db/generic_query.sql", "r").read()
        df = pd.read_sql(query,con=self.db)

        # get count of each project per state
        dfa = df.groupby(["InstState"]).size().reset_index(name="Projects")

        # get aggregated grants for each state
        dfb = df.groupby([
                "InstState",
            ]).agg({
                "AwardOutright": np.sum,
                "AwardMatching": np.sum
            }).reset_index()
        
        # aggregate
        dfb["AmountSum"] = dfb["AwardOutright"] + dfb["AwardMatching"]

        # merge both DataFrames
        df_final = pd.merge(dfa, dfb, on="InstState", how="inner")

        # json/dict prep
        jdata = dict()
        for index, row in df_final.iterrows():
            temp_dict = {
                row["InstState"]:{
                    "Projects":row["Projects"],
                    "AmountSum": round(row["AmountSum"],2)
                }
            }
            jdata.update(temp_dict)

        # write to file
        with open(f'{os.getcwd()}/output/{self.config["queries"]["Projects"]["name"]}.json', "w") as f:
            json.dump(jdata, f, indent=4)
        
        return df_final