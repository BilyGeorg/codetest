import datetime as dt
import pandas as pd
import numpy as np
import sqlite3
import json
import os
import io

#print(os.getcwd())
print("Reporting")

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 150)


class Reporting():
    
    def __init__(self, config):
        self.config = config
        self.db, self.db_cursor = self.db_connector()


    def db_connector(self):
        ''' Establish connection with the DB file'''
        conn = sqlite3.connect(f'{os.getcwd()}{self.config["db_file_loc"]}')
        cursor = conn.cursor()
        return conn, cursor

    
    def read(self, sql_query=None, sql_file=None):
        '''Reads simple SQL string'''

        if sql_file is not None:
            query = open(sql_file, "r")
            df_sql = pd.read_sql(query.read(),con=self.db)
            return df_sql
        elif isinstance(sql_query, str) == True:
            df_sql = pd.read_sql(sql_query.read(),con=self.db)
            return df_sql
        else:
            print("Please parse sql_query or sql_file")
            return None
    

    def read_generic_sql_query(self):
        '''Read SQL'''
        sql_query = open(f"{os.getcwd()}/solution/generic_query.sql","r")
        #print(sql_query.read())
        df_sql = pd.read_sql(sql_query.read(),con=self.db)
        return df_sql

    # Reporting

    def participants(self, df_sql=pd.DataFrame):
        '''Fetch a list of participants who are co project directors who worked on projects within a certain state, the state will be provided as a parameter input'''

        # (1) Fetch a list of participants who are co project directors who worked on projects within a certain state, the state will be provided as a parameter input
        df = df_sql.copy()

        # first filter
        df = df.loc[
                    (df["Participants"].notna())
                &   (df["InstState"].isin(self.config["queries"]["Participants"]["states"]))
            ]
        df = df.loc[df[self.config["queries"]["Participants"]["name"]].str.contains(self.config["queries"]["Participants"]["criteria"], regex=False) == True]
        
        # convert string to list
        df["Participants"] = df["Participants"].apply(lambda row: row.split(";"))
        
        # filter for the relevant criteria
        df["Participants"] = df["Participants"].apply(lambda row: [x for x in row if self.config["queries"]["Participants"]["criteria"] in x])
        
        # explode list of participants
        df = df.explode('Participants')
        
        # clean-up
        df["Participants"] = df["Participants"].str.replace(rf'{self.config["queries"]["Participants"]["criteria"]}',"",regex=False).str.replace("[]","", regex=False).str.strip()
        
        # sort
        df.sort_values(["Participants"],ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # json/dict prep
        jdata = {"states":self.config["queries"]["Participants"]["states"],self.config["queries"]["Participants"]["criteria"]:df[self.config["queries"]["Participants"]["name"]].to_list()}
        
        # write to file
        with open(f'{os.getcwd()}/{self.config["queries"]["Participants"]["name"]}.json', "w") as f:
            json.dump(jdata, f, indent=4)
        
        return df


    def supplements(self, df_sql=pd.DataFrame):
        '''Aggregate of the total number of supplements given per year'''

        # (2) Aggregate of the total number of supplements given per year
        df = df_sql.copy()
        
        # aggregate
        df = df.groupby([
                self.config["queries"]["Supplements"]["char_criteria"] #"YearAwarded"
            ]).agg({
                self.config["queries"]["Supplements"]["num_criteria"]: np.sum #"SupplementCount"
                #self.config["queries"]["Supplements"]["num_criteria"]: np.mean
            }).reset_index()
        
        # json/dict prep
        jdata = {f'{self.config["queries"]["Supplements"]["char_criteria"]}-{self.config["queries"]["Supplements"]["num_criteria"]}':pd.Series(df[self.config["queries"]["Supplements"]["num_criteria"]].values,index=df[self.config["queries"]["Supplements"]["char_criteria"]]).to_dict()}
        
        # write to file
        with open(f'{os.getcwd()}/{self.config["queries"]["Supplements"]["name"]}.json', "w") as f:
            json.dump(jdata, f, indent=4)

        return df
   

    def projects(self, df_sql=pd.DataFrame):
        '''Count of each project per state with aggregated grants for each state'''

        # (3) Count of each project per state with aggregated grants for each state
        df = df_sql.copy()
        
        # get count of each project per state
        dfa = df.groupby(["InstState"]).size().reset_index(name="Projects")

        # get aggregated grants for each state
        dfb = df.groupby([
                "InstState",
            ]).agg({
                "OriginalAmount": np.sum
            }).reset_index()

        # merge both DataFrames
        df_final = pd.merge(dfa, dfb, on="InstState", how="inner")

        # json/dict prep
        jdata = dict()
        for index, row in df_final.iterrows():
            temp_dict = {
                row["InstState"]:{
                    "Projects":row["Projects"],
                    "TotalGrantsAmount": round(row["OriginalAmount"],2)
                }
            }
            jdata.update(temp_dict)

        # write to file
        with open(f'{os.getcwd()}/{self.config["queries"]["Projects"]["name"]}.json', "w") as f:
            json.dump(jdata, f, indent=4)
        
        return df_final