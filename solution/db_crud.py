import datetime as dt
import pandas as pd
import numpy as np
import sqlite3
import json
import os
import io

print(os.getcwd())
print("CRUD")

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 150)


class CRUD():
    
    def __init__(self, config):
        self.config = config
        self.db, self.db_cursor = self.db_connector()
        

    def db_connector(self):
        ''' Establish connection with the DB file'''
        conn = sqlite3.connect(f'{os.getcwd()}{self.config["db_file_loc"]}')
        cursor = conn.cursor()
        return conn, cursor


    def create(self, df=pd.DataFrame, table_name=str):
        '''Creates a table from parsed DataFrame'''

        # Map DataFrame dtypes to SQL dtypes
        sqlite_dtypes_map = self.map_sqlite_dtypes(df=df)

        # Find primary key
        primary_key = self.find_primary_key(df=df)

        # Generate create table string
        create_table_string = self.create_table_sql_string(table_name=table_name, primary_key=primary_key, sqlite_dtypes_map=sqlite_dtypes_map)

        # Create table
        self.db_cursor.execute(create_table_string)
        self.db.commit()
    

    def insert(self, df=pd.DataFrame, table_name=str):#, replace=False):
        '''Insert-appends or insert-replaces table if replace=True'''

        if len(df)>0: # and replace == False:
            # Insert data
            df.to_sql(table_name, self.db, if_exists='append', index = False)          
        # elif len(df)>0 and replace == True:
        #     # Replace exsiting data with new data
        #     df.to_sql(table_name, self.db, if_exists='replace', index = False)
        else:
            print("No data")
            return None
        
    
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


    def update(self, df=pd.DataFrame):
        ''' '''
        pass
    

    def delete(self, df=pd.DataFrame):
        ''' '''
        pass
    

    def read_file(self, file_path):
        ''' '''
        df = pd.read_csv(file_path)
        return df


    def format_file(self, df=pd.DataFrame):
        ''' '''
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df


    def map_sqlite_dtypes(self, df=pd.DataFrame):
        ''' '''

        dtypes = self.config["dtypes_map"]

        col_dtype_dict = dict()
        for col in df.columns.tolist():
            dtype = df[col].dtype
            for key, value in dtypes.items():
                if dtype == key:
                    temp_dict = {col:value}
                    col_dtype_dict.update(temp_dict)

        return col_dtype_dict


    def find_primary_key(self, df=pd.DataFrame):
        ''' '''
        
        length = df.shape[0]

        for col in df.columns.tolist():
            nunique = df[col].nunique()
            if nunique == length:
                return col


    def create_table_sql_string(self, table_name=str, primary_key=str, sqlite_dtypes_map=dict):
        ''' '''
        temp_cols = list()
        for key, value in sqlite_dtypes_map.items():
            if key == primary_key:
                prim_key = f"[{key}] {value} PRIMARY KEY"
                temp_cols.append(prim_key)
            else:
                temp_key = f"[{key}] {value}"
                temp_cols.append(temp_key)

        sql_cols = ",".join(temp_cols)

        create_table_string = f'''CREATE TABLE {table_name} ({sql_cols})'''

        return create_table_string


    def pipeline(self):

        # Read file
        #df = self.read_file(file_path=self.config["path_to_file"])
        df = pd.read_csv(self.config["path_to_file"])

        # Format file
        df = self.format_file(df=df)

        # Generate DB and create table schema
        self.create(df=df, table_name=self.config["table_name"])
         
        # Insert/Insert and drop data
        self.insert(df=df, table_name=self.config["table_name"], replace=True)

        # # Update
        # self.update(df=df)
        
        # # Delete
        # self.delete(df=df)

        pass