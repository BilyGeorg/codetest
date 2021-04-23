import pandas as pd
import numpy as np
import sqlite3
import json
import os


class CRUD():

    def __init__(self, config):
        self.config = config
        if not os.path.exists(f'{os.getcwd()}{self.config["db_file_loc"]}'):
            # Create new DB
            self.db = sqlite3.connect(f'{os.getcwd()}{self.config["db_file_loc"]}')
            self.cursor = self.db.cursor()            
        else: # Connect to present DB
            self.db, self.db_cursor = self.db_connector(db_path=self.config["db_file_loc"])


    def db_connector(self, db_path):
        ''' Establish connection with the DB file'''
        conn = sqlite3.connect(f'{os.getcwd()}{db_path}')
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
    

    def insert(self, df=pd.DataFrame, table_name=str):
        '''Insert-appends table'''

        if len(df)>0:
            # Insert data
            df.to_sql(table_name, self.db, if_exists='append', index = False)          
        else:
            print("No data")
            return None
        
    
    def read(self, sql_query=None, sql_file=None):
        '''Reads SQL'''

        if sql_file:
            query = open(f"{os.getcwd()}{sql_file}", "r")
            df = pd.read_sql(query.read(),con=self.db)
            return df
        elif sql_query:
            df = pd.read_sql(sql_query,con=self.db)
            return df
        else:
            print("Please parse sql_query or sql_file")
            return None


    def read_file(self, file_path):
        '''Read file, input takes in .csv and .txt files'''
        df = pd.read_csv(f"{os.getcwd()}{file_path}")
        return df


    def format_file(self, df=pd.DataFrame):
        '''Drops duplicates if present across all values and keys, resets the index'''
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df


    def map_sqlite_dtypes(self, df=pd.DataFrame):
        '''Maps DataFrame data types with database specific data types'''

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
        '''Finds primary key in a table based on the unique identifier'''
        
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

        create_table_string = f'''CREATE TABLE IF NOT EXISTS {table_name} ({sql_cols})'''

        return create_table_string


    def pipeline(self):
 
        # Read file
        df = self.read_file(file_path=self.config["path_to_file"])

        # Format file
        df = self.format_file(df=df)
        
        if not os.path.exists(f'{os.getcwd()}{self.config["db_file_loc"]}'):
            print("DB Pipeline")
            # Generate DB and create table schema
            self.create(df=df, table_name=self.config["table_name"])
 
            # Insert data
            self.insert(df=df, table_name=self.config["table_name"])

        else:

            try:
                df = self.read(sql_file=self.config["generic_query"])
                if len(df)>0:
                    print("Using present DB")
            except: # Insert data
                self.insert(df=df, table_name=self.config["table_name"])