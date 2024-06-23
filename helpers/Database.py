# have function that can update and query data from database

import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, Date, Float, MetaData, insert
from dotenv import load_dotenv
import hashlib
import pickle
import os 
load_dotenv()

# host = os.getenv("HOST")
# port = os.getenv("PORT")
# user = os.getenv("USER")
# password = os.getenv("PASSWORD")
# database = os.getenv("DATABASE")

host = 'localhost'
port = '3306'
user = 'root'
password = '1234'
database = 'test'


file_dir = os.path.dirname(os.path.abspath(__file__))
processed_id_path = os.path.join(file_dir, 'processed_id.csv')

class Database ():
    def __init__ (self):
        self.host = host
        self.port = port 
        self.user = user 
        self.password = password 
        self.database = database
        
        self.connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()  
        self.load_record_id()
    def save_record_id(self, df) :
        """ 
        Save the updated row id to the file to avoid reprocessing
        """
        if not os.path.exists(processed_id_path):
            df_id = df['id'].unique()
            df_id = pd.DataFrame(df_id, columns=['id'])
            df_id['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
            df_id.to_csv(processed_id_path, index=False)
        else:
            df_id = pd.read_csv(processed_id_path)
            new_id = pd.DataFrame(df['id'].unique(), columns=['id'])
            new_id['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
            df_id = pd.concat([df_id,new_id ])
            df_id.drop_duplicates(inplace=True)
            df_id.to_csv(processed_id_path, index=False)   
            
    def load_record_id(self):
        if not os.path.exists(processed_id_path):
            self.processed_id = []
        else:
            processed_id = pd.read_csv(processed_id_path)['id'].values.tolist()
            self.processed_id = processed_id
        
    def filter_duplicate(self, df):
        df = df[~df['id'].isin(self.processed_id)]
        return df
        
        
    def insert_data(self, table, df):
        cursor = self.cursor
        # if table not exist, create table
        cursor.execute(f"SHOW TABLES LIKE '{table}'")
        result = cursor.fetchone()
        
        if not result:
            # Create table
            columns = df.columns
            column_types = []
            for col in columns:
                if df[col].dtype == 'O':
                    column_types.append(f'{col} VARCHAR(255)')
                elif df[col].dtype == 'float64':
                    column_types.append(f'{col} FLOAT')
                elif df[col].dtype == 'int64':
                    column_types.append(f'{col} INT')
                elif df[col].dtype == 'datetime64[ns]':
                    column_types.append(f'{col} DATE')
            create_query = f"CREATE TABLE {table} ({','.join(column_types)})"
            cursor.execute(create_query)
            print(f"Table {table} created.")
            
            # Insert data
            placeholders = ','.join(['(' + ','.join(['%s'] * len(df.columns)) + ')'])
            insert_query = f"INSERT INTO {table} ({','.join(df.columns)}) VALUES {placeholders}"
            cursor.executemany(insert_query, df.fillna(0).values.tolist())
            self.connection.commit()
            print(f"{len(df)} records inserted.")
            
            
            cursor.close()
            self.save_record_id(df)
        else:
            try:
                
                df = self.filter_duplicate(df)
                # Insert df_to_update records to the table
                placeholders = ','.join(['(' + ','.join(['%s'] * len(df.columns)) + ')'])
                insert_query = f"INSERT INTO {table} ({','.join(df.columns)}) VALUES {placeholders}"
                for row in df.values.tolist():
                    cursor.execute(insert_query, row)
                
                self.connection.commit()
                print(f"{len(df)} records inserted.")
                self.save_record_id(df)
            except Exception as e:
                print(f"Error inserting data: {str(e)}")
        
            finally:
                cursor.close()
                       

    def execute_query(self, query, params=None):
        cursor = self.cursor
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        self.connection.commit()
             
    def query(self, query):
        cursor = self.cursor

        cursor.execute(query)
        result = cursor.fetchall()

        if not result:  # Check if the result set is empty
            return pd.DataFrame(columns=cursor.column_names)  # Return an empty DataFrame with column names

        # If the result set is not empty, proceed as before
        result = pd.DataFrame(result, columns=cursor.column_names)
        return result
        
    def load_data(self, qry, destination_dir):
        cursor = self.cursor
        cursor.execute(qry)
        result = cursor.fetchall()
        columns = cursor.column_names
        
        result = pd.DataFrame(result)
        result.columns = columns
        result.to_csv(destination_dir, index=False)

    def get_data(self, table):
        result = self.query(f"SELECT * FROM {table}")
        return result


if __name__ == "__main__":
    db = Database()
