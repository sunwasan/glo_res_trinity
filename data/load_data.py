import pandas as pd 
import sys 
import os 

file_dir = os.path.dirname(__file__)
project_dir = os.path.join(file_dir, '..')

dirs = [file_dir, project_dir]
[sys.path.append(d) for d in dirs if d not in sys.path]
    
import json

from helpers.Database import Database

destination_dir = file_dir

def load_data(date:str = pd.to_datetime('today').strftime('%Y-%m-%d')):
    db = Database()
    df = db.query(f"SELECT * FROM bloomberg WHERE DATE(create_at) = '{date}'")
    df['date'] = pd.to_datetime(df['date'])
    current_year = pd.Timestamp.now().year
    df = df[df['date'].dt.year == current_year]
    df.to_csv(os.path.join(destination_dir, 'bloomberg.csv'), index=False)
    

    
    return df

def load_sym():
    db = Database()
    df = db.query("SELECT DISTINCT(symbol) FROM bloomberg")
    df.to_csv(os.path.join(destination_dir, 'symbols.csv'), index=False)
    
    return df

if __name__ == '__main__':
    # load_data()
    load_sym()
