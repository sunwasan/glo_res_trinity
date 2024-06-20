import pandas as pd 
import sys 
import os 

file_dir = os.path.dirname(__file__)
update_dir = os.path.join(file_dir, '..', '..')
project_dir = os.path.join(update_dir, '..')

dirs = [file_dir, project_dir]
[sys.path.append(d) for d in dirs if d not in sys.path]
    
import json

from helpers.Database import Database
from helpers.Gdrive import Gdrive
destination_dir = data_dir = os.path.join(project_dir, 'data')

def load_gdrive():
    gdrive = Gdrive()
    gdrive.load_files()

def load_data(date:str = pd.to_datetime('today').strftime('%Y-%m-%d')):
    db = Database()
    df = db.query(f"SELECT * FROM bloomberg WHERE DATE(create_at) = '{date}'")
    df['date'] = pd.to_datetime(df['date'])
    current_year = pd.Timestamp.now().year
    df = df[df['date'].dt.year == current_year]
    df.to_csv(os.path.join(destination_dir, 'bloomberg.csv'), index=False)
    
    return df

if __name__ == '__main__':
    load_data()