import os
import sys

file_dir = os.path.dirname(__file__)
chatbot_dir = os.path.join(file_dir, '..')
bloomberg_dir = os.path.join(chatbot_dir, '..')
dirs = [chatbot_dir, bloomberg_dir, file_dir]

sys.path.extend(d for d in dirs if d not in sys.path)

import polars as pl 
import pandas as pd 
import json 

data_dir = os.path.join(bloomberg_dir,'data')

import warnings 
import numpy as np
warnings.filterwarnings('ignore')
from helpers.Gsheet import Gsheet

from dotenv import load_dotenv
load_dotenv()

BROKER_SHEET_URL = os.getenv("BROKER_SHEET_URL")

def load_broker(load:bool = False):
    gs = Gsheet(BROKER_SHEET_URL)
    df = gs.get_sheet("main")
    df.columns = df.iloc[0]
    df = df[1:]
    df['foreign broker'] = df['foreign broker'].apply(lambda x: x == 'TRUE')
    df['type'] = np.where(df['foreign broker'], 'foreign', 'local')
    
    if load:
        df.to_csv(os.path.join(data_dir, 'broker.csv'), index=False)
    
    return df
    