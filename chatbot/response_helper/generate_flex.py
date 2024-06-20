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

from Flex import Flex
from helpers.Database import Database

import warnings 

warnings.filterwarnings('ignore')

def get_sym(sym:str):
    
    flex = Flex(sym)
    

    # --------------------------- Bloomberg Compopnent --------------------------- #
    broker_list = pl.read_csv(os.path.join(data_dir, 'broker.csv')).to_pandas()
    type_dict = broker_list[['broker', 'type']].set_index('broker').to_dict()['type']
    
    bloomberg = pl.read_csv(os.path.join(data_dir, 'bloomberg.csv')).to_pandas()
    bloomberg = bloomberg[bloomberg['symbol'] == sym]


    if bloomberg.empty:
        bloomberg_foreign_component = None
        bloomberg_local_component = None
    else:  
        bloomberg_df = bloomberg[['broker', 'comment', 'target']]
        bloomberg_df['type'] = bloomberg_df['broker'].map(type_dict)
        
        bloomberg_df_local = bloomberg_df[bloomberg_df['type'] == 'local'].drop(columns='type').reset_index(drop=True)
        bloomberg_df_foreign = bloomberg_df[bloomberg_df['type'] == 'foreign'].drop(columns='type').reset_index(drop=True)
        
        
        bloomberg_foreign_component = flex.table_3_1_1(bloomberg_df_foreign, True)
        bloomberg_local_component = flex.table_3_1_1(bloomberg_df_local, True)
    
    
    # ------------------------------- IM Component ------------------------------- #
    im = pl.read_csv(os.path.join(data_dir, 'im.csv')).to_pandas()
    im_df = im[im['symbol'] == sym]
    if im_df.empty:
        im_component = None
    else:
        im_df.drop(columns=['symbol'], inplace=True)
        im_df.columns = ['ขั้นต่ำ', 'Leverage', 'ใช้เงิน']

        im_df['ขั้นต่ำ'] = im_df['ขั้นต่ำ'].apply(lambda x: f'{x:,.0f} สัญญา')
        im_df['ใช้เงิน'] = im_df['ใช้เงิน'].apply(lambda x: f'{x:,.0f} บาท')
        im_df['Leverage'] = im_df['Leverage'].apply(lambda x: f'{x} เท่า')
        im_df = im_df.T.reset_index()
        im_df = im_df.T.reset_index(drop=True).T
        
        im_component = flex.generate_df(im_df, False)

    # ----------------------------- Average Component ---------------------------- #
    
    db = Database()
    year_df = db.query(f"SELECT * FROM bloomberg WHERE symbol = '{sym}' AND YEAR(date) = YEAR(CURDATE())")
    year_df['date'] = pd.to_datetime(year_df['date'])
    average_target = year_df['target'].mean()
    mode_comment = year_df['comment'].mode().values[0]
    n_mode_comment = year_df['comment'].value_counts().max()
    
    year_res = pd.DataFrame({'Average Target': [average_target], 'Mode Comment': [mode_comment]})
    year_res['Average Target'] = year_res['Average Target'].apply(lambda x: f'{x:,.2f}')
    year_res['Mode Comment'] = year_res['Mode Comment'].apply(lambda x: f'{x} ({n_mode_comment})')
    
    year_res = year_res.T.reset_index()
    year_res = year_res.T.reset_index(drop=True).T
    
    average_component = flex.generate_df(year_res, False)    
    
    # --------------------------------- Max Date --------------------------------- #
    max_date = bloomberg['date'].max()
    # ------------------------------ Merge Component ----------------------------- #
    order_flex = [
        flex.header_component(),
        im_component,
        flex.separator_component(),
        flex.green_text('FOREIGN BROKER'),
        bloomberg_foreign_component,
        flex.separator_component(),
        flex.green_text('LOCAL BROKER'),
        bloomberg_local_component,
        flex.separator_component(),
        flex.green_text('AVERAGE'),
        average_component,
        flex.footer_component(max_date),


    ]
    
    data = flex.generate_flex(order_flex)
    
    json_data = json.dumps(data, indent=4)
    return json_data