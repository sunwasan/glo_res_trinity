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

from helpers.Flex import Flex
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
    bloomberg['date'] = pd.to_datetime(bloomberg['date'])
    bloomberg = bloomberg[bloomberg['date'].dt.year == pd.Timestamp.now().year]
    bloomberg['date'] = bloomberg['date'].dt.strftime('%Y-%m-%d')

    if bloomberg.empty:
        bloomberg_foreign_component = None
        bloomberg_local_component = None
    else:  
        bloomberg_df = bloomberg[['broker', 'comment', 'target', 'date']]
        bloomberg_df['type'] = bloomberg_df['broker'].map(type_dict)
        
        bloomberg_df['date'] = pd.to_datetime(bloomberg_df['date'])
        bloomberg_df.sort_values('date', ascending=False, inplace=True)
        bloomberg_df['date'] = bloomberg_df['date'].dt.strftime('%Y-%m-%d')
        bloomberg_df.drop_duplicates(subset=['broker', 'comment', 'target'], keep='first', inplace=True)

        for col in bloomberg_df.columns:
            if col == 'target':
                bloomberg_df[col] = bloomberg_df[col].apply(lambda x: f'{x:,.2f}')
            else:
                bloomberg_df[col] = bloomberg_df[col].apply(lambda x: f'{x}')

        bloomberg_df_local = bloomberg_df[bloomberg_df['type'] == 'local'].drop(columns='type').reset_index(drop=True)
        bloomberg_df_foreign = bloomberg_df[bloomberg_df['type'] == 'foreign'].drop(columns='type').reset_index(drop=True)
        
        
        bloomberg_foreign_component = flex.table_4_col(bloomberg_df_foreign, True)
        bloomberg_local_component = flex.table_4_col(bloomberg_df_local, True)
    
    


    # ------------------------------ Summary Component ---------------------------- #
    im = pl.read_csv(os.path.join(data_dir, 'im.csv')).to_pandas()
    im_df = im[im['symbol'] == sym]
    if im_df.empty:
        im_component = None
    else:
        im_df.drop(columns=['symbol'], inplace=True)
        im_df.columns = ['ขั้นต่ำ', 'Leverage', 'ใช้เงิน']

        im_df['ขั้นต่ำ'] = im_df['ขั้นต่ำ'].apply(lambda x: f'{x:,.0f}')
        im_df['ใช้เงิน'] = im_df['ใช้เงิน'].apply(lambda x: f'{x:,.0f}')
        im_df['Leverage'] = im_df['Leverage'].apply(lambda x: f'{x}')
        im_df = im_df.T.reset_index()
        im_df = im_df.T.reset_index(drop=True).T
        
        im_df[2] = ['สัญญา', 'เท่า', 'บาท']
        
        im_component = flex.table_3_1_1(im_df, False)
        

    # ------------------------------ Summary Component ---------------------------- #
    
    # ---------------------------------- Average --------------------------------- #
    avg_sep_local = pd.DataFrame(data=[ bloomberg_df_local['target'].astype(float).mean().round(2)], columns=["Average Target Price"])
    avg_sep_foreign = pd.DataFrame(data=[ bloomberg_df_foreign['target'].astype(float).mean().round(2)], columns=["Average Target Price"])
    
    avg_sep_local = avg_sep_local.T.reset_index().T.reset_index(drop=True).T
    avg_sep_foreign = avg_sep_foreign.T.reset_index().T.reset_index(drop=True).T
    
    avg_sep_component_foreign = flex.generate_df(avg_sep_foreign, False)
    avg_sep_component_local = flex.generate_df(avg_sep_local, False)

    # ----------------------------------- Mode ----------------------------------- #

    most_common_local = bloomberg_df_local.comment.value_counts().iloc[[0]].reset_index()
    most_common_foreign = bloomberg_df_foreign.comment.value_counts().iloc[[0]].reset_index()


    most_common_local['Most Common Comment'] = most_common_local['comment'].astype(str) + ' (' + most_common_local['count'].astype(str) + ')'
    most_common_foreign['Most Common Comment'] = most_common_foreign['comment'].astype(str) + ' (' + most_common_foreign['count'].astype(str) + ')'
    most_common_local = most_common_local[['Most Common Comment']].T.reset_index().T.reset_index(drop=True).T
    most_common_foreign = most_common_foreign[['Most Common Comment']].T.reset_index().T.reset_index(drop=True).T
    
    mode_sep_local_component = flex.generate_df(most_common_local, False)
    mode_sep_foreign_component = flex.generate_df(most_common_foreign, False)
    

    
    # --------------------------------- Max Date --------------------------------- #
    max_date = bloomberg['date'].max()
    # ------------------------------ Merge Component ----------------------------- #
    order_flex = [
        flex.header_component(),
        flex.separator_component(),
        flex.green_text('BLOCK TRADE'),
        im_component,
        flex.separator_component(),
        flex.green_text('FOREIGN BROKER'),
        bloomberg_foreign_component,
        flex.green_text('FOREIGN SUMMARY'),
        avg_sep_component_foreign,
        mode_sep_foreign_component,
        flex.separator_component(),
        flex.green_text('LOCAL BROKER'),
        bloomberg_local_component,
        flex.green_text('LOCAL SUMMARY'),
        avg_sep_component_local,
        mode_sep_local_component,
        flex.separator_component(),
        flex.footer_component(max_date),


    ]
    
    data = flex.generate_flex(order_flex)
    
    json_data = json.dumps(data, indent=4)
    return json_data