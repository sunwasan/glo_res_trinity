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
import warnings 
from helpers.Database import Database

warnings.filterwarnings('ignore')

def get_alert(date:str = None):
    db = Database()
    df_raw = db.get_data('bloomberg')

    if not date:
        yesterday = (pd.to_datetime('today') - pd.DateOffset(days=1)).date()
    else :
        yesterday = pd.to_datetime(date).date()

    df_raw['create_at'] = pd.to_datetime(df_raw['create_at']).dt.date
    df_raw['date'] = pd.to_datetime(df_raw['date']).dt.date
    
    df = df_raw.copy()
    df = df[df['date'] <= yesterday]

    df.drop_duplicates(subset=['symbol', 'broker', 'comment', 'target'], keep='first', inplace=True)

    df = df.sort_values(by=['broker', 'symbol', 'date'], ascending=True)

    # -------------------------------- New Listed -------------------------------- #
    df['count'] = df.groupby(['broker', 'symbol'])['symbol'].transform('count')

    new_listed = df[(df['count'] == 1) & (df['date'] == yesterday)]
    new_listed = new_listed[['broker', 'symbol', 'target', 'comment']].reset_index(drop=True)
    new_listed.sort_values('symbol',ascending=True, inplace=True)
    # ---------------------------------- Changes --------------------------------- #
    df['previous_target'] = df.groupby(['broker', 'symbol'])['target'].shift(1)
    df['previous_comment'] = df.groupby(['broker', 'symbol'])['comment'].shift(1)
    df['previous_date'] = df.groupby(['broker', 'symbol'])['date'].shift(1)
    # ----------------------- Change that occured yesterday ---------------------- #
    df.dropna(subset=['previous_target', 'previous_comment'], inplace=True, how ='all')
    yesterday_changes = df[df['date'] == yesterday].copy()
    # ------------------------------ Target Changes ------------------------------ #
    target_changes = yesterday_changes[yesterday_changes['target'] != yesterday_changes['previous_target']]
    target_changes = target_changes[['broker', 'target', 'previous_target', 'symbol', 'date', 'previous_date']]
    target_changes['diff'] = target_changes['target'] - target_changes['previous_target']

    target_changes = target_changes.astype({'target': 'str'})
    target_changes.loc[:,'target'] = target_changes['target'] + ' (' + round(target_changes['diff'],2).astype(str) + ')'
    target_used = target_changes[['broker', 'symbol', 'previous_target', 'target']].reset_index(drop=True)
    target_used.columns = ['broker', 'symbol', 'from', 'to']

    # ------------------------------ Comment Changes ----------------------------- #
    comment_changes = yesterday_changes[yesterday_changes['comment'] != yesterday_changes['previous_comment']]
    comment_changes = comment_changes[['broker', 'comment', 'previous_comment', 'symbol', 'date', 'previous_date']]

    comment_used = comment_changes[['broker', 'symbol', 'previous_comment', 'comment']].reset_index(drop=True)
    comment_used.columns = ['broker', 'symbol', 'from', 'to']
    
    res = {
        'new_listed': new_listed,
        'target': target_used,
        'comment': comment_used,
        'date': yesterday.strftime('%Y-%m-%d')
    }
    
    return res

def alert(date_input:str = None):
    print(date_input)
    res = get_alert(date_input)
    new_listed = res['new_listed']
    comment = res['comment']
    target = res['target']
    date = res['date']
    
    # Make it all astype str
    new_listed = new_listed.astype(str)
    comment = comment.astype(str)
    target = target.astype(str)
    
    
    flex = Flex(date)
    
    
    if new_listed.empty:
        new_listed = pd.DataFrame(columns=['broker', 'symbol', 'target', 'comment'])
    if target.empty:
        target = pd.DataFrame(columns=['broker', 'symbol', 'from', 'to'])
    if comment.empty:
        comment = pd.DataFrame(columns=['broker', 'symbol', 'from', 'to'])
    
        
    comment_component = flex.table_4_col(comment,True)
    target_component = flex.table_4_col(target,True)
    new_listed_component = flex.table_4_col(new_listed,True)

    order_flex = [
        flex.header_component("REPORT FOR "),
        flex.separator_component(),
        flex.green_text("COMMENT"),
        comment_component,
        flex.separator_component(),
        flex.green_text("TARGET"),
        target_component,
        flex.separator_component(),
        flex.green_text("NEW LISTED"),
        new_listed_component,
        flex.separator_component(),
        flex.footer_component()        
    ]
    
    data = flex.generate_flex(order_flex)
    
    json_data = json.dumps(data, indent=4)
    return json_data
    