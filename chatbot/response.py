import os
import sys

file_dir = os.path.dirname(__file__)
# chatbot_dir = os.path.join(file_dir, '..')
bloomberg_dir = os.path.join(file_dir, '..')
dirs = [bloomberg_dir, bloomberg_dir, file_dir]

sys.path.extend(d for d in dirs if d not in sys.path)


import polars as pl 
import pandas as pd 
import json 
import re
from response_helper import get_sym
from response_helper import alert

def generate_response(user_input:str):
    symbol = ''.join([c for c in user_input if c.isalpha()]).upper()
    if 'ALERT' in symbol:
        # if any date in string
        date = re.findall(r'\d{4}-\d{2}-\d{2}', user_input)[0]
        if date:
            response = alert(date)
        else:
            response = alert()
    else:
        response = get_sym(symbol)
    return response
    
if __name__ == '__main__':
    user_input = 'WHA'
    response = generate_response(user_input)
    print(response)