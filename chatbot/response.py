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
    user_input = user_input.strip()
    date_format = re.compile(r'\d{4}-\d{2}-\d{2}')

    #no need to exact match, just need to find the date format
    if date_format.match(user_input):
        response = alert(user_input)
    else:
        response = get_sym(symbol)
        
    return response
    
if __name__ == '__main__':
    user_input = '2024-06-18'
    response = generate_response(user_input)
    print(response)