from flask import Flask, request, jsonify
import requests 
from dotenv import load_dotenv
import os 
import time 
import json 
import polars as pl
import sys 
import re
load_dotenv()

file_dir = os.path.dirname(__file__)
bloomberg_dir = os.path.join(file_dir, '..')
dirs = [bloomberg_dir, bloomberg_dir, file_dir]

sys.path.extend(d for d in dirs if d not in sys.path)
CHANNEL_ACCESS_TOKEN = os.getenv('CHANNEL_ACCESS_TOKEN')

app = Flask(__name__)

symbols = pl.read_csv(os.path.join(bloomberg_dir, 'data/symbols.csv'))
symbols = symbols['symbol'].to_list()
# Endpoint to receive webhook events from Line

from authorization.UserDb import UserDb
from Response import Response
userdb = UserDb()

@app.route('/', methods=['POST'])
def webhook():
    payload = request.json
    events = payload['events']
    
    lastest_event = events[-1]
    handle_event(lastest_event)



    return jsonify({'status': 'ok'})

def handle_event(event):
    r = Response(event)


if __name__ == '__main__':
    app.run(debug=True, port=8765 , host='0.0.0.0')
