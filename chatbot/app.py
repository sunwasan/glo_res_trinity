from flask import Flask, request, jsonify
import requests 
from dotenv import load_dotenv
import os 
import time 
from response import generate_response
import json 
import polars as pl
import sys 
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
@app.route('/', methods=['POST'])
def webhook():
    payload = request.json
    events = payload['events']
    lastest_event = events[-1]
    if lastest_event['type'] == 'message':
        timestamp = lastest_event['timestamp']
        current_time = time.time()
        if current_time - timestamp > 60:
            return jsonify({'status': 'ok'})
        else:
            handle_event(lastest_event)


    return jsonify({'status': 'ok'})

def handle_event(event):
    if event['type'] == 'message':
        text = event['message']['text']
        text = text.strip().upper()
        if text in symbols:
            reply_message(event['replyToken'], text)
        elif 'ALERT' in text:
            reply_message(event['replyToken'], text)
        else:
            pass
def reply_message(reply_token, message):
    
    
    # r_json = json.load(open(r'C:\Users\vps\sunny\crm\bloomberg\chatbot\response_helper\box.json'))
    r_json = json.loads(generate_response(message))
    r = requests.post(
    'https://api.line.me/v2/bot/message/reply', 
    headers={
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'
    }, 
    json={
        'replyToken': f"{reply_token}",
        'messages': [
                {
                "type": "flex",
                "altText": f"Report for {message}",
                "contents": r_json
                }
            ]
    }
    )
    
    # if r.json()['message'] != 'Invalid reply token':
    #     print (r.status_code)
    #     print(r.json()['message'])

if __name__ == '__main__':
    app.run(debug=True, port=8765 , host='0.0.0.0')
