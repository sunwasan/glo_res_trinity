import pandas as pd 
import sys 
import os 

file_dir = os.getcwd()
update_dir = os.path.join(file_dir, '..')
project_dir = os.path.join(update_dir, '..')

dirs = [file_dir, project_dir]
[sys.path.append(d) for d in dirs if d not in sys.path]
    
import json

from helpers.Database import Database

from chatbot.response_helper.get_date import get_date
import requests
from dotenv import load_dotenv
import json 

load_dotenv()

## generate UUID for the retry key
import uuid
uuid = uuid.uuid1()

yesterday = (pd.to_datetime('today') - pd.DateOffset(days=1)).date()

def boardcast_daily(date:str = yesterday.strftime("%Y-%m-%d")):
    line_url = 'https://api.line.me/v2/bot/message/broadcast'

    response = requests.post(line_url, 
                headers={
                    'Authorization': 'Bearer ' + os.getenv('CHANNEL_ACCESS_TOKEN'),
                    'Content-Type': 'application/json' , 
                    'X-Line-Retry-Key': str(uuid)
                    },
                json={
                        'messages': [
                            {
                                'type': 'flex',
                                "altText": f"Report for {date}",
                                'contents': json.loads(get_date(date))
                            }
                        ]
                })
    
    print(response.json())

if __name__ == '__main__':
    boardcast_daily()