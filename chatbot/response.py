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
from response_helper import get_date
import requests

import time

from dotenv import load_dotenv

load_dotenv()

CHANNEL_ACCESS_TOKEN = os.getenv('CHANNEL_ACCESS_TOKEN')

from authorization.UserDb import UserDb

userdb = UserDb()

class Response():
    """ 
    {
        "type": "message",
        "message": {
            "type": "text",
            "id": "513799912531165505",
            "quoteToken": "CwOxK-hPO8uKbhR9uEpEMyCsUUIzzi4t1EDXOwvZdF4ZVR20gOEUbYM1bogxfgyaUAUaQ7W053jY9rfzYo1LkVj1w-4WP9UX5VHtT2BZmdf6D00F3tMkF1Pvj2IFlP5e_gAXrkhhYntjC6OGaTsOGQ",
            "text": "kbank"
    },
        "webhookEventId": "01J10JBNNVJ1SHN2K9WPMG9F7M",
        "deliveryContext": {
            "isRedelivery": True
    },
        "timestamp": 1719079916737,
        "source": {
            "type": "user",
            "userId": "Ue834c0058e991257b0790434d0b6926e"
    },
        "replyToken": "6793343586394455adfc52c6e844432d",
        "mode": "active"
    }
    """
    def __init__ (self, event):

        self.event = event
        print(event)
        self.type = event['type']
        if self.type != 'unfollow':
            self.isredivery = event['deliveryContext']['isRedelivery']
            if not self.isredivery:
                self.userId = event['source']['userId']
                self.replyToken = event['replyToken']
                self.status = None 
                timestamp = event['timestamp']
                self.timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1000))
                
                self.authorize()
                
                self.handle_event()

    
    def authorize(self):    
        is_user = userdb.check_exist_user(self.userId)
        if is_user:
            self.status = userdb.check_permission(self.userId)
            
    

    
    def register(self, text):
        def get_user_profile():
            r = requests.get(
                f'https://api.line.me/v2/bot/profile/{self.userId}',
                headers={
                    'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'
                }
            )
            
            return r.json()
        status = self.status
        if not status:
            user_prof = get_user_profile()
            display_name = user_prof['displayName']
            name = text.split(' ')[1]
            last_name = text.split(' ')[2]
            
            userdb.register_user(self.userId,display_name, name, last_name)
            
            self.send_message('Register Success Wait For Permission')

        else:
            self.send_message('You Already Registered')

    def handle_event(self):
        status = self.status
        ## New User
        if self.type == 'follow' and not status:
            self.send_message('Welcome to Glo Res Trinity\nPlease Register First\nr ชื่อจริง นามสกุล') 
            
        elif self.type == 'message':
            text = self.event['message']['text']
            if 'r' in text:
                self.register(text)
            elif '/' in text:
                self.admin_command(text) 
            elif not status:
                self.send_message('Please Register First\n-> r ชื่อจริง นามสกุล')       
            elif status == 'N':
                self.send_message('Permission Denied Wait For Permission')
            elif status == 'Y' or status == 'A':
                self.send_data(text)    

            
        
    
    def send_message(self, message:str):
        r = requests.post(
        'https://api.line.me/v2/bot/message/reply', 
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'
        }, 
        json={
            'replyToken': f"{self.replyToken}",
            'messages': [
                    {
                    "type": "text",
                    "label": f"{message}",
                    "text": f"{message}"
                    }
                ]
        }
        )
        report = r.json()
        return report 
    


    def send_data(self, user_input:str):
        print(user_input)
        symbol = ''.join([c for c in user_input if c.isalpha()]).upper()
        user_input = user_input.strip()
        date_format = re.compile(r'\d{4}-\d{2}-\d{2}')

        #no need to exact match, just need to find the date format
        if date_format.match(user_input):
            response = get_date(user_input) # send daily report in given date
        else:
            response = get_sym(symbol) # send sym data
            
        r = requests.post(
            'https://api.line.me/v2/bot/message/reply', 
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'
            }, 
            json={
                'replyToken': f"{self.replyToken}",
                'messages': [
                        {
                        "type": "flex",
                        "altText": f"Report for {user_input}",
                        "contents": json.loads(response)
                        }
                    ]
            }
        )
        return r.json()
    
    def send_message_to(self, user_id:str, message:str):
        r = requests.post(
        'https://api.line.me/v2/bot/message/push', 
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {CHANNEL_ACCESS_TOKEN}'
        },
        json={
            'to': user_id,
            'messages': [
                    {
                    "type": "text",
                    "label": f"{message}",
                    "text": f"{message}"
                    }
                ]
        }
        )
        report = r.json()
        return report
    

            
if __name__ == '__main__':
    r = Response()