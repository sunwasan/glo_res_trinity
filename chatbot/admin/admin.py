
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

MY_USER_ID = os.getenv('MY_USER_ID')
from authorization.UserDb import UserDb

userdb = UserDb()

class Admin():
    def __init__(self, adminId:str = MY_USER_ID):
        self.status = 'A'
        self.adminId = adminId
        
    def grant_permission(self, line_name:str):
        r = userdb.grant_permission(line_name)
        if r['status'] == True:
            userId = r['user_id']
            self.send_message_to(userId, 'Permission Granted')
        else:
            self.send_message_to(userId, 'Permission Denied')
    
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
    
    def all_user(self):
        all_user = userdb.get_all_user()
        name = all_user['first_name'] + ' ' + all_user['last_name']
        all_user['name'] = name
        full_message = ""
        for i in range(len(all_user)):
            full_message += f"-{name[i]}\n"
        return full_message
    
    
    def unchecked_user(self, date:str = None):
        uncheck_user = userdb.get_uncheck_user(date)
        return uncheck_user
 
            
    def grant_permission(self, name:str, permission:str = 'Y'):
        r = userdb.grant_permission(name = name, permission = permission)
        user_id = r['user_id']
        if r['status'] == True:
            self.send_message_to(user_id, 'Permission Granted')
        else:
            self.send_message_to(user_id, 'Permission Denied')
            
           
if __name__ == '__main__':
    admin = Admin()
    unchecked_user = admin.unchecked_user()
    print(unchecked_user)
    # all_user = admin.all_user()
    
    # admin.send_message_to(MY_USER_ID, all_user)
    # admin.grant_permission(name = 'เฉลิมพล เนียมศรี',permission= 'A')