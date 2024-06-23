import os 
import sys 

auth_dir = os.path.dirname(__file__)
chatbot_dir = os.path.join(auth_dir, '..')
bloomberg_dir = os.path.join(chatbot_dir, '..')

dirs = [chatbot_dir, bloomberg_dir, auth_dir]
sys.path.extend(d for d in dirs if d not in sys.path)

from helpers.Database import Database

import json 
import polars as pl 
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

MY_USER_ID = os.getenv('MY_USER_ID')

class UserDb(Database):
    def __init__(self):
        super().__init__()
        self.table = 'grt_line_user'
        
    def creat_table(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS grt_line_user (
            user_id VARCHAR(45) NOT NULL PRIMARY KEY,
            line_name VARCHAR(50) NOT NULL,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            permission VARCHAR(10) DEFAULT NULL,
            create_at DATE NOT NULL
        );
        """
        self.execute_query(query)

    def register_user(self, user_id:str, line_name:str, name:str, last_name:str):
        self.insert_user(user_id, line_name, name, last_name)

    def insert_user(self, user_id: str, line_name: str, name: str, last_name: str, permission: str = None):
        qry = """
        INSERT INTO {} (user_id, line_name, first_name, last_name, permission, create_at)
        VALUES (%s, %s, %s, %s, %s, NOW());
        """.format(self.table)  # Use .format() only for table name, not for values
        values = (user_id, line_name, name, last_name, permission)
        self.execute_query(qry, values)

    def get_user_id(self, name:str = None):
        if name:
            name = name.strip().upper()
            first_name = name.split(' ')[0].strip()
            last_name = name.split(' ')[1].strip()
            qry = f"""
            SELECT user_id FROM {self.table}
            WHERE first_name = '{first_name}' AND last_name = '{last_name}';
            """
            return self.query(qry)['user_id'][0]
    
    def grant_permission(self, name:str = None, user_id:str = None, permission:str = 'Y'):
        if name:
            user_id = self.get_user_id(name)
            
        qry = f"""
        UPDATE {self.table}
        SET permission = '{permission}'
        WHERE user_id = '{user_id}';
        """
        r = {}
        try:
            self.execute_query(qry)
            print(f'User {user_id} has been granted permission {permission}.')
            r['status'] = True
            r['user_id'] = user_id
        except Exception as e:
            print(f'Error granting permission: {str(e)}')
            r['status'] = False
            r['user_id'] = user_id
        return r
        
    def revoke_permission(self, name:str = None, user_id:str = None, permission:str = 'N'):
        if name:
            user_id = self.get_user_id(name)
            
        qry = f"""
        UPDATE {self.table}
        SET permission = '{permission}'
        WHERE user_id = '{user_id}';
        """
        try:
            self.execute_query(qry)
            print(f'User {user_id} has been revoked permission {permission}.')
        except Exception as e:
            print(f'Error revoking permission: {str(e)}')
            
    def remove_user(self, name:str = None, user_id:str = None):
        if name:
            user_id = self.get_user_id(name)
            
        qry = f"""
        DELETE FROM {self.table}
        WHERE user_id = '{user_id}';
        """
        try:
            self.execute_query(qry)
            print(f'User {user_id} has been removed.')
        except Exception as e:
            print(f'Error removing user: {str(e)}')
        
    
    def check_exist_user(self, user_id:str):
        qry = f"""
        SELECT * FROM {self.table}
        WHERE user_id = '{user_id}';
        """
        is_exist = False 
        if not self.query(qry).empty:
            is_exist = True
        
        return is_exist
    
    def get_all_user(self):
        qry = f"""
        SELECT * FROM {self.table};
        """
        return self.query(qry)
    
    def get_user(self, user_id:str):
        qry = f"""
        SELECT * FROM {self.table}
        WHERE user_id = '{user_id}';
        """
        return self.query(qry, self.table)
    
    def check_permission(self, user_id:str):
        qry = f"""
        SELECT permission FROM {self.table}
        WHERE user_id = '{user_id}';
        """
        status = self.query(qry)['permission'][0]
        return status
        
    def get_uncheck_user(self, date:str = None):
        if date:
            qry = f"""
            SELECT * FROM {self.table}
            WHERE create_at = '{date}' AND permission IS NULL;
            """
        else:
            qry = f"""
            SELECT * FROM {self.table}
            WHERE permission IS NULL;
            """
        return self.query(qry)
        
if __name__ == '__main__':
    userdb = UserDb()
    userdb.creat_table()
    # userdb.grant_permission(MY_USER_ID, 'A')