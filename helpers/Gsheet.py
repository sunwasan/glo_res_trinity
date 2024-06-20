"""
automatesheet@gold-chassis-398713.iam.gserviceaccount.com

"""

import os 
import sys 

file_dir = os.path.dirname(__file__)
project_dir = os.path.join(file_dir, '..')

if file_dir not in sys.path:
    sys.path.append(file_dir)

    
import json 

import gspread
from oauth2client.service_account import ServiceAccountCredentials
# import set_with_dataframe
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
 
creds_path = os.path.join(file_dir, 'cred.json')

class Gsheet ():
    """ 
    init 
        - sheet_url : url of the google sheet
        - creds_path : path to the credentials file
        - client : gspread client
        - worksheet : worksheet object
        - sheets : list of all sheets in the worksheet
    
    function 
        - get_sheet(sheet_name) : get sheet by name
        - set_sheet(sheet_name, df) : set sheet by name
        - add_sheet(sheet_name, df) : add new sheet
    """

    def __init__(self, sheet_url):
        self.creds_path = creds_path
        self.sheet_url = sheet_url
        self.get_client()
        self.worksheet = self.client.open_by_url(self.sheet_url)
        self.get_sheets()

    def get_client(self):
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_path, self.scope)
        self.client = gspread.authorize(self.creds)
    
    def get_sheets(self):
        """ 
        Get all sheets name in worksheet
        """
        self.sheets = self.worksheet.worksheets()
        
        
    def get_sheet(self, sheet_name):
        """
        Get sheet by name
        """
        self.sheet = self.worksheet.worksheet(sheet_name)
        data = self.sheet.get_all_values()
        df = pd.DataFrame(data)
        
        return df
    
    def set_sheet(self, sheet_name, df):
        """
        Set sheet by name
        """
        self.sheet = self.worksheet.worksheet(sheet_name)
        set_with_dataframe(self.sheet, df)
    
    def add_sheet(self, sheet_name, df:pd.DataFrame = None):
        """
        Add new sheet
        """
        titles = list(set([sheet.title for sheet in self.sheets]))
        if not sheet_name in titles:
            if df is not None:
                self.worksheet.add_worksheet(title=sheet_name, rows=df.shape[0], cols=df.shape[1])
                self.set_sheet(sheet_name, df)
            else:
                self.worksheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        else:
            print(f'{sheet_name} already exists')