from google.oauth2 import service_account
from googleapiclient.discovery import build

from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import io
import numpy as np 
import json 
import os 
import warnings 
import sys 
import hashlib
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")

file_dir = os.path.dirname(__file__)

if file_dir not in sys.path:
    sys.path.append(file_dir)

from Database import Database
cfg = json.load(open(os.path.join(file_dir,'config.json'), 'r'))
cred_path = os.path.join(file_dir, 'cred.json')
data_dir = cfg['data_dir']
if not os.path.exists(data_dir):
    os.mkdir(data_dir)
    
meta_path = os.path.join(file_dir, 'meta.json')
processed_id_path = os.path.join(file_dir, 'processed_id.csv')

class Gdrive():
    """
    A class for accessing and processing files from Google Drive API.

    This class provides methods to initialize the necessary directories, credentials, and service for accessing Google Drive API.
    It also allows opening a folder in Google Drive, retrieving files, processing files, and updating metadata.

    Attributes:
        data_dir (str): The directory path for storing data files.
        service (googleapiclient.discovery.Resource): The Google Drive API service object.
        items (list): A list of files and folders retrieved from Google Drive.
        meta (dict): The metadata loaded from the 'meta.json' file.
    Functions:
        get_file(item['id'] / file_id ) : to get processed df given file
        load_files() : process all un processing file in gdrive
    """

    def __init__(self):
        """
        Initializes the GetGdrive object.
        
        This method sets up the necessary directories, credentials, and service for accessing Google Drive API.
        It also initializes the `items` list and loads the metadata.
        """
   
        self.data_dir = data_dir
        
        SERVICE_ACCOUNT_FILE = cred_path
        SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        self.service = build('drive', 'v3', credentials=credentials)
        self.open_folder()
        self.meta = self.load_meta()
        

    def load_meta(self):
        """
        Load the metadata from the 'meta.json' file.
        
        Returns:
            dict: The loaded metadata as a dictionary.
        """
        try:
            with open(meta_path, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            data = {}
        return data
         
    def update_meta(self, file_id, file_name, file_path):
        """
        Update the metadata with the information of a new file.

        Args:
            file_id (str): The ID of the file.
            file_name (str): The name of the file.
            file_path (str): The path of the file.

        Returns:
            None
        """
        data = self.load_meta()

        if file_id not in data:
            data[file_id] = []

        data[file_id].append({
            'name': file_name,
            'status': 'processed',
            'path': file_path
        })
        

        with open(meta_path, 'w') as f:
            json.dump(data, f)  
            
        self.meta = data
                
    def open_folder(self):
        """
        Opens a folder in Google Drive using the provided folder URL.

        This method retrieves the list of files and folders inside the specified folder URL
        and stores them in the `self.items` attribute.

        Args:
            self: The instance of the class.

        Returns:
            None
        """
        folder_url = r'1Gnmm1Uf4h0gLDClWJumht7nLnTjmbmEC'
        query = f"'{folder_url}' in parents"

        results = self.service.files().list(q=query, fields="nextPageToken, files(id, name, createdTime)").execute()
        self.items = results.get('files', [])
        
    def download_sheet_file(self, file_id):
        service = self.service
        request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        downloaded = io.BytesIO()
        downloader = MediaIoBaseDownload(downloaded, request)
        done = False
        while done is False:
            _, done = downloader.next_chunk()

        downloaded.seek(0)

        df = pd.read_excel(downloaded, sheet_name=None)  
        return df

    def download_excel_file(self, file_id):
        service = self.service
        request = service.files().get_media(fileId=file_id)
        downloaded = io.BytesIO()
        downloader = MediaIoBaseDownload(downloaded, request)

        done = False
        while done is False:
            _, done = downloader.next_chunk()

        downloaded.seek(0)
        df = pd.read_excel(downloaded, sheet_name=None)
        return df
        
    def open_file(self, item):
        """
        Opens a file from Google Drive and returns the content as pandas DataFrames.

        Args:
            item (dict): A dictionary containing information about the file.

        Returns:
            dict: A dictionary of pandas DataFrames, where the keys are the sheet names and the values are the corresponding DataFrames.
        """
        file_name = item['name']
        create_at = item['createdTime']
        if 'TARGET' in file_name.upper():
            print("file openned :",True)
            file_id = item['id']
            try:
                type_file = "SHEET FILE"
                # print("SHEET FILE")
                df = self.download_sheet_file(file_id)
            except:
                type_file = "EXCEL FILE"
                print("NOT SHEET FILE TRY EXCEL FILE")
                df = self.download_excel_file(file_id)
            print(f"file type: {type_file}")
            df['created_at'] = create_at
            return df
        else:
            return pd.DataFrame()
    
    
    
    def process_file(self, item, commit: bool = True  ):
        """
        Process a file and return a DataFrame containing the prepped sheets.

        Args:
            item (dict): A dictionary containing information about the file.

        Returns:
            dict: A dictionary containing the prepped DataFrame and the file path.

        """
        file_id = item['id']
        file_name = item['name']
        # Turn in to thai time zone 
        created_at  = item['createdTime']
        created_at = datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.%fZ')
        created_at = pytz.utc.localize(created_at).astimezone(pytz.timezone('Asia/Bangkok'))        
        created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"process file: {file_name}")

        def prep_sheet(sheet_df:pd.DataFrame, sheet_name:str):
            symbol = ''.join([t for t in sheet_name if t.isalpha()])
            df = sheet_df.copy()
            columns = ['broker', 'analyst', 'comment', 'target', 'date']
            df = df.dropna(how='all', axis=0)\
                .dropna(how='all', axis=1)\
                .replace('#NAME?', np.nan)
            df.columns = columns
            df = df.dropna(subset=['broker', 'date'])
            df['symbol'] = symbol
            
            #drop row that target not a number
            df = df[df['target'].apply(lambda x: str(x).replace('.','',1).isdigit())]

            df['create_at'] = created_at
            
            return df
        
        dfs = self.open_file(item)
        sheets = list(dfs.keys())
        
        all_prep_sheets = []
        for s in sheets:
            try:
                sheet_df = dfs[s]
                preped_sheet = prep_sheet(sheet_df, s)
                all_prep_sheets.append(preped_sheet)
            except Exception as e :
                pass
            
        full_sheets = pd.concat(all_prep_sheets)
        full_sheets['id'] = full_sheets['date'].astype(str).str.strip().str.lower() + full_sheets['symbol'].str.strip().str.lower() + full_sheets['broker'].str.strip().str.lower()
        full_sheets['id'] = full_sheets['id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        
        
        file_path = os.path.join(self.data_dir, f'{file_name}_{file_id}.csv')
        full_sheets.drop_duplicates(inplace=True)
        
        if commit:
            full_sheets.to_csv(file_path, index=False)
            db = Database()
            db.insert_data('bloomberg',full_sheets.fillna(0))
        
        return {'df': full_sheets, 'path': file_path}
    
    def load_files(self):
        
        """
        Loads files from the items list and processes them if they are not already processed.
        Updates the meta information for each processed file.

        Args:
            None

        Returns:
            None
        """
        print("Start load files")
        items = self.items 
        meta = self.meta
        report = {}
        for item in items:
            file_name = item['name'] 
            file_id = item['id']

            if meta:
                try:
                    item_status = meta.get(file_id, {})[0].get('status')
                except:
                    item_status = None
            else:
                item_status = None
                
            if item_status != 'processed' and 'TARGET' in file_name.upper():
                res = self.process_file(item)
                file_path = res['path']
                self.update_meta(file_id, file_name, file_path)
                report[file_name] = 'updated'
        
                    
            else:
                report[file_name] = 'ignored'
                
        return report

    def get_file(self, file_key: str):
        """
        Retrieves a file from the Google Drive based on the provided file key.

        Args:
            file_key (str): The key of the file to retrieve.

        Returns:
            pandas.DataFrame: The contents of the file as a DataFrame if the file is found.
            None: If the file is not found.

        Raises:
            None

        """
        meta = self.meta 
        keys = meta.keys()
        items = self.items
        raw_keys = [i['id'] for i in items]
        if file_key in keys:
            file_info = meta[file_key]
            file_path = file_info[0]['path']
            df = pd.read_csv(file_path)
            return df
        elif file_key in raw_keys:
            for item in items:
                if item['id'] == file_key:
                    res = self.process_file(item)
                    file_path = res['path']
                    self.update_meta(item['id'], item['name'], file_path)
                    return res['df']
        else:
            print(f"{file_key} not found")
        
    

    

    
if __name__ == '__main__':
 
    gdrive = Gdrive()
    gdrive.load_files()
    
    # items = gdrive.items
    # print(items)
