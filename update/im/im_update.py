import os
import sys

file_dir = os.path.dirname(__file__)
update_dir = os.path.join(file_dir, '..')
bloomberg_dir = os.path.join(update_dir, '..')
dirs = [update_dir, bloomberg_dir, file_dir]

data_dir = os.path.join(bloomberg_dir, 'data')

sys.path.extend(d for d in dirs if d not in sys.path)

from helpers.Gsheet import Gsheet
from dotenv import load_dotenv

load_dotenv()

im_sheet_url = os.getenv("IM_SHEET_URL")


def load_im(load:bool = False):
    wb = Gsheet(im_sheet_url)
    im  = wb.get_sheet('IM')

    im.columns = im.iloc[0]
    im = im[1:]
    im.columns = im.columns.str.lower()
    im = im.dropna(subset='stock symbol')\
        .replace({'': None})\
        .dropna(how='all', axis=1)\
        .dropna(how='all', axis=0)\
        .rename(columns={'stock symbol': 'symbol',
                        'mininum':'minimum'
                        })


    im_cut = im[['symbol', 'minimum', 'leverage', 'total im']]
    im_num = im_cut.copy()

    im_num.iloc[:,1:] = im_cut.iloc[:,1:].replace({',':''}, regex=True).astype(float)
    
    if load:
        im_num.to_csv(os.path.join(data_dir, 'im.csv'), index=False)
        
    return im_num
        
if __name__ == '__main__':
    load_im(load=True)