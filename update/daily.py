import sys 
import os 

file_dir = os.path.dirname(__file__)

from bloomberg.alert import boardcast_alert
from bloomberg.bloomberg_update import load_gdrive , load_data
from im.broker_type import load_broker
from im.im_update import load_im


def main():
    load_gdrive()
    load_data()
    load_broker()
    load_im()
    boardcast_alert()
    
if __name__ == '__main__':
    main()
