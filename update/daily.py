import sys 
import os 

file_dir = os.path.dirname(__file__)

from bloomberg.alert import boardcast_alert
from bloomberg.bloomberg_update import load_gdrive , load_data
from im.broker_type import load_broker
from im.im_update import load_im


def main():
    print("Start updating data")
    load_gdrive()
    print("Load Gdrive")
    load_data()
    print("Load Data")
    load_broker()
    print("Load Broker")
    load_im()
    print("Load IM")
    boardcast_alert()
    print("Boardcast Alert")
    
if __name__ == '__main__':
    main()
