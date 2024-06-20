import os 
import sys 

file_dir = os.path.dirname(__file__)
dirs = [file_dir]
sys.path.extend(d for d in dirs if d not in sys.path)

from symbol import get_sym
from alert import alert