import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np
from Funcs.func import *


#===============================================================================
# Step #1: Read In DataBase Using A Sqlite3 Connection & Any Additional Data
#===============================================================================
out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Misc/main_data.csv"
db_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Data/DataStorage.db"
bus_stops_csv = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Data/Data_Bus_Stops.csv"
bus_stops = pd.read_csv(bus_stops_csv)
