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

# Find Bus Stops That Are Located At A Main Terminal, Find The Associated Main Bus Terminal
# Columns Needed stop_id where value is non-numeric, and parent_station where value is not null

# Find Main Bus Terminals
main_bus_terminals = bus_stops[~bus_stops["stop_id"].str.isnumeric()]

# Find Bus Stops Located In A Main Bus Terminal
stops_in_main_terminals = bus_stops[~bus_stops["parent_station"].isnull()]
print(stops_in_main_terminals)
