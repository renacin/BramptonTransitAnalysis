import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np
import difflib


#===============================================================================
# Step #1: Read In Bus Stops Within A Given Route Segment
#===============================================================================
in_data_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Data/185_501_BusStops.csv"
segment_data = pd.read_csv(in_data_path)







"""
Example Routes:
11-295,      Num Points 20705
Route Name: 1-295,       Num Points 29225
Route Name: 35-295,      Num Points 5027
Route Name: 2-295,       Num Points 10851
Route Name: 6-295,       Num Points 3191
Route Name: 8-295,       Num Points 5664
Route Name: 30-295,      Num Points 14713
Route Name: 31-295,      Num Points 1963
Route Name: 115-295,     Num Points 6046
Route Name: 52-295,      Num Points 2193
Route Name: 502-295,     Num Points 30129
Route Name: 505-295,     Num Points 12717


"""
