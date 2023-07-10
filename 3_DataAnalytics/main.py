import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

import networkx as nx

#===============================================================================
# Step #1: Read In Cleaned Data
#===============================================================================
in_data_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Misc/Test_Data.csv"
segment_data = pd.read_csv(in_data_path)

# Focus On One Specific Route
segment_data = segment_data[segment_data["ROUTE_ID"] == '1-295']

# Create A Network Map
edge_list = [(x, y) for x, y in zip(segment_data["CUR_STP_NM"].to_list(), segment_data["NXT_STP_NAME"].to_list())]
G = nx.from_edgelist(edge_list)
nx.draw_spring(G, with_labels=True, font_size=8)
plt.show()


# print(len(unique_values))
# seg_dir = segment_data[["CUR_STP_NM", "NXT_STP_NAME"]].drop_duplicates()
# print(seg_dir)


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
