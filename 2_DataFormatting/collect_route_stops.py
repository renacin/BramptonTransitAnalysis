import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

import networkx as nx
#
# #===============================================================================
# # Step #1: Read In Cleaned Data
# #===============================================================================
# in_data_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Misc/Test_Data.csv"
# segment_data = pd.read_csv(in_data_path)
#
# # Focus On One Specific Route
# segment_data = segment_data[(segment_data["ROUTE_ID"] == '502-295') & (segment_data["U_NAME"].str.contains("Weekday"))]
# test_df = segment_data.groupby(["U_NAME"], as_index=False).agg(
# 			FIRST_STP  = ("CUR_STP_NM", "first"),
# 			LAST_STP   = ("CUR_STP_NM", "last"),
# 			NUM_STP    = ("CUR_STP_NM", lambda s: len(s)),
# 			TRVL_DIST  = ("DST_BTW_STPS", lambda s: round(s.sum(), 2)),
# 			TRVL_TME   = ("TRVL_TIME", lambda s: round(s.sum(), 2)),
#
# )
#
# test_first = test_df.value_counts("FIRST_STP").rename_axis().reset_index(name='COUNT')
# print(test_df)
# print(test_first)
#
#
#
#
#
# stops_from_triplinx = ["City Centre Transit Terminal - Platform L", , "Hurontario St at Eglinton Ave - NB (Zum Main)", "Hurontario St at Bristol Rd - NB (Zum Main)", "Hurontario St at Matheson Blvd - NB (Zum Main)", "Hurontario St at Britannia Rd - NB (Zum Main)", "Hurontario St at Courtneypark Dr - NB (Zum Main)", "Hurontario St at Derry Rd - NB (Zum Main)", "County Court South - Zum Main Station Stop NB", "County Court North - Zum Main Station Stop NB", "Brampton Gateway Terminal Route 502 NB Stop", "Nanwood - Zum Main Station Stop NB", "Wellington - Zum Main Station Stop NB", "Theatre Lane - Zum Main Station Stop NB", "Vodden - Zum Main Station Stop NB", "Williams - Zum Main Station Stop NB", "Bovaird - Zum Main Station Stop NB", "Sandalwood - Zum Main Station Stop NB", "Sandalwood Loop - Route 502 Stop"]



import difflib

varA = 'plaimountain'
varS = ['piaimauntain','sky','skymountain','dog','231']

#it parse varB by letters
best = difflib.get_close_matches(varA, varS)
print(best[0])
score = difflib.SequenceMatcher(None, varA, best[0]).ratio()
print(score)











"""
plt.bar(x=test_first['FIRST_STP'], height=test_first['COUNT'])
plt.show()
"""
# out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Misc/Test_Data_Misc.csv"
# test.to_csv(out_path, index=False)

# data = [23, 45, 56, 78, 213]
# plt.bar([1,2,3,4,5], data)
# plt.show()

# segment_data = segment_data[segment_data["DST_BTW_STPS"] < (segment_data["DST_BTW_STPS"].mean() + segment_data["DST_BTW_STPS"].std())]



# plt.hist(segment_data["DST_BTW_STPS"], bins=15)
# plt.show()
# print(segment_data.columns)

# # Create A Network Map
# edge_list = [(x, y) for x, y in zip(segment_data["CUR_STP_NM"].to_list(), segment_data["NXT_STP_NAME"].to_list())]
# G = nx.from_edgelist(edge_list)
# nx.draw_spring(G, with_labels=True, font_size=8)
# plt.show()


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
