import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np
import difflib


#===============================================================================
# Step #1: Read In Cleaned Data
#===============================================================================
in_data_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Misc/Test_Data.csv"
segment_data = pd.read_csv(in_data_path)

# Saved Data
stops_from_triplinx = ["City Centre Transit Terminal - Platform L", "Hurontario St at Eglinton Ave - NB (Zum Main)", "Hurontario St at Bristol Rd - NB (Zum Main)", "Hurontario St at Matheson Blvd - NB (Zum Main)", "Hurontario St at Britannia Rd - NB (Zum Main)", "Hurontario St at Courtneypark Dr - NB (Zum Main)", "Hurontario St at Derry Rd - NB (Zum Main)", "County Court South - Zum Main Station Stop NB", "County Court North - Zum Main Station Stop NB", "Brampton Gateway Terminal Route 502 NB Stop", "Nanwood - Zum Main Station Stop NB", "Wellington - Zum Main Station Stop NB", "Theatre Lane - Zum Main Station Stop NB", "Vodden - Zum Main Station Stop NB", "Williams - Zum Main Station Stop NB", "Bovaird - Zum Main Station Stop NB", "Sandalwood - Zum Main Station Stop NB", "Sandalwood Loop - Route 502 Stop"]


# Focus On One Specific Route
segment_data = segment_data[(segment_data["ROUTE_ID"] == '502-295') & (segment_data["U_NAME"].str.contains("Weekday"))]
segment_data["CLST_NM"] = segment_data["CUR_STP_NM"].apply(lambda x: difflib.get_close_matches(x, stops_from_triplinx)).str[0]
segment_data["CLST_SCR"] = segment_data.apply(lambda x: difflib.SequenceMatcher(None, segment_data["CUR_STP_NM"], segment_data["CLST_NM"]).ratio(), axis=1)


out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Misc/Test_Data.csv"
segment_data.to_csv(out_path, index=False)

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
