import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np
import networkx as nx
import jenkspy as jp


#===============================================================================
# Step #0: Testing Methodologies
#===============================================================================

# Read In Data
in_data_path_1 = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Data/185_501_BusStops.csv"
bus_stops_in_segment = pd.read_csv(in_data_path_1, usecols=["stop_name"])["stop_name"].tolist()

in_data_path_2 = r"/Users/renacin/Desktop/Misc/main_data.csv"
bus_data = pd.read_csv(in_data_path_2)
# bus_data = bus_data[(bus_data["ROUTE_ID"] == "501-295") & (bus_data["CUR_STP_NM"].isin(bus_stops_in_segment)) & (bus_data["NXT_STP_NAME"].isin(bus_stops_in_segment))]


# Why Can't I See Downtown Terminal In End Data?
testing = bus_data[(bus_data["ROUTE_ID"] == "501-295") & ((bus_data["CUR_STP_NM"] == "Downtown Terminal") | (bus_data["NXT_STP_NAME"] == "Downtown Terminal"))]

out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Data"
main_out_path = out_path + "/testing_downtown_data.csv"
testing.to_csv(main_out_path, index=False)



# # Output Data, What Is The Name Of The Downtown Bus Terminal
# out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Data"
# main_out_path = out_path + "/testing_data.csv"
#
# seg_dict = pd.DataFrame.from_dict({"SEGMENTS": testing["SEGMENT_NAME"].unique().tolist()})
# seg_dict.to_csv(main_out_path, index=False)
#
# Downtown Terminal

# # Apply Fisher-Jenk Algorithm To Find Natural Breaks In Data
# test_df = bus_data["SEGMENT_NAME"].value_counts().rename_axis("SEGMENT_NAME").reset_index(name="COUNT")
# breaks = jp.jenks_breaks(test_df["COUNT"], 2)
# test_df = test_df[test_df["COUNT"] > breaks[1]]
# bus_data = bus_data[bus_data["SEGMENT_NAME"].isin(test_df["SEGMENT_NAME"])]
#
#
# # Make A Connection Graph
# edge_list = [(x, y) for x, y in zip(bus_data["CUR_STP_NM"].to_list(), bus_data["NXT_STP_NAME"].to_list())]
# G = nx.from_edgelist(edge_list)
#
# # Find The Number Of Start/End Points What Kind Of A Segment Is This?
# start_end = [x[0] for x in G.degree if x[1] == 1]
# paths = [path for path in nx.all_simple_paths(G, source=start_end[0], target=start_end[1])]
#
#
# # Define A Function That Takes In A List Of Bus Stops & Determines If It's The Right Order Given Segment Names From A Dataframe
# def true_order(bus_stops_list, unq_segments):
#
# 	# From The List Provided Create A Dataframe & Create A Column That Mimics A Complete Segment Name
# 	data = {"CUR": bus_stops_list}
# 	temp_df = pd.DataFrame.from_dict(data)
# 	temp_df["NXT"] = temp_df["CUR"].shift(-1).ffill()
# 	temp_df["SEGMENT_NAME"] =  temp_df["CUR"] + " -- TO -- " + temp_df["NXT"]
#
# 	if temp_df["SEGMENT_NAME"].isin(bus_data["SEGMENT_NAME"].unique()).any() == False:
# 		data = {"CUR": bus_stops_list[::-1]}
# 		temp_df = pd.DataFrame.from_dict(data)
# 		temp_df["NXT"] = temp_df["CUR"].shift(-1).ffill()
# 		temp_df["SEGMENT_NAME"] =  temp_df["CUR"] + " -- TO -- " + temp_df["NXT"]
#
# 	return temp_df["SEGMENT_NAME"].to_list()
#
#
#
#
# # Segment Has 2 Routes With A Common Bus Stop
# if len(paths) == 1:
#
# 	# Now That We Have Each Route, We Need To Figure Out The Effective Directions of Each
# 	route_1 = true_order(paths[0][0:(len(paths[0])//2) + 1], list(bus_data["SEGMENT_NAME"].unique()))
# 	route_2 = true_order(paths[0][(len(paths[0])//2):], list(bus_data["SEGMENT_NAME"].unique()))
#
# 	# Create A Dataframe For Both & Find Average & Standard Deviation Measures
# 	temp_df_1 = pd.DataFrame.from_dict({"ORDER": [x+1 for x in range(len(route_1))], "SEGMENT_NAME": route_1})
# 	temp_bus_data = bus_data[bus_data["SEGMENT_NAME"].isin(temp_df_1["SEGMENT_NAME"])]
# 	temp_bus_data = temp_bus_data.groupby(["SEGMENT_NAME"], as_index=False).agg(
# 				TM_MEAN = ("TRVL_TIME", lambda s: s.mean()),
# 				TM_STD = ("TRVL_TIME", lambda s: s.std()),
# 	)
#
# 	temp_df_1 = temp_df_1.merge(temp_bus_data, how="left", on=["SEGMENT_NAME"]).dropna()
# 	temp_df_1["RN_TM_MEAN"] = temp_df_1["TM_MEAN"].cumsum()
# 	temp_df_1["RN_TM_MEAN_-STD"] = temp_df_1["RN_TM_MEAN"] - temp_df_1["TM_STD"]
# 	temp_df_1["RN_TM_MEAN_+STD"] = temp_df_1["RN_TM_MEAN"] + temp_df_1["TM_STD"]
#
# 	plt.plot(temp_df_1["SEGMENT_NAME"], temp_df_1["RN_TM_MEAN"], color='#CC4F1B')
# 	plt.fill_between(temp_df_1["SEGMENT_NAME"], temp_df_1["RN_TM_MEAN_-STD"], temp_df_1["RN_TM_MEAN_+STD"], alpha=0.5, edgecolor='#CC4F1B', facecolor='#FF9848')
# 	plt.xticks(rotation=90)
#
# 	out_location = r"/Users/renacin/Documents/Test.pdf"
# 	plt.savefig(out_location, bbox_inches='tight')
#
#
#
#





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
