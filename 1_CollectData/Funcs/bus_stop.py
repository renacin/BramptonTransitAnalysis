# Name:                                            Renacin Matadeen
# Date:                                               12/29/2023
# Title                                Gather Transit Route Info, Stops In A Route
#
# ---------------------------------------------------------------------------------------------------------------------
import sqlite3
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
# ---------------------------------------------------------------------------------------------------------------------


def get_rt_info(transit_url):
	"""
	Given a URL, this function navigates to Brampton Transit's Routes & Map Page,
	parses all hrefs related to routes, and returns a pandas dataframe with the
	scraped data.
	"""

	# Navigate To WebPage & Grab HTML Data
	page = requests.get(transit_url)
	soup = BeautifulSoup(page.content, "html.parser")

	# Parse All HTML Data, Find All HREF Tags
	rt_data = []
	for tag in soup.find_all('a', href=True):
		str_ref = str(tag)
		if "https://www.triplinx.ca/en/route-schedules/6/RouteSchedules/" in str_ref:
			for var in ['<a href="', '</a>']:
				str_ref = str_ref.replace(var, "")

			# Parse Out Route Name, Direction, Group, Link, Etc...
			raw_link, dir = str_ref.split('">')
			link = raw_link.split('?')[0]
			full_data = [link + "#trips", dir] + link.split("/")[7:10]
			rt_data.append(full_data)

	# Return A Pandas Dataframe With Route Data
	return pd.DataFrame(rt_data, columns=["RT_LINK", "RT_DIR", "RT_GRP", "RT_NUM", "RT_NM"])



def get_rt_stops(rt_links, rt_names):
	"""
	Given a list of links relating to bus stops visited on certain routes, this
	function navigates through each, pulling information regarding each bus stop
	visited. This function returns a pandas dataframe with all the parsed information.
	"""

	stp_data = []
	num_rts = len(rt_names)
	counter = 1
	for link, name in zip(rt_links, rt_names):

		# Navigate To WebPage, Pull All HTML, Convert To String, Use Regex To Pull All Stop Names
		page = requests.get(link)
		soup = BeautifulSoup(page.content, "html.parser")
		hrefs = soup.find_all(class_="link-to-stop")

		# Create A List Of The Bus Stops Found W/ Name For Join To Main Data
		rw_bs = [name + "###" + str(x).split('">')[1].replace("</a>", "") for x in hrefs]
		stp_data.extend(rw_bs)

		print(f"Parsed: {name}, Progress: {counter}/{num_rts}, Num Stops: {len(rw_bs)}")
		counter += 1

	# Return A Pandas Dataframe With Route Data
	stp_df = pd.DataFrame(stp_data, columns=["RAW_DATA"])
	stp_df[["RT_NM", "STP_NM"]] = stp_df["RAW_DATA"].str.split("###", n=1, expand=True)
	stp_df.drop(["RAW_DATA"], axis=1, inplace=True)

	# Add A Column That Shows Row Number For Each Bus Stop In A Route
	stp_df["RT_STP_NUM"] = stp_df.groupby(["RT_NM"]).cumcount() + 1

	# Add A Column That Shows How Many Bus Stops Are In A Given Route
	num_stps_df = stp_df.groupby("RT_NM", as_index=False).agg(RT_NUM_STPS = ("RT_NM", "count"))
	stp_df = stp_df.merge(num_stps_df, on='RT_NM', how='left')

	return stp_df



def comp_data(parsed_df, downld_df):
	"""
	Given bus stops parsed from Brampton Transit affiliated links (Gives Direction, & Order),
	and data dowloaded from Brampton Transit's Bus Stop Open Data Catalogue (Gives Exact Location),
	compare the two. Are there any Bus Stops from the parsed list that cannot be found in
	Brampton Transit's Open Data Catalogue.

	Identified Comparison Issues:
		1) In some cases "&" is written as "&amp;"
	"""

	# Informed By Comparison, Make Changes
	parsed_df["STP_NM"] = parsed_df["STP_NM"].str.replace('&amp;', '&')

	# Get Unique Bus Stop Names From Parsed Dataframe
	unq_parsed_stps = pd.DataFrame(parsed_df["STP_NM"].unique().tolist(), columns=["Parsed_Bus_Stps"])
	unq_parsed_stps["In_OpenData"] = np.where(unq_parsed_stps["Parsed_Bus_Stps"].isin(downld_df["stop_name"]), "Y", "N")

	# Which Bus Stops Are Missing?
	misng_stps = unq_parsed_stps[unq_parsed_stps["In_OpenData"] == "N"]
	print(f"Parsed DF Len: {len(parsed_df)}, Downloaded DF Len: {len(downld_df)}, Number Of Missing Stops: {len(misng_stps)}")

	return parsed_df



# ---------------------------------------------------------------------------------------------------------------------
def main():

	# Define Needed Path Variables
	out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_Data"

	# Gather All Needed Data
	rt_df = get_rt_info("https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-and-Maps.aspx")
	stp_df = get_rt_stops(rt_df["RT_LINK"].to_list(), rt_df["RT_NM"].to_list())

	# Using All Stops As Main Data, Left Join Route Information
	stp_data_df = stp_df.merge(rt_df, on='RT_NM', how='left')

	# Compare Data From Bus Stops Collected And Brampton Bus Stop Dataset. Which Are Missing?
	path_dwnld_stp_data = out_path + "/BT_BusStops.csv"
	dwnld_stp_data_df = pd.read_csv(path_dwnld_stp_data)
	stp_data_df = comp_data(stp_data_df, dwnld_stp_data_df)

	# Add Information From Brampton Transit Open Data Catalogue's Bus Stop Dataset | Write To SQLite3 DB
	db_path = out_path + "/DataStorage.db"
	con = sqlite3.connect(db_path)
	stp_data_df.to_sql("BusStopsInRoutes", con, if_exists="replace", index=False)
	dwnld_stp_data_df.to_sql("BusStopsInformation", con, if_exists="replace", index=False)



# ---------------------------------------------------------------------------------------------------------------------

# Main Entry Point Into Python Code
if __name__ == "__main__":
    main()
