# Name:                                            Renacin Matadeen
# Date:                                               01/10/2024
# Title                  Main functions used within data collection effort will be found in this file
#
# ----------------------------------------------------------------------------------------------------------------------
import json
import time
import sqlite3
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
# ----------------------------------------------------------------------------------------------------------------------


class DataCollector:
	""" This class will gather data on both bus locations as well as weather data """


	# -------------------- Functions Run On Instantiation ----------------------
	def __init__(self, db_path, skp_rte_dwn=False, skp_stp_dwn=False):
		""" This function will run when the DataCollector Class is instantiated """

		# Internalize Needed URLs: Bus Location API, Bus Routes, Bus Stops
		self.bus_loc_url = r"https://nextride.brampton.ca:81/API/VehiclePositions?format=json"
		self.bus_routes_url = r"https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-and-Maps.aspx"
		self.bus_stops_url = r"https://opendata.arcgis.com/api/v3/datasets/1c9869fd805e45339a9e3373fc10ce08_0/downloads/data?format=csv&spatialRefId=3857&where=1%3D1"

		# Create A Connection To The Database
		self.conn = sqlite3.connect(db_path)

		# Ensure Database Exists, Grab Recent Bus Stop Info, Grab Route Data
		self.__db_check()

		if skp_stp_dwn == False:
			self.__get_bus_stops()

		if skp_rte_dwn == False:
			self.__get_bus_route()


	# -------------------------- Private Function 1 ----------------------------
	def __db_check(self):
		""" On instantiation this function will be called. Create a database
		that will store bus location data; as well as basic database functionality
		data. This is a private function. It cannot be called."""

		# Connect to database check if it has data in it | Create If Not There
		try:
			self.conn.execute(
			'''
			CREATE TABLE IF NOT EXISTS BUS_LOC_DB (
			id                    TEXT, is_deleted            TEXT, trip_update           TEXT,
			alert                 TEXT, trip_id               TEXT, start_time            TEXT,
			start_date            TEXT, schedule_relationship TEXT, route_id              TEXT,
			latitude              TEXT, longitude             TEXT, bearing               TEXT,
			odometer              TEXT, speed                 TEXT, current_stop_sequence TEXT,
			current_status        TEXT, timestamp             TEXT, congestion_level      TEXT,
			stop_id               TEXT, vehicle_id            TEXT, label                 TEXT,
			license_plate         TEXT, dt_colc               TEXT);
			''')

		except sqlite3.OperationalError as e:
			print(e)


		# Connect to database check if it has data in it | Create If Not There
		try:
			self.conn.execute(
			'''
			CREATE TABLE IF NOT EXISTS DB_META_DT (
			time                    TEXT,
			new_rows                TEXT,
			all_rows                TEXT,
			time_2_comp             TEXT);
			''')

		except sqlite3.OperationalError as e:
			print(e)


	# -------------------------- Private Function 2  ----------------------------
	def __get_bus_stops(self):
		""" On instantiation this function will be called. Using Brampton's Open
		Data API Link, Download, Bus Stop Data To SQLite3 Database. This function
		should only be run on instantiation. """

		# Compare Data From Bus Stops Collected And Brampton Bus Stop Dataset. Which Are Missing?
		dwnld_stp_data_df = pd.read_csv(self.bus_stops_url)
		dwnld_stp_data_df.to_sql("BUS_STP_DB", self.conn, if_exists="replace", index=False)


	# -------------------------- Private Function 3  ---------------------------
	def __get_rts(self):
		"""
		Given a URL, this function navigates to Brampton Transit's Routes & Map Page,
		parses all hrefs related to routes, and returns a pandas dataframe with the
		scraped data.
		"""

		# Navigate To WebPage & Grab HTML Data
		page = requests.get(self.bus_routes_url )
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


	def __get_rt_stops(self, rt_links, rt_names):
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


	def __comp_data(self, parsed_df, downld_df):
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


	def __get_bus_route(self):
		""" On instantiation this function will be called. Using Brampton's Open
		Data API Link, Download, Bus Route Data, And Related Bus Stops, Export To
		SQLite3 Database. This function should only be run on instantiation. """

		# Pull Route Info, Then Related Bus Stop Info
		rt_df = self.__get_rts()
		stp_df = self.__get_rt_stops(rt_df["RT_LINK"].to_list(), rt_df["RT_NM"].to_list())

		# Merge Data
		stp_data_df = stp_df.merge(rt_df, on='RT_NM', how='left')

		# Compare Bus Stop Names, Ensure All Names Are Consistent
		dwnld_stp_data_df = pd.read_sql_query("SELECT * FROM BUS_STP_DB", self.conn)
		stp_data_df = self.__comp_data(stp_data_df, dwnld_stp_data_df)

		# Upload To Database
		stp_data_df.to_sql("BUS_RTE_DB", self.conn, if_exists="replace", index=False)


	# -------------------------- Public Function 1 -----------------------------
	def get_bus_loc(self):
		"""
		When called, this function will navigate to Brampton Transit JSON GTFS
		link, scrape, format, and then upload data to the linked database. It
		will merge old data found in the database keeping new and old records.
		"""

		# What Is The Start Time
		start_time = time.time()

		# Injest As JSON, and Load Into Pandas Dataframe
		response = requests.get(self.bus_loc_url)
		data = json.loads(response.text)
		resp_tsmp = data["header"]["timestamp"]
		bus_loc_df = pd.json_normalize(data["entity"])

		# Rename Columns With Periods In Name
		bus_loc_df = bus_loc_df.rename(columns={
			'vehicle.trip.trip_id': 'trip_id',
			'vehicle.trip.start_time': 'start_time',
			'vehicle.trip.start_date': 'start_date',
			'vehicle.trip.schedule_relationship': 'schedule_relationship',
			'vehicle.trip.route_id': 'route_id',
			'vehicle.position.latitude': 'latitude',
			'vehicle.position.longitude': 'longitude',
			'vehicle.position.bearing': 'bearing',
			'vehicle.position.odometer': 'odometer',
			'vehicle.position.speed': 'speed',
			'vehicle.current_stop_sequence': 'current_stop_sequence',
			'vehicle.current_status': 'current_status',
			'vehicle.timestamp': 'timestamp',
			'vehicle.congestion_level': 'congestion_level',
			'vehicle.stop_id': 'stop_id',
			'vehicle.vehicle.id': 'vehicle_id',
			'vehicle.vehicle.label': 'label',
			'vehicle.vehicle.license_plate': 'license_plate'})

		# Create A Datetime So We Know The Exact Time In Human Readable
		bus_loc_df["dt_colc"] = pd.to_datetime(bus_loc_df["timestamp"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')

		# Gather Old Data
		old_bus_lod_df = pd.read_sql_query("SELECT * FROM BUS_LOC_DB", self.conn)
		len_before = len(old_bus_lod_df)

		# Merge Data
		updt_bus_lod_df = pd.concat([old_bus_lod_df, bus_loc_df])
		updt_bus_lod_df = updt_bus_lod_df.drop_duplicates(subset=["timestamp", "latitude", "longitude", "label", "id", "vehicle_id", "stop_id", "trip_id", "speed"])

		# Upload Data
		updt_bus_lod_df = updt_bus_lod_df.astype(str)
		updt_bus_lod_df.to_sql("BUS_LOC_DB", self.conn, if_exists="replace", index=False)

		# Gather Details. How Are Things Going?
		now = datetime.now()
		dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
		time_to_comp_sec = round((time.time() - start_time), 2)
		new_rows = len(updt_bus_lod_df) - len_before
		tot_rows = len(updt_bus_lod_df)

		# Print Details Of Datapull. How Are Things Going?
		print(f"Time: {dt_string}, Total New Rows: {new_rows}, Total Rows {tot_rows}, Time To Complete: {time_to_comp_sec} Seconds")

		# Append Metadata
		old_db_meta = pd.read_sql_query("SELECT * FROM DB_META_DT", self.conn)
		new_db_meta = pd.DataFrame.from_dict({"time": [dt_string], "new_rows": [new_rows], "all_rows": [tot_rows], "time_2_comp": [time_to_comp_sec]})
		updt_db_meta = pd.concat([old_db_meta, new_db_meta])

		updt_db_meta = updt_db_meta.drop_duplicates(subset=["time", "new_rows", "all_rows", "time_2_comp"])
		updt_db_meta = updt_db_meta.astype(str)

		updt_db_meta.to_sql("DB_META_DT", self.conn, if_exists="replace", index=False)


# ----------------------------------------------------------------------------------------------------------------------

# Main Entry Point Into Python Code
if __name__ == "__main__":

	# Dine Paths Needed
	out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
	db_path = out_path + "/DataStorage.db"

	# Create An Instance Of The Data Collector
	Collector = DataCollector(db_path, skp_rte_dwn=True, skp_stp_dwn=True)

	# For Testing
	Collector.get_bus_loc()





"""
	# Keept Collecting Data, Make Exceptions For Error Catching
	while True:
		try:
			Collector.get_bus_loc()
			time.sleep(15)

		except KeyboardInterrupt:
			now = datetime.now()
			dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
			print(f" Stoped By User: {dt_string}")
			break

		except Exception as e:
			print(f"Error: {e}")
			break
"""
