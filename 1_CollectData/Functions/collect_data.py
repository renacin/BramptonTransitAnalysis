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
			u_id                  TEXT,
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
			CREATE TABLE IF NOT EXISTS U_ID_TEMP (
			u_id                    TEXT, timestamp               TEXT);
			''')

		except sqlite3.OperationalError as e:
			print(e)


		# Connect to database check if it has data in it | Create If Not There
		try:
			self.conn.execute(
			'''
			CREATE TABLE IF NOT EXISTS DB_META_DT (
			time                    TEXT, time_2_comp             TEXT);
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

		# Injest As JSON, and Load Into Pandas Dataframe, Include Timeout
		timeout_val = 2
		try:
			response = requests.get(self.bus_loc_url, timeout=timeout_val)
			data = json.loads(response.text)
			resp_tsmp = data["header"]["timestamp"]
			bus_loc_df = pd.json_normalize(data["entity"])

			# Rename Columns With Periods In Name
			bus_loc_df = bus_loc_df.rename(columns={
				'vehicle.trip.trip_id': 'trip_id', 'vehicle.trip.start_time': 'start_time', 'vehicle.trip.start_date': 'start_date',
				'vehicle.trip.schedule_relationship': 'schedule_relationship', 'vehicle.trip.route_id': 'route_id',
				'vehicle.position.latitude': 'latitude', 'vehicle.position.longitude': 'longitude', 'vehicle.position.bearing': 'bearing', 'vehicle.position.odometer': 'odometer', 'vehicle.position.speed': 'speed',
				'vehicle.current_stop_sequence': 'current_stop_sequence', 'vehicle.current_status': 'current_status', 'vehicle.timestamp': 'timestamp',
				'vehicle.congestion_level': 'congestion_level', 'vehicle.stop_id': 'stop_id', 'vehicle.vehicle.id': 'vehicle_id', 'vehicle.vehicle.label': 'label',
				'vehicle.vehicle.license_plate': 'license_plate'})

			# Create A Datetime So We Know The Exact Time In Human Readable Rather Than Timestamp From EPOCH
			bus_loc_df["dt_colc"] = pd.to_datetime(bus_loc_df["timestamp"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')

			# Create A U_ID Column Based On Route ID, Vehicle ID, And Timestamp To Act As A Unique ID For The Table
			bus_loc_df["u_id"] = bus_loc_df["route_id"] + "_" + bus_loc_df["vehicle_id"] + "_" + bus_loc_df["timestamp"].astype(str)

			# Upload New Data To An Intermediary Temp Table, Check If The U_IDs Are In A Cache From 10 Min Ago, If Not Add To Database
			bus_loc_df.to_sql('bus_temp', self.conn, if_exists='replace', index=False)
			self.conn.execute("""
			    INSERT INTO BUS_LOC_DB(u_id, id, is_deleted, trip_update, alert, trip_id, start_time,
									   start_date, schedule_relationship, route_id, latitude, longitude, bearing,
									   odometer, speed, current_stop_sequence, current_status, timestamp, congestion_level,
									   stop_id, vehicle_id, label, license_plate, dt_colc)
				SELECT
					A.u_id,                  A.id,             A.is_deleted,
					A.trip_update,           A.alert,          A.trip_id,
					A.start_time,            A.start_date,     A.schedule_relationship,
					A.route_id,              A.latitude,       A.longitude,
					A.bearing,               A.odometer,       A.speed,
					A.current_stop_sequence, A.current_status, A.timestamp,
					A.congestion_level,      A.stop_id,        A.vehicle_id,
					A.label,                 A.license_plate,  A.dt_colc

				FROM
					bus_temp AS A

				WHERE NOT EXISTS (
					SELECT u_id FROM U_ID_TEMP AS B
					WHERE B.u_id = A.u_id)
			""")
			self.conn.execute('DROP TABLE IF EXISTS bus_temp')
			self.conn.commit()

			# Combine U_IDs From New Data & U_IDs In Most Recent Cache
			all_uids = pd.concat([pd.read_sql_query("SELECT * FROM U_ID_TEMP", self.conn),
								  bus_loc_df[["u_id", "timestamp"]]
								  ])

			# Sort, Where The Most Recent U_IDs Are At The Top, Remove Duplicates
			all_uids["timestamp"] = all_uids["timestamp"].astype('int')
			all_uids = all_uids.sort_values(by="timestamp", ascending=False)
			all_uids = all_uids.drop_duplicates()

			# Find The Max Time Stamp, And Only Keep Rows A Couple Of Min Back From That Value
			min_back = 8
			max_timestamp = all_uids["timestamp"].max() - (min_back * 60)
			all_uids = all_uids[all_uids["timestamp"] >= max_timestamp]

			# Now That We Have
			all_uids.to_sql('U_ID_TEMP', self.conn, if_exists='replace', index=False)

			# Size After & Time To Complete
			dt_string = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
			time_to_comp_sec = round((time.time() - start_time), 2)

			# Print Details Of Datapull. How Are Things Going?
			print(f"Time: {dt_string}, Time To Complete: {time_to_comp_sec} Seconds")

			# Upload Metadata To Database
			self.conn.execute(f"""INSERT INTO DB_META_DT VALUES ('{str(dt_string)}', '{str(time_to_comp_sec)}')""")
			self.conn.commit()


		except requests.exceptions.Timeout:
			# Size After & Time To Complete
			dt_string = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
			print(f"Time: {dt_string}, Time To Complete: {timeout_val} Seconds")

			# Upload Metadata To Database
			self.conn.execute(f"""INSERT INTO DB_META_DT VALUES ('{str(dt_string)}', '{str(timeout_val)}')""")
			self.conn.commit()


		except Exception as e:
			print(f"Error: {e}")
			pass


	# -------------------------- Public Function 2 -----------------------------
	def xprt_data(self, out_path, out_table, dup_col, input_val=True):
		"""
		When called, this function will gather all data in a given table, format
		the data in that data table, export it as a CSV to a given path, and then
		empty out the the chosen table if the appropriate choice is given.
		"""

		# Define Needed Variables
		dt_nw = datetime.now().strftime("%d-%m-%Y")
		tm_nw = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
		db_path = out_path + f"/{out_table}_{dt_nw}.csv"

		# Read Data From Defined Database, Remove Duplicates
		df = pd.read_sql_query(f"SELECT * FROM {out_table}", self.conn)
		df = df.drop_duplicates()

		# We Need To Keep An A Version Of The Input Dataframe With No Rows & Just Columns
		empty_df = df.iloc[:0].copy()

		# If List Then All Cols In List
		if type(input_val) == list:
			df = df[input_val]
			df = df.drop_duplicates(subset=[dup_col])

		# If Value = True Then All
		elif input_val == True:
			pass

		# If Invalid, Let The User Know
		else:
			print("Invalid Arguement")
			raise ValueError("Invalid Choice: Choose Either List Of Columns, Or True")

		# Export Data
		df.to_csv(db_path, index=False)
		del df

		# Write Over DB Table So It's Now Empty
		empty_df.to_sql(f"{out_table}", self.conn, if_exists="replace", index=False)
		print(f"Time: {tm_nw}, Data Successfully Export & DB Table - {out_table} Cleaned")
