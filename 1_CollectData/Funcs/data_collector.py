# Name:                                            Renacin Matadeen
# Date:                                               01/10/2024
# Title                  Main functions used within data collection effort will be found in this file
#
# ----------------------------------------------------------------------------------------------------------------------
import json
import sqlite3
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
# ----------------------------------------------------------------------------------------------------------------------


class DataCollector:
	""" This class will gather data on both bus locations as well as weather data """


	# -------------------- Functions Run On Instantiation ----------------------
	def __init__(self, db_path):
		""" This function will run when the DataCollector Class is instantiated """

		# Internalize Needed URLs: Bus Location API, Bus Routes, Bus Stops
		self.bus_loc_url = r"https://nextride.brampton.ca:81/API/VehiclePositions?format=json"
		self.bus_routes_url = r"https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-and-Maps.aspx"
		self.bus_stops_url = r"https://opendata.arcgis.com/api/v3/datasets/1c9869fd805e45339a9e3373fc10ce08_0/downloads/data?format=csv&spatialRefId=3857&where=1%3D1"

		# Create A Connection To The Database
		self.conn = sqlite3.connect(db_path)

		# Given Path, Make Sure Needed Database Exists, Establish Connection To DB
		self.__db_check()

		# Grab The Most Recent Bus Stop Data From Brampton Open Data
		self.__get_bus_stops()



	# -------------------------- Private Functions -----------------------------
	def __db_check(self):
		""" On instantiation this function will be called. Create a database
		that will store bus location data. This is a private function. It cannot
		be called."""

		# Connect to database check if it has data in it | Create If Not There
		try:
			self.conn.execute(
			'''
			CREATE TABLE IF NOT EXISTS BUS_LOC_DB (
			id                    TEXT,
			is_deleted            TEXT,
			trip_update           TEXT,
			alert                 TEXT,
			trip_id               TEXT,
			start_time            TEXT,
			start_date            TEXT,
			schedule_relationship TEXT,
			route_id              TEXT,
			latitude              TEXT,
			longitude             TEXT,
			bearing               TEXT,
			odometer              TEXT,
			speed                 TEXT,
			current_stop_sequence TEXT,
			current_status        TEXT,
			timestamp             TEXT,
			congestion_level      TEXT,
			stop_id               TEXT,
			vehicle_id            TEXT,
			label                 TEXT,
			license_plate         TEXT,
			dt_colc               TEXT
			);
			''')

		except sqlite3.OperationalError as e:
			print(e)


	# --------------------------------------------------------------------------
	def __get_bus_stops(self):
		""" On instantiation this function will be called. Using Brampton's Open
		Data API Link, Download, Bus Stop Data To SQLite3 Database. This function
		should only be run on instantiation. """

		# Compare Data From Bus Stops Collected And Brampton Bus Stop Dataset. Which Are Missing?
		dwnld_stp_data_df = pd.read_csv(self.bus_stops_url)
		dwnld_stp_data_df.to_sql("BUS_STP_DB", self.conn, if_exists="replace", index=False)


	# --------------------------------------------------------------------------








# ----------------------------------------------------------------------------------------------------------------------

# Main Entry Point Into Python Code
if __name__ == "__main__":

	# Dine Paths Needed
	out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
	db_path = out_path + "/DataStorage.db"

	# Create An Instance Of The Data Collector
	Collector = DataCollector(db_path)
