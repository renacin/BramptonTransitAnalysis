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

		# Given Path, Make Sure Needed Database Exists, Establish Connection To DB
		self.conn =	self.__db_check(db_path)

		# Check To See If Data On Routes Has Been Collected, If It Does Compare, Keep Bigger List
		# TODO

	# -------------------------- Private Functions ----------------------------
	def __db_check(self, db_path):
		""" On instantiation this function will be called. Create a database
		that will store bus location data. This is a private function. It cannot
		be called."""

		# Connect to database check if it has data in it | Create If Not There
		try:
			conn = sqlite3.connect(db_path)
			conn.execute(
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

			return conn

		except sqlite3.OperationalError as e:
			print(e)


# ----------------------------------------------------------------------------------------------------------------------

# Main Entry Point Into Python Code
if __name__ == "__main__":

	# Create A Instance Of The Data Collector
	out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
	db_path = out_path + "/DataStorage.db"
	Collector = DataCollector(db_path)


	"""
	Order of operations for data collector:
	1) Instantiate Needed Variables
	2) On Instantiation Of Class, Given A Path, Create, Or Check Existence Of Database
	3) On Instantiation Get Most Recent Version Of Routes, And Bus Stops. If Data Exists, Keep Larger Dataset

	"""
