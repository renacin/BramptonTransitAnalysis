# Name:                                            Renacin Matadeen
# Date:                                               01/07/2024
# Title                                Gather Transit Bus Stop Location Data
#
# ---------------------------------------------------------------------------------------------------------------------
import json
import sqlite3
import requests
import pandas as pd
# ---------------------------------------------------------------------------------------------------------------------


def pull_data():
	"""
	This function will navigate to Brapton Transit's NextRide bus API, and pull
	location data given. It will do some formating, then return a pandas Dataframe
	"""

	# Grab Response From Link
	link = r"https://nextride.brampton.ca:81/API/VehiclePositions?format=json"
	response = requests.get(link)

	# Injest As JSON, and Load Into Pandas Dataframe
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

	return bus_loc_df



def to_db(db_path, new_bus_lod_df):
	"""
	This function takes the path to a SQLite3 Database, as well as a pandas dataframe containing
	bus location data, and will upload data to that database. If the database doesn't exist it will
	make it.
	"""

	try:
		# Connect to database check if it has data in it | Create If Not There
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

	except sqlite3.OperationalError as e:
		print(e)

	# Gather Old Data
	old_bus_lod_df = pd.read_sql_query("SELECT * FROM BUS_LOC_DB", conn)

	# Merge Data
	updt_bus_lod_df = pd.concat([old_bus_lod_df, new_bus_lod_df])
	updt_bus_lod_df = updt_bus_lod_df.drop_duplicates(subset=["timestamp", "latitude", "longitude", "label", "id", "vehicle_id", "stop_id", "trip_id", "speed"])
	print(len(updt_bus_lod_df))

	# Upload Data
	updt_bus_lod_df.to_sql("BUS_LOC_DB", conn, if_exists="replace", index=False)


# ---------------------------------------------------------------------------------------------------------------------
def main():

	# Define Needed Path Variables
	out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
	db_path = out_path + "/TestDB.db"

	# Pull Bus Location Data, And Upload To Database
	data_pull = pull_data()
	to_db(db_path, data_pull)


# ---------------------------------------------------------------------------------------------------------------------
# Main Entry Point Into Python Code
if __name__ == "__main__":
    main()
