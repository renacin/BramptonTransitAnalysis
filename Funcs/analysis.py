# Name:                                            Renacin Matadeen
# Date:                                               05/03/2021
# Title                              Main functions used to analyses collected data
#
# ----------------------------------------------------------------------------------------------------------------------
import os, sys, time, re
import json, requests, ast

import datetime
import sqlite3

import pandas as pd
# ----------------------------------------------------------------------------------------------------------------------


class DataCollection:
    """ This class will return each table within a connected database as a dictionary containing pandas dataframes """


    def __init__(self, database_connection):
        """ On initialization return a dictionary containing each table as a pandas dataframe"""

        conn = sqlite3.connect(database_connection)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")

        db_tables = [
            v[0] for v in cursor.fetchall()
            if v[0] != "sqlite_sequence"
            ]

        self.dataframe_dict = {
            table: pd.read_sql_query(f"SELECT * FROM {table}", conn)
            for table in db_tables
            }

        cursor.close()


    def process_transit_data(self):
        """ This function will identify key statistics from transit data | First explore the data"""

        # Grab transit data from stored dictionary
        transit_df = self.dataframe_dict["TRANSIT_LOCATION_DB"]
        weather_df = self.dataframe_dict["WEATHER_DB"]

        # Start data exploration
        all_routes = transit_df["route_id"].unique().tolist()
        for route in all_routes:
            # Focus On Specific Route; Organize By Trip ID
            temp_df = transit_df[transit_df["route_id"] == route]
            print(f"Route Name: {route}, Num Points {len(temp_df)}")

"""
congestion_level, current_status
current_stop_sequence
stop_id	timestamp
latitude
longitud
bearing
odometer
speed
trip_id
start_time
start_date
schedule_relationship
route_id
id
label
license_plate
weather_id


43.73527, -79.63649
43.67615, -79.72186

"""
