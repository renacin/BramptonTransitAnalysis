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
        """ This function will identify key statistics from transit data """

        # Grab transit data from stored dictionary | Start exploration
        transit_df = self.dataframe_dict["TRANSIT_LOCATION_DB"]
        all_routes = transit_df["route_id"].unique().tolist()
        for route in all_routes:

            # Focus On Specific Route; Organize By Trip ID
            temp_df = transit_df[transit_df["route_id"] == route]

            all_trips = temp_df["trip_id"].unique().tolist()
            for trip in all_trips:

                # Focus on specific trip ID
                temp_df = temp_df[temp_df["trip_id"] == trip]
                temp_df = temp_df.sort_values(["timestamp"], ascending=True)

                # Fix Lat/Long Accurary (5 Dec Pts. for high acc, 3 Dec Pts. for low acc and duplicate removal)
                # Remove Duplicate Rows (Mask all columns except for lat & long)
                temp_df = temp_df.sort_values(["timestamp"], ascending=True)
                for cordtype in ["latitude", "longitude"]:

                    # Fix lat long columns for high acc
                    temp_df[cordtype] = temp_df[cordtype].astype(float)
                    temp_df[cordtype] = temp_df[cordtype].round(decimals=5)

                    # Create new columns
                    new_cord_col = f"{cordtype}_Round"
                    temp_df[new_cord_col] = temp_df[cordtype].round(decimals=3)


                temp_df = temp_df.drop_duplicates(subset = ["latitude_Round", "longitude_Round"], keep = 'first')
                del temp_df["latitude_Round"], temp_df["longitude_Round"]
                temp_df.to_csv(r"C:\Users\renac\Desktop\TestData.csv", index=False)


                break
            break

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
