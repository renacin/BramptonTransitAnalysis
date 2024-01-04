# Name:                                            Renacin Matadeen
# Date:                                               04/25/2021
# Title                  Main functions used within data collection effort will be found in this file
#
# ----------------------------------------------------------------------------------------------------------------------
import os, sys, time, re
import json, requests, ast

import datetime
import sqlite3

import pandas as pd
from bs4 import BeautifulSoup
# ----------------------------------------------------------------------------------------------------------------------


class WebCrawler:
    """ This class will gather data on both bus locations as well as weather data """


    def __init__(self):
        self.transit_url = "https://nextride.brampton.ca:81/API/VehiclePositions?format=json"


    def gather_transit_data(self):
        """ This function takes the JSON link associated with Brampton Transit's GTFS link and parses JSON data from the webpage """

        # Grab data from JSON stream
        res = requests.get(self.transit_url)
        soup = str(BeautifulSoup(res.content, "lxml"))

        # Clean up html tags
        for str_rem in ["<html><body><p>", "</p></body></html>"]:
            soup = soup.replace(str_rem, "")
        all_data = json.loads(soup)

        # Prepare for Pandas DF & Return
        entity_data = [data["vehicle"] for data in all_data["entity"]]
        return [data["vehicle"] for data in all_data["entity"]]


# ----------------------------------------------------------------------------------------------------------------------


class SQLite_Database:
    """ This class will store both transit, and weather data """


    # Initial function, identify, or create a db in the location provided
    def __init__(self, db_location):
        try:
            # Connect to database check if it has data in it | Create two tables joined by primary key
            self.conn = sqlite3.connect(db_location)
            self.conn.execute('''CREATE TABLE IF NOT EXISTS TRANSIT_LOCATION_DB
                (congestion_level TEXT, current_status TEXT, current_stop_sequence TEXT,
                stop_id TEXT, timestamp TEXT, latitude TEXT, longitude TEXT,
                bearing TEXT, odometer TEXT, speed TEXT, trip_id TEXT,
                start_time TEXT, start_date TEXT, schedule_relationship TEXT,
                route_id TEXT, id TEXT, label TEXT, license_plate TEXT,
                weather_id TEXT);''')

            print("Connected To Database")

        except sqlite3.OperationalError as e:
            print(e)


    # Function To Insert Data Into Database
    def addtoDB(self, data_df, table_num):
        cursor = self.conn.cursor()

        # Insert into bus location table
        old_buslocation_df = pd.read_sql_query("SELECT * FROM TRANSIT_LOCATION_DB", self.conn)
        old_weather_df = pd.read_sql_query("SELECT * FROM WEATHER_DB", self.conn)

        new_buslocation_df = data_df
        old_wea_list = list(old_weather_df.iloc[-1])
        new_buslocation_df["weather_id"] = old_wea_list[0]
        updated_buslocation_df = pd.concat([old_buslocation_df, new_buslocation_df])
        updated_buslocation_df.drop_duplicates(subset=["timestamp", "latitude", "longitude", "label", "id"], inplace=True)

        # Print update & append data
        updated_buslocation_df.to_sql("TRANSIT_LOCATION_DB", self.conn, if_exists="replace", index=False)


        self.conn.commit()
        cursor.close()
