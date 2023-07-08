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
        self.weather_url = "https://forecast.weather.gov/data/obhistory/metric/CYYZ.html"


    def gather_weather_data(self):
        """ This function takes the weather link provided and grabs weather data for brampton """

        # Grab raw HTML data from weather page
        wea_html = requests.get(self.weather_url)
        df_list = pd.read_html(wea_html.text) # this parses all the tables in webpages to a list
        weather_df = df_list[3]
        cleaned_weather_df = weather_df.iloc[3:4]

        # Change Column Names
        new_columns = ["Date", "Time", "Wind", "Visib", "Weather",
                       "SkyCond", "AirTemp", "DewPoint", "HrMax6",
                       "HrMin6", "RelHum", "WindChill", "HeatIndex",
                       "AltPres", "SeaPres", "Precip1hr", "Precitp3hr",
                       "Precip6hr"]

        # Final clean up
        cleaned_weather_df.columns = new_columns
        cleaned_weather_df = cleaned_weather_df.reset_index()
        cleaned_weather_df.drop(columns=["index"], inplace=True)
        cleaned_weather_df["weather_id"] = str(0)
        cleaned_weather_df["Month"] = str(datetime.datetime.now().strftime("%m"))
        cleaned_weather_df["Year"] = str(datetime.date.today().year)

        return cleaned_weather_df[["weather_id", "Year", "Month", "Date", "Time", "Wind", "Visib", "Weather", "SkyCond", "AirTemp", "DewPoint", "HrMax6", "HrMin6", "RelHum", "WindChill", "HeatIndex", "AltPres", "SeaPres", "Precip1hr", "Precitp3hr", "Precip6hr"]]


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


    def clean_transit_data(self, raw_data):
        """ This function takes the parsed JSON data, cleans it and returns as a pandas dataframe """

        # Create pandas DF with data
        df = pd.DataFrame(raw_data)

        # Expand columns that contain data as dicts
        for col_2_xpand in ["position", "trip", "vehicle"]:

            # Expand column data into dictionary
            temp_df = df[col_2_xpand].astype("str")
            temp_df = temp_df.apply(lambda x: ast.literal_eval(x))
            temp_df = temp_df.apply(pd.Series)

            # Add columns to orginal dataset & remove old columns
            df = pd.concat([df, temp_df], axis=1)
            df.drop(columns=[col_2_xpand], inplace=True)

        return df



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

            self.conn.execute('''CREATE TABLE IF NOT EXISTS WEATHER_DB
                (weather_id TEXT, Year TEXT, Month TEXT, Date TEXT, Time TEXT,
                Wind TEXT, Visib TEXT, Weather TEXT, SkyCond TEXT, AirTemp TEXT,
                DewPoint TEXT, HrMax6 TEXT, HrMin6 TEXT, RelHum TEXT,
                WindChill TEXT, HeatIndex TEXT, AltPres TEXT, SeaPres TEXT,
                Precip1hr TEXT, Precitp3hr TEXT, Precip6hr TEXT);''')

            print("Connected To Database")

        except sqlite3.OperationalError as e:
            print(e)


    # Function To Insert Data Into Database
    def addtoDB(self, data_df, table_num):
        cursor = self.conn.cursor()

        # Insert into bus location table
        if table_num == 1:
            old_buslocation_df = pd.read_sql_query("SELECT * FROM TRANSIT_LOCATION_DB", self.conn)
            old_weather_df = pd.read_sql_query("SELECT * FROM WEATHER_DB", self.conn)

            new_buslocation_df = data_df
            old_wea_list = list(old_weather_df.iloc[-1])
            new_buslocation_df["weather_id"] = old_wea_list[0]
            updated_buslocation_df = pd.concat([old_buslocation_df, new_buslocation_df])
            updated_buslocation_df.drop_duplicates(subset=["timestamp", "latitude", "longitude", "label", "id"], inplace=True)

            # Print update & append data
            updated_buslocation_df.to_sql("TRANSIT_LOCATION_DB", self.conn, if_exists="replace", index=False)


        # Insert into weather table
        elif table_num == 2:
            old_weather_df = pd.read_sql_query("SELECT * FROM WEATHER_DB", self.conn)
            new_weather_df = data_df

            try:
                new_comp = list(new_weather_df.iloc[0])
                old_comp = list(old_weather_df.iloc[-1])

                if new_comp[1:5] != old_comp[1:5]:
                    new_weather_df["weather_id"] = str(int(old_comp[0]) + 1)
                    updated_weather_df = pd.concat([old_weather_df, new_weather_df])

                    # Print update & append data
                    updated_weather_df.to_sql("WEATHER_DB", self.conn, if_exists="replace", index=False)

            except IndexError:
                # If no entry add data to table
                updated_weather_df = pd.concat([old_weather_df, new_weather_df])
                updated_weather_df.to_sql("WEATHER_DB", self.conn, if_exists="replace", index=False)

        else:
            pass


        self.conn.commit()
        cursor.close()
