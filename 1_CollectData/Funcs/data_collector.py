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


    def __init__(self):
        self.transit_url = "https://nextride.brampton.ca:81/API/VehiclePositions?format=json"


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
