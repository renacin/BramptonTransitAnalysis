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
import matplotlib.pyplot as plt
import geopy
from geopy.distance import VincentyDistance, geodesic

import warnings
warnings.filterwarnings('ignore')
# ----------------------------------------------------------------------------------------------------------------------


class DataCollection:
    """ This class will return each table within a connected database as a dictionary containing pandas dataframes """


    def __init__(self, csv_file_loc):
        """ On initialization return a dictionary containing each table as a pandas dataframe """

        self.transit_data_csv = pd.read_csv(csv_file_loc)


    def process_transit_data(self):
        """ This function will identify key statistics from transit data | First explore the data"""

        def dist2term(bus_coord):
            """ This function takes both the bus location as well as stop location and calculates the distance between """

            bus_point = geopy.Point(bus_coord[0], bus_coord[1])
            term_point = geopy.Point(43.72056, -79.80911)
            dist_measure = str(geodesic(bus_point, term_point)).replace(" km", "")

            return round(float(dist_measure), 3)

        def rem_idle(temp_df):
            """ This function takes a dataframe containing bus location data and removes and point where the bus is idling """

            for cordtype in ["latitude", "longitude"]:

                # Fix lat long columns for high acc
                temp_df[cordtype] = temp_df[cordtype].astype(float)
                temp_df[cordtype] = temp_df[cordtype].round(decimals=5)

                # Create new columns
                new_cord_col = f"{cordtype}_Round"
                temp_df[new_cord_col] = temp_df[cordtype].round(decimals=3)

            cleaned_df = temp_df.drop_duplicates(subset = ["latitude_Round", "longitude_Round", "unq_id"], keep = 'last')

            del cleaned_df["latitude_Round"], cleaned_df["longitude_Round"]
            return cleaned_df


        # Grab transit data from stored dictionary
        transit_df = self.transit_data_csv

        # When Was Data Collected; Was It Continuous, or were there errors?
        route_df = transit_df[transit_df["route_id"] == "502-295"]

        # Pull lat & Longs & Create new column
        bus_coords = [(bus_lat, bus_long) for bus_lat, bus_long in zip(route_df["latitude"].tolist(), route_df["longitude"].tolist())]

        # Calculate distance to each terminal on 502 route
        route_df["Dist2Sandalwood_Loop"] = [dist2term(bus_coord) for bus_coord in bus_coords]

        # Plot only one bus trip at a time, and only trips moving away from the Sandalwood loop (Begining values should be greater than 1 KM)
        route_df["unq_id"] = route_df["trip_id"].astype(str) + "_" + route_df["id"].astype(str)
        list_of_trips = route_df["unq_id"].unique().tolist()
        for trip in list_of_trips:

            # Remove data points where bus is idling at the begining or end of the trip
            unq_trip_df = route_df[route_df["unq_id"] == trip]
            unq_trip_df["timesince"] = (unq_trip_df["timestamp"] - unq_trip_df["timestamp"].min()) / 60

            # Plot Trip Graphs, Start At Sandalwood, Trip must be less than 120 minutes
            if (unq_trip_df["Dist2Sandalwood_Loop"].tolist()[0] <= 1) and (unq_trip_df["timesince"].max() <= 200) and (unq_trip_df["timesince"].max() >= 10):
                plt.plot(unq_trip_df["timesince"], unq_trip_df["Dist2Sandalwood_Loop"])
                plt.show()
