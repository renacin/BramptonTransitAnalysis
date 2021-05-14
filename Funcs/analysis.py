# Name:                                            Renacin Matadeen
# Date:                                               05/03/2021
# Title                              Main functions used to analyses collected data
#
# ----------------------------------------------------------------------------------------------------------------------
import time, datetime, warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopy
from geopy.distance import geodesic
# ----------------------------------------------------------------------------------------------------------------------


class DataCollection:
    """ This class will return each table within a connected database as a dictionary containing pandas dataframes """


    def __init__(self, csv_file_loc):
        """ On initialization return a dictionary containing each table as a pandas dataframe """

        self.transit_data_csv = pd.read_csv(csv_file_loc)


    # -----------------------------------------------------------------------------------------------------------------
    def process_transit_data(self):
        """ This function will identify key statistics from transit data | First explore the data"""


        def vec_haversine(coord):

            """
            This function will calculate the distance between bus location and bus stop; returns distance in km
            Taken from: https://datascience.blog.wzb.eu/2018/02/02/vectorization-and-parallelization-in-python-with-numpy-and-pandas/
            """
            b_lat, b_lng = (43.72056, -79.80911)
            a_lat, a_lng = coord[0], coord[1]
            R = 6371  # earth radius in km

            a_lat = np.radians(a_lat)
            a_lng = np.radians(a_lng)
            b_lat = np.radians(b_lat)
            b_lng = np.radians(b_lng)

            d_lat = b_lat - a_lat
            d_lng = b_lng - a_lng

            d_lat_sq = np.sin(d_lat / 2) ** 2
            d_lng_sq = np.sin(d_lng / 2) ** 2

            a = d_lat_sq + np.cos(a_lat) * np.cos(b_lat) * d_lng_sq
            c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))

            return R * c  # returns distance between a and b in km


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


        # -----------------------------------------------------------------------------------------------------------------
        # Grab transit data from stored dictionary
        transit_df = self.transit_data_csv

        # When Was Data Collected; Was It Continuous, or were there errors?
        route_df = transit_df[transit_df["route_id"] == "502-295"]

        # Pull lat & Longs & Create new column
        route_df["bus_coords"] = list(zip(route_df["latitude"], route_df["longitude"]))
        route_df["Dist2Sandalwood_Loop"] = route_df["bus_coords"].apply(vec_haversine)

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
