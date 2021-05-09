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


        def dist2term(bus_coord, term_coord):
            """ This function takes both the bus location as well as stop location and calculates the distance between """

            bus_point = geopy.Point(bus_coord[0], bus_coord[1])
            term_point = geopy.Point(term_coord[0], term_coord[1])
            dist_measure = str(geodesic(bus_point, term_point)).replace(" km", "")

            return round(float(dist_measure), 3)




        # Grab transit data from stored dictionary
        transit_df = self.dataframe_dict["TRANSIT_LOCATION_DB"]
        weather_df = self.dataframe_dict["WEATHER_DB"]

        # Main terminals on stop
        term_dict = {"Sandalwood_Loop": (43.72056, -79.80911), "Square_One_Bus_Terminal": (43.59458, -79.64834)}

        # When Was Data Collected; Was It Continuous, or were there errors?
        route_df = transit_df[transit_df["route_id"] == "502-295"]

        # Pull lat & Longs
        bus_lat = route_df["latitude"].tolist()
        bus_long = route_df["longitude"].tolist()

        bus_coords = [(bus_lat, bus_long)for bus_lat, bus_long in zip(bus_lat, bus_long)]

        # Calculate distance to each terminal on 502 route
        for terminal in term_dict:
            term_coord = term_dict[terminal]
            route_df[f"Dist2{terminal}"] = [dist2term(bus_coord, term_coord) for bus_coord in bus_coords]

        # Plot only one trip
        list_of_trips = route_df["trip_id"].unique().tolist()
        for trip in list_of_trips:
            unq_trip_df = route_df[route_df["trip_id"] == trip]
            list_of_buses = unq_trip_df["id"].unique().tolist()
            for bus in list_of_buses:
                unq_bus_trip_df = unq_trip_df[unq_trip_df["id"] == bus]
                plt.plot(unq_bus_trip_df["timestamp"], unq_bus_trip_df["Dist2Sandalwood_Loop"])
                plt.show()
