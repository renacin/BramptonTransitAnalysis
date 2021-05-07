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

        # When Was Data Collected; Was It Continuous, or were there errors?
        all_timestamps = sorted(transit_df["timestamp"].tolist())
        start_timestamp = all_timestamps[0]
        end_timestamp = all_timestamps[0] + 3600
        obs_in_hour = []
        for x in range(121):
            df_query = transit_df[(transit_df["timestamp"] >= start_timestamp) & (transit_df["timestamp"] <= end_timestamp)]
            obs_in_hour.append(len(df_query))
            start_timestamp += 3600
            end_timestamp += 3600

        hours = [x for x in range(121)]
        hrs5 = [x for x in range(0, 121, 5)]
        plt.rcParams["figure.figsize"] = (10, 6)
        plt.bar(hours, obs_in_hour, color='#607c8e')
        plt.title("# Data Points Collected Every Hour Since 4/30/2021, 10:00 AM")
        plt.xlabel("Number Of Hours Since Start")
        plt.xticks(hrs5, hrs5, rotation ='vertical')
        plt.ylabel("# Of Data Points")
        plt.grid(axis='y', alpha=0.75)
        plt.show()
