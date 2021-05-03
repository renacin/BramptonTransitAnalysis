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
        self.transit_url = "http://nextride.brampton.ca:81/API/VehiclePositions?format=json"
        self.weather_url = "https://forecast.weather.gov/data/obhistory/metric/CYYZ.html"


    def gather_weather_data(self):
        """ This function takes the weather link provided and grabs weather data for brampton """
        pass
