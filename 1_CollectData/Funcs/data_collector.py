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
		print("Hello World")
