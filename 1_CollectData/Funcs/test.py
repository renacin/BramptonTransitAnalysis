# Name:                                            Renacin Matadeen
# Date:                                               01/07/2024
# Title                                Gather Transit Bus Stop Location Data
#
# ---------------------------------------------------------------------------------------------------------------------
import sqlite3
import requests
import pandas as pd
# ---------------------------------------------------------------------------------------------------------------------

testing = pd.read_csv('https://opendata.arcgis.com/api/v3/datasets/1c9869fd805e45339a9e3373fc10ce08_0/downloads/data?format=csv&spatialRefId=3857&where=1%3D1')
print(testing)
