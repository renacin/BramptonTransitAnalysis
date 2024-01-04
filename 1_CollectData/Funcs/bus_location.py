# Name:                                            Renacin Matadeen
# Date:                                               12/29/2023
# Title                                Gather Transit Bus Stop Location Data
#
# ---------------------------------------------------------------------------------------------------------------------
import json
import sqlite3
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
# ---------------------------------------------------------------------------------------------------------------------

def main():

	# Grab Response From Link
	link = r"https://nextride.brampton.ca:81/API/VehiclePositions?format=json"
	response = requests.get(link)

	# Injest As JSON, and Load Into Pandas Dataframe
	data = json.loads(response.text)
	resp_tsmp = data["header"]["timestamp"]
	bus_loc_df = pd.json_normalize(data["entity"])

	# For Testing Write Data Out
	out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_Data"
	path_testing = out_path + "/Testing.csv"
	bus_loc_df.to_csv(path_testing)



# ---------------------------------------------------------------------------------------------------------------------

# Main Entry Point Into Python Code
if __name__ == "__main__":
    main()
