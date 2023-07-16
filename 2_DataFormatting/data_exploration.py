import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np
from Funcs.func import *


#===============================================================================
# Step #1: Read In DataBase Using A Sqlite3 Connection & Any Additional Data
#===============================================================================
out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Misc/with_parent_terminals.csv"

db_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Data/DataStorage.db"
bus_stops_csv = r"/Users/renacin/Documents/BramptonTransitAnalysis/2_DataFormatting/Data/Data_Bus_Stops.csv"
bus_stops = pd.read_csv(bus_stops_csv)

# Find Bus Stops That Are Located At A Main Terminal, Find The Associated Main Bus Terminal
# Columns Needed stop_id where value is non-numeric, and parent_station where value is not null

# Find Main Bus Stops In Different Location Types
parent_bus_terminals = bus_stops[~bus_stops["stop_id"].str.isnumeric()]
stops_in_terminals = bus_stops[~bus_stops["parent_station"].isnull()]
stops_not_terminals = bus_stops[bus_stops["parent_station"].isnull() & bus_stops["stop_id"].str.isnumeric()]

# Created A Cleaned Station Name, If Bus Stop Located In Parent Station
con = sqlite3.connect(":memory:")
parent_bus_terminals.to_sql("parent_bus_terminals", con, index=False)
stops_in_terminals.to_sql("stops_in_terminals", con, index=False)
stops_not_terminals.to_sql("stops_not_terminals", con, index=False)

sql_query = f'''
-- Step #1: Left Join Parent Stop Information To Bus Terminals In Parent Station
WITH
S1 AS (
SELECT  A.*,
		B.STOP_NAME AS CLEANED_STOP_NAME,
		B.STOP_LAT AS CLEANED_STOP_LAT,
		B.STOP_LON AS CLEANED_STOP_LON

FROM stops_in_terminals AS A
LEFT JOIN parent_bus_terminals as B
ON (A.parent_station = B.stop_id)
),

-- Step #2: Concat Other Terminals Not Found In Main Bus Stations
S2 AS (
SELECT
	A.*,
	'-'	AS CLEANED_STOP_NAME,
	'-'	AS CLEANED_STOP_LAT,
	'-'	AS CLEANED_STOP_LON

FROM stops_not_terminals AS A

UNION ALL

SELECT B.*
FROM S1 AS B
)

-- Step #3: Clean Up Table For Easier Usage Later Down The Line
SELECT
	A.*,

	CASE WHEN A.CLEANED_STOP_NAME = '-'
		 THEN A.STOP_NAME
		 ELSE A.CLEANED_STOP_NAME
	END AS CLEANED_STOP_NAME_,

	CASE WHEN A.CLEANED_STOP_LAT = '-'
		 THEN A.STOP_LAT
		 ELSE A.CLEANED_STOP_LAT
	END AS CLEANED_STOP_LAT_,

	CASE WHEN A.CLEANED_STOP_LON = '-'
		 THEN A.STOP_LON
		 ELSE A.CLEANED_STOP_LON
	END AS CLEANED_STOP_LON_

FROM S2 AS A
'''
main_data = pd.read_sql_query(sql_query, con)
for col in ["CLEANED_STOP_NAME", "CLEANED_STOP_LAT", "CLEANED_STOP_LON"]:
	del main_data[col]
con.close()

main_data.to_csv(out_path, index=False)
