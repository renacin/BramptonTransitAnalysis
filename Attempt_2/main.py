import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np


# Read In DataBase Using A Sqlite3 Connection & Any Additional Data
db_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/Attempt_2/Data/DataStorage.db"
bus_stops_csv = r"/Users/renacin/Documents/BramptonTransitAnalysis/Attempt_2/Data/Data_Bus_Stops.csv"
bus_stops = pd.read_csv(bus_stops_csv)
con = sqlite3.connect(db_path)
bus_stops.to_sql("BusStops", con, if_exists="replace", index=False)



sql_query = f'''

-- Step #1: Pull Certain Fields, And Create New Ones
WITH
RawData AS (
	SELECT
		RANK()
		OVER (PARTITION BY A.TRIP_ID, A.ID ORDER BY A.timestamp)                                                   AS TIME_RANK,
		A.timestamp                                                                                                AS EP_TIME,

		printf("%.2f", CAST(A.TIMESTAMP - MIN(A.TIMESTAMP)
		OVER (PARTITION BY A.TRIP_ID, A.ID) AS DECIMAL) / 60.0)                                                    AS TRIP_TIME,

		printf("%.2f", CAST(A.TIMESTAMP - COALESCE(LAG(A.TIMESTAMP)
		OVER (PARTITION BY A.TRIP_ID, A.ID ORDER BY A.TIMESTAMP), A.TIMESTAMP) AS DECIMAL) / 60.0)                 AS LST_UPDT,

		A.latitude                                                                                                 AS C_LAT,
		A.longitude                                                                                                AS C_LONG,
		CAST(A.bearing AS INTERGER)                                                                                AS DIR,

		CAST(AVG(A.bearing)
		OVER (PARTITION BY A.TRIP_ID, A.ID) AS INTERGER)                                                           AS AVG_DIR,

		printf("%.2f", A.speed)                                                                                    AS KPH,
		printf("%.2f", AVG(A.speed) OVER (PARTITION BY A.TRIP_ID, A.ID))                                           AS AVG_KPH,


		A.route_id                                                                                                 AS ROUTE_ID,
		A.trip_id                                                                                                  AS TRIP_ID,
		A.id                                                                                                       AS ID,
		A.stop_id                                                                                                  AS STOP_ID

	FROM TRANSIT_LOCATION_DB AS A
	ORDER BY A.TRIP_ID, A.ID, A.TIMESTAMP
),


-- Step #2: Format Bus Stop Data Before Merge Onto Final Table
StopLoc AS (
	SELECT
		stop_id                                                          AS STP_ID,
		stop_name                                                        AS STP_NAME,
		stop_lat                                                         AS STP_LAT,
		stop_lon                                                         AS STP_LONG

	FROM BusStops
),


-- Step #3: Merge Stop Information Onto Main Data
CleanedData AS (
	SELECT
		RawData.*,
		StopLoc.*,

		LAG(StopLoc.STP_NAME) OVER (PARTITION BY RawData.TRIP_ID, RawData.ID ORDER BY RawData.EP_TIME)      AS PRV_STP_NAME,
		LAG(StopLoc.STP_LAT) OVER (PARTITION BY RawData.TRIP_ID, RawData.ID ORDER BY RawData.EP_TIME)       AS PRV_STP_LAT,
		LAG(StopLoc.STP_LONG) OVER (PARTITION BY RawData.TRIP_ID, RawData.ID ORDER BY RawData.EP_TIME)      AS PRV_STP_LONG

	FROM RawData LEFT JOIN StopLoc ON (CAST(RawData.STOP_ID AS VARCHAR) = CAST(StopLoc.STP_ID AS VARCHAR))
),


-- Step #4: Clean Up Erronious Data
Cleaner_Data AS (
	SELECT
		CleanedData.TIME_RANK,
		CleanedData.EP_TIME,
		CleanedData.TRIP_TIME,
		CleanedData.LST_UPDT,
		CleanedData.C_LAT,
		CleanedData.C_LONG,
		CleanedData.DIR,
		CleanedData.AVG_DIR,
		CleanedData.KPH,
		CleanedData.AVG_KPH,
		CleanedData.ROUTE_ID,
		CleanedData.TRIP_ID,
		CleanedData.ID,
		CleanedData.STOP_ID,
		CleanedData.STP_ID,
		CleanedData.STP_NAME,
		CleanedData.STP_LAT,
		CleanedData.STP_LONG,

		CASE WHEN CleanedData.PRV_STP_NAME == CleanedData.STP_NAME THEN CleanedData.PRV_STP_NAME = NULL
		ELSE CleanedData.PRV_STP_NAME
		END AS PRV_STP_NAME,

		CASE WHEN CleanedData.PRV_STP_LAT == CleanedData.STP_LAT THEN CleanedData.PRV_STP_LAT = NULL
		ELSE CleanedData.PRV_STP_LAT
		END AS PRV_STP_LAT,

		CASE WHEN CleanedData.PRV_STP_LONG == CleanedData.STP_LONG THEN CleanedData.PRV_STP_LONG = NULL
		ELSE CleanedData.PRV_STP_LONG
		END AS PRV_STP_LONG

	FROM CleanedData
)


SELECT *
FROM Cleaner_Data
WHERE Cleaner_Data.TRIP_ID = '16825147-210426-MULTI-Weekday-01'
AND Cleaner_Data.ID = 2084
'''

# Pull A Small Subset Of Data For A Certain Bus Route
transit_df = pd.read_sql_query(sql_query, con)


# Find Distance Between First Bus Location
def vec_haversine(coord1, coord2):
    """
	coord1 = first location reported
	coord2 = current location reported

    This function will calculate the distance between bus location and bus stop; returns distance in km
    Taken from: https://datascience.blog.wzb.eu/2018/02/02/vectorization-and-parallelization-in-python-with-numpy-and-pandas/
    """
    b_lat, b_lng = coord1[0], coord1[1]
    a_lat, a_lng = coord2[0], coord2[1]

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

# Determine Previous Bus Stop
transit_df["PRV_STP_NAME"] = transit_df["PRV_STP_NAME"].ffill()
transit_df["PRV_STP_LAT"] = transit_df["PRV_STP_LAT"].ffill()
transit_df["PRV_STP_LONG"] = transit_df["PRV_STP_LONG"].ffill()

# Determine Distance From Previous & Next Bus Stop
transit_df["DST_2_NBSTP"] = round(transit_df.apply(lambda x: vec_haversine((x["STP_LAT"], x["STP_LONG"]), (x["C_LAT"], x["C_LONG"])), axis=1), 4)
transit_df["DST_2_PBSTP"] = round(transit_df.apply(lambda x: vec_haversine((x["PRV_STP_LAT"], x["PRV_STP_LONG"]), (x["C_LAT"], x["C_LONG"])), axis=1), 4)

# Determine The Speed Travelled For Entire Duration
speed_df = transit_df.groupby(["ROUTE_ID", "TRIP_ID", "ID", "AVG_DIR"], as_index=False).agg({
			'number_units':sum
			}
)
out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/Attempt_2/Misc/Test_Data.csv"
transit_df.to_csv(out_path, index=False)

























# plt.plot(transit_df["TRIP_TIME"], transit_df["DST_FROM_STRT"])
# plt.show()


# # Make A Plot To View Bus Locations
# fig, ax = plt.subplots(figsize=(10, 6))
# ax.scatter(transit_df["LONG"].tolist(), transit_df["LAT"].tolist())
# plt.show()







"""
General Notes:
	+ Displaying Table Names In Sqlite3
		SELECT * FROM sqlite_master
"""
