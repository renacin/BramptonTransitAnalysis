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
		A.route_id                                                                                                 AS ROUTE_ID,
		A.trip_id                                                                                                  AS TRIP_ID,
		A.id                                                                                                       AS ID,
		A.stop_id                                                                                                  AS STOP_ID

	FROM TRANSIT_LOCATION_DB AS A
	ORDER BY A.TRIP_ID, A.ID, A.TIMESTAMP
),


-- Step #2: Using Above Table Determine First Recorded Stop
FirstStop AS (
	SELECT
		RawData.*

	FROM RawData
	WHERE RawData.TIME_RANK = 1
),


-- Step #3: Join Both Tables, Adding First Location Recorded In Trip
CleanData AS (
	SELECT
		RawData.*,
		FirstStop.C_LAT                                                       AS P_LAT,
		FirstStop.C_LONG                                                      AS P_LONG

	FROM RawData LEFT JOIN FirstStop ON (RawData.TRIP_ID = FirstStop.TRIP_ID AND RawData.ID = FirstStop.ID)

),


-- Step #4: Format Bus Stop Data Before Merge Onto Final Table
StopLoc AS (
	SELECT
		stop_id                                                          AS STP_ID,
		stop_name                                                        AS STP_NAME,
		stop_lat                                                         AS STP_LAT,
		stop_lon                                                         AS STP_LONG

	FROM BusStops
	LIMIT 100
)

-- Step #5: Merge Stop Information Onto Main Data
SELECT
	CleanData.*,
	StopLoc.*

FROM CleanData LEFT JOIN StopLoc ON (CAST(CleanData.STOP_ID AS VARCHAR) = CAST(StopLoc.STP_ID AS VARCHAR))

WHERE CleanData.TRIP_ID = '16825147-210426-MULTI-Weekday-01'
AND CleanData.ID = 2084
'''


"""
My Left Join Isn't Working; Why?
00050102, 00050104, 00050104, 00050106, 00050108, 00050108


From Bus Location Data
    stop_id route_id
0  00045510    1-295
1  00023040    1-295
2  00050156    1-295
3  00045533    1-295
4  15090711    1-295
5  00045505    1-295

6  00045528    1-295
7  00002390    1-295
8  00000210    1-295
9  00021137    1-295
"""















sql_query = f'''

-- Step #1: Try To Merge Bus Stop Information To Bus Location Data
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
	A.route_id                                                                                                 AS ROUTE_ID,
	A.trip_id                                                                                                  AS TRIP_ID,
	A.id                                                                                                       AS ID,
	A.stop_id                                                                                                  AS STOP_ID,

	B.*

FROM TRANSIT_LOCATION_DB AS A
LEFT JOIN BusStops AS B ON (A.stop_id = B.stop_id)

WHERE A.trip_id = '16825147-210426-MULTI-Weekday-01'
ORDER BY A.TRIP_ID, A.ID, A.TIMESTAMP
'''

# Pull A Small Subset Of Data For A Certain Bus Route
transit_df = pd.read_sql_query(sql_query, con)
print(transit_df)

out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/Attempt_2/Misc/Test_Data.csv"
transit_df.to_csv(out_path, index=False)





















#
# # Find Distance Between First Bus Location
# def vec_haversine(coord1, coord2):
#     """
# 	coord1 = first location reported
# 	coord2 = current location reported
#
#     This function will calculate the distance between bus location and bus stop; returns distance in km
#     Taken from: https://datascience.blog.wzb.eu/2018/02/02/vectorization-and-parallelization-in-python-with-numpy-and-pandas/
#     """
#     b_lat, b_lng = coord1[0], coord1[1]
#     a_lat, a_lng = coord2[0], coord2[1]
#
#     R = 6371  # earth radius in km
#
#     a_lat = np.radians(a_lat)
#     a_lng = np.radians(a_lng)
#     b_lat = np.radians(b_lat)
#     b_lng = np.radians(b_lng)
#
#     d_lat = b_lat - a_lat
#     d_lng = b_lng - a_lng
#
#     d_lat_sq = np.sin(d_lat / 2) ** 2
#     d_lng_sq = np.sin(d_lng / 2) ** 2
#
#     a = d_lat_sq + np.cos(a_lat) * np.cos(b_lat) * d_lng_sq
#     c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
#
#     return R * c  # returns distance between a and b in km
#
#
# transit_df["DST_FROM_STRT"] = transit_df.apply(lambda x: vec_haversine((x["P_LAT"], x["P_LONG"]), (x["C_LAT"], x["C_LONG"])), axis=1)


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

	CASE WHEN A.bearing BETWEEN 90 AND 180 THEN 'NORT-WEST'
	     WHEN A.bearing BETWEEN 90 AND 180 THEN 'SOUTH-EAST'
		 ELSE
	END AS CARDI,



"""
