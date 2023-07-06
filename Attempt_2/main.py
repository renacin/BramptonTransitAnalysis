import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

# ------------------------------------------------------------------------------
# Define Functions Needed For Analysis

# Find Distance Between Points
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


# ------------------------------------------------------------------------------


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


		LAG(A.latitude) OVER (PARTITION BY A.TRIP_ID, A.ID ORDER BY A.TIMESTAMP)                                   AS PRV_LAT,
		LAG(A.longitude) OVER (PARTITION BY A.TRIP_ID, A.ID ORDER BY A.TIMESTAMP)                                  AS PRV_LONG,


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
		CleanedData.EP_TIME,
		CleanedData.TRIP_TIME,
		CleanedData.LST_UPDT,
		CleanedData.C_LAT,
		CleanedData.C_LONG,
		CleanedData.PRV_LAT,
		CleanedData.PRV_LONG,
		CleanedData.DIR,
		CleanedData.AVG_DIR,
		CleanedData.ROUTE_ID,
		CleanedData.TRIP_ID,
		CleanedData.ID,
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
FROM Cleaner_Data AS A
WHERE A.ROUTE_ID = '501-295'
AND A.TRIP_ID = '16829122-210426-MULTI-Weekday-01'
AND A.ID = 1483

'''
# Pull A Small Subset Of Data For A Certain Bus Route
transit_df = pd.read_sql_query(sql_query, con)
print("Finished Gathering Data")


# Determine Previous Bus Stop
for col in ["PRV_STP_NAME", "PRV_STP_LAT", "PRV_STP_LONG"]:
	transit_df[col] = transit_df.groupby(["ROUTE_ID", "TRIP_ID", "ID", "AVG_DIR"])[col].ffill()
print("Finished FFilling Data")


# Determine Distance From Previous & Next Bus Stop
transit_df["DST_2_NBSTP"] = round(transit_df.apply(lambda x: vec_haversine((x["STP_LAT"], x["STP_LONG"]), (x["C_LAT"], x["C_LONG"])), axis=1), 4)
transit_df["DST_2_PBSTP"] = round(transit_df.apply(lambda x: vec_haversine((x["PRV_STP_LAT"], x["PRV_STP_LONG"]), (x["C_LAT"], x["C_LONG"])), axis=1), 4)
transit_df["DST_2_PLOC"] = round(transit_df.apply(lambda x: vec_haversine((x["PRV_LAT"], x["PRV_LONG"]), (x["C_LAT"], x["C_LONG"])), axis=1), 4)
print("Finished Calculating Distances")


# Determine The Speed Travelled For Entire Duration
speed_df = transit_df.groupby(["ROUTE_ID", "TRIP_ID", "ID", "AVG_DIR"], as_index=False).agg(
			TRIP_TIME = ("TRIP_TIME", "last"),
			TRIP_LEN = ("DST_2_PLOC", "sum"),
)
speed_df["TRIP_TIME"] = speed_df["TRIP_TIME"].astype("float")
speed_df["TRIP_SPD"] = round(speed_df["TRIP_LEN"] / speed_df["TRIP_TIME"] / 60, 2)
print("Finished Calculating Trip Speed")


transit_df = transit_df.merge(speed_df, how="left", on=["ROUTE_ID", "TRIP_ID", "ID", "AVG_DIR"])
transit_df["TME_2_PBSTP"] = round(((transit_df["DST_2_PBSTP"] / transit_df["TRIP_SPD"])*60)*60)
transit_df["TME_2_NBSTP"] = round(((transit_df["DST_2_NBSTP"] / transit_df["TRIP_SPD"])*60)*60)
transit_df["ARV_TME_PBSTP"] = transit_df["EP_TIME"] - transit_df["TME_2_PBSTP"]
print("Finished Determining Arrival Time ")


cleaned_df = transit_df.loc[:, ['ROUTE_ID', 'TRIP_ID', 'ID', 'AVG_DIR', 'PRV_STP_NAME', 'PRV_STP_LAT', 'PRV_STP_LONG', 'ARV_TME_PBSTP']]
cleaned_df.dropna(inplace=True)
cleaned_df.drop_duplicates(subset=['ROUTE_ID', 'TRIP_ID', 'ID', 'AVG_DIR', 'PRV_STP_NAME', 'PRV_STP_LAT', 'PRV_STP_LONG'], inplace=True)
cleaned_df.sort_values(['ROUTE_ID', 'TRIP_ID', 'ID', 'ARV_TME_PBSTP'], inplace=True)


out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/Attempt_2/Misc/Test_Data.csv"
cleaned_df.to_csv(out_path, index=False)
print("Finished Writing Data")

























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
