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
		A.id                                                                                                       AS ID,
		A.route_id                                                                                                 AS ROUTE_ID,
		A.trip_id                                                                                                  AS TRIP_ID,
		CAST(A.bearing AS INTERGER)                                                                                AS DIR,
		CAST(AVG(A.bearing)
		OVER (PARTITION BY A.ROUTE_ID, A.TRIP_ID, A.ID) AS INTERGER)                                               AS AVG_DIR,

		COALESCE(LAG(A.latitude)
		OVER (PARTITION BY A.ROUTE_ID, A.TRIP_ID, A.ID ORDER BY A.TIMESTAMP),
		A.latitude)                                                                                                AS PRV_LAT,

		COALESCE(LAG(A.longitude)
		OVER (PARTITION BY A.ROUTE_ID, A.TRIP_ID, A.ID ORDER BY A.TIMESTAMP),
		A.latitude)                                                                                                AS PRV_LONG,

		A.latitude                                                                                                 AS C_LAT,
		A.longitude                                                                                                AS C_LONG,

		A.stop_id                                                                                                  AS NXT_STP_ID,
		COALESCE(LAG(A.stop_id)
		OVER (PARTITION BY A.ROUTE_ID, A.TRIP_ID, A.ID ORDER BY A.TIMESTAMP), A.stop_id)                           AS PRV_STP_ID

	FROM TRANSIT_LOCATION_DB AS A
	ORDER BY A.TRIP_ID, A.ID, A.TIMESTAMP
),


-- Step #2: Merge Bus Stop Information Onto Main Table
WithStopData AS (
	SELECT
		A.*,

		B2.stop_name                                                     AS PRV_STP_NAME,
		B2.stop_lat                                                      AS PRV_STP_LAT,
		B2.stop_lon                                                      AS PRV_STP_LONG,

		B1.stop_name                                                     AS NXT_STP_NAME,
		B1.stop_lat                                                      AS NXT_STP_LAT,
		B1.stop_lon                                                      AS NXT_STP_LONG


	FROM RawData AS A
	LEFT JOIN BusStops AS B1 ON (A.NXT_STP_ID = B1.stop_id)
	LEFT JOIN BusStops AS B2 ON (A.PRV_STP_ID = B2.stop_id)
)


SELECT *
FROM WithStopData AS A
WHERE A.ROUTE_ID = '30-295'

'''


# Pull A Small Subset Of Data For A Certain Bus Route
transit_df = pd.read_sql_query(sql_query, con)
transit_df.sort_values(['ROUTE_ID', 'TRIP_ID', 'ID', 'EP_TIME'], inplace=True)
out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/Attempt_2/Misc/Test_Data.csv"


# If Next Stop Is Equal To Previous Stop, Replace With Blank, Foward Fill Next Stop Values & Replace First
for n_col, p_col in zip(["NXT_STP_NAME", "NXT_STP_LAT", "NXT_STP_LONG"], ["PRV_STP_NAME", "PRV_STP_LAT", "PRV_STP_LONG"]):
	transit_df.loc[transit_df[n_col] == transit_df[p_col], p_col] = np.nan
	transit_df[p_col] = transit_df.groupby(["ROUTE_ID", "TRIP_ID", "ID", "AVG_DIR"])[p_col].ffill()
	transit_df[p_col] = transit_df[p_col].fillna(transit_df[n_col])
transit_df = transit_df.drop_duplicates(subset=["NXT_STP_NAME", "PRV_STP_NAME"], keep="last")


# Determine Distance Between Previous Location  & Current Location
transit_df["DST_PSTP_NXTSTP"] = round(transit_df.apply(lambda x: vec_haversine((x["PRV_STP_LAT"], x["PRV_STP_LONG"]), (x["NXT_STP_LAT"], x["NXT_STP_LONG"])), axis=1), 4)
transit_df["DST_2_PBSTP"] = round(transit_df.apply(lambda x: vec_haversine((x["PRV_STP_LAT"], x["PRV_STP_LONG"]), (x["C_LAT"], x["C_LONG"])), axis=1), 4)

speed_df = transit_df.groupby(["ROUTE_ID", "TRIP_ID", "ID", "AVG_DIR"], as_index=False).agg(
			TRIP_DUR = ("EP_TIME", lambda s: round((((s.iloc[-1] - s.iloc[0])/60))/60, 2)),
			TRIP_LEN = ("DST_PSTP_NXTSTP", lambda s: round(s.sum(), 2)),
)
speed_df["TRIP_SPD"] = round(speed_df["TRIP_LEN"] / speed_df["TRIP_DUR"], 2)
transit_df = transit_df.merge(speed_df, how="left", on=["ROUTE_ID", "TRIP_ID", "ID", "AVG_DIR"])
transit_df["TME_2_PBSTP"] = round(((transit_df["DST_2_PBSTP"] / transit_df["TRIP_SPD"])*60)*60)
transit_df["ARV_TME_PBSTP"] = transit_df["EP_TIME"] - transit_df["TME_2_PBSTP"]

cleaned_df = transit_df.loc[:, ["ID", "ROUTE_ID", "TRIP_ID", "AVG_DIR", "PRV_STP_NAME", "PRV_STP_LAT", "PRV_STP_LONG", "TRIP_DUR", "TRIP_LEN", "TRIP_SPD", "ARV_TME_PBSTP"]]
cleaned_df = cleaned_df.drop_duplicates(subset=["PRV_STP_NAME"], keep="last")
cleaned_df.sort_values(['ROUTE_ID', 'TRIP_ID', 'ID', 'ARV_TME_PBSTP'], inplace=True)

transit_df.to_csv(out_path, index=False)
