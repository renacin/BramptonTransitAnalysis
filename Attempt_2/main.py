import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np
from Funcs.func import *

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

		A.latitude                                                                                                 AS C_LAT,
		A.longitude                                                                                                AS C_LONG,

		A.stop_id                                                                                                  AS NXT_STP_ID


	FROM TRANSIT_LOCATION_DB AS A
	ORDER BY A.TRIP_ID, A.ID, A.TIMESTAMP
),

-- Step #2: Previous Stop ID Needs To Be Determined With Average Direction
RD AS (
	SELECT
		A.*,
		COALESCE(LAG(A.NXT_STP_ID)
		OVER (PARTITION BY A.ROUTE_ID, A.TRIP_ID, A.ID, A.AVG_DIR ORDER BY A.EP_TIME), A.NXT_STP_ID)            AS PRV_STP_ID
	FROM RawData AS A
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


	FROM RD AS A
	LEFT JOIN BusStops AS B1 ON (A.NXT_STP_ID = B1.stop_id)
	LEFT JOIN BusStops AS B2 ON (A.PRV_STP_ID = B2.stop_id)
)


SELECT *
FROM WithStopData AS A
'''

# Pull A Small Subset Of Data For A Certain Bus Route
transit_df = pd.read_sql_query(sql_query, con)
con.close()
transit_df.sort_values(["ID", "ROUTE_ID", "TRIP_ID", "EP_TIME"], inplace=True)
transit_df.drop_duplicates(inplace=True)
out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/Attempt_2/Misc/Test_Data.csv"


# If Next Stop Is Equal To Previous Stop, Replace With Blank, Foward Fill Next Stop Values & Replace First
for n_col, p_col in zip(["NXT_STP_ID", "NXT_STP_NAME", "NXT_STP_LAT", "NXT_STP_LONG"], ["PRV_STP_ID", "PRV_STP_NAME", "PRV_STP_LAT", "PRV_STP_LONG"]):
	transit_df.loc[transit_df[n_col] == transit_df[p_col], p_col] = np.nan
	transit_df[p_col] = transit_df.groupby(["ID", "ROUTE_ID", "TRIP_ID", "AVG_DIR"])[p_col].ffill()
	transit_df[p_col] = transit_df[p_col].fillna(transit_df[n_col])
transit_df = transit_df.drop_duplicates(subset=["ID", "ROUTE_ID", "TRIP_ID", "AVG_DIR", "NXT_STP_ID", "PRV_STP_ID"], keep="last")



# Determine Distance Between Previous Location  & Current Location
transit_df["DST_PSTP_NXTSTP"] = round(transit_df.apply(lambda x: vec_haversine((x["PRV_STP_LAT"], x["PRV_STP_LONG"]), (x["NXT_STP_LAT"], x["NXT_STP_LONG"])), axis=1), 4)
transit_df["DST_2_PBSTP"] = round(transit_df.apply(lambda x: vec_haversine((x["PRV_STP_LAT"], x["PRV_STP_LONG"]), (x["C_LAT"], x["C_LONG"])), axis=1), 4)

speed_df = transit_df.groupby(["ID", "ROUTE_ID", "TRIP_ID", "AVG_DIR"], as_index=False).agg(
			TRIP_DUR = ("EP_TIME", lambda s: round((((s.iloc[-1] - s.iloc[0])/60))/60, 2)),
			TRIP_LEN = ("DST_PSTP_NXTSTP", lambda s: round(s.sum(), 2)),
)

speed_df["TRIP_SPD"] = speed_df["TRIP_LEN"] / speed_df["TRIP_DUR"]
transit_df = transit_df.merge(speed_df, how="left", on=["ROUTE_ID", "TRIP_ID", "ID", "AVG_DIR"])
transit_df["TME_2_PBSTP"] = ((transit_df["DST_2_PBSTP"] / transit_df["TRIP_SPD"])*60)*60
transit_df["ARV_TME_PBSTP"] = transit_df["EP_TIME"] - transit_df["TME_2_PBSTP"]



con = sqlite3.connect(":memory:")
transit_df.to_sql("main_data", con, index=False)

sql_query = f'''
-- Step #1: Reorganize Data & Keep Only Needed Columns
SELECT
	A.ID,
	A.ROUTE_ID,
	A.TRIP_ID,
	A.DIR,
	A.AVG_DIR,

	A.DST_PSTP_NXTSTP                                                                             AS DST_BTW_STPS,
	A.PRV_STP_ID                                                                                  AS CUR_STP_ID,
	A.PRV_STP_NAME                                                                                AS CUR_STP_NM,
	A.PRV_STP_LAT                                                                                 AS CUR_STP_LAT,
	A.PRV_STP_LONG                                                                                AS CUR_STP_LONG,
	A.ARV_TME_PBSTP                                                                               AS CUR_STP_TIME,

	A.PRV_STP_NAME || ' -- TO -- ' || A.NXT_STP_NAME                                              AS SEGMENT_NAME,

	A.NXT_STP_ID,
	A.NXT_STP_NAME,
	A.NXT_STP_LAT,
	A.NXT_STP_LONG,
	LEAD(A.ARV_TME_PBSTP)
	OVER (PARTITION BY A.ID, A.ROUTE_ID, A.TRIP_ID, A.AVG_DIR ORDER BY A.EP_TIME)                 AS NXT_STP_TIME

FROM main_data AS A
WHERE A.DST_PSTP_NXTSTP > 0
'''
main_data = pd.read_sql_query(sql_query, con).dropna()
con.close()
main_data["TRVL_TIME"] = round((main_data["NXT_STP_TIME"] - main_data["CUR_STP_TIME"]) / 60, 2)


main_data = main_data[main_data["SEGMENT_NAME"] == "McMurchy - Zum Steeles Station Stop WB -- TO -- Sheridan College Term - 3/3A/4/4A/11/51/511/104 WB"]
main_data.to_csv(out_path, index=False)

"""
Most Common Segments:
McMurchy - Zum Steeles Station Stop WB -- TO -- Sheridan College Term - 3/3A/4/4A/11/51/511/104 WB      536
Airport Road - Zum Steeles Station Stop WB -- TO -- Torbram - Zum Steeles Station Stop WB               488
Williams - Zum Main Station Stop SB -- TO -- Vodden - Zum Main Station Stop SB                          392
Torbram - Zum Steeles Station Stop EB -- TO -- Airport Road - Zum Steeles Station Stop EB               384
Bramalea - Zum Steeles Station Stop EB -- TO -- Torbram - Zum Steeles Station Stop EB                   383
County Court South - Zum Main Station Stop NB -- TO -- County Court North - Zum Main Station Stop NB    374
Torbram - Zum Queen Station Stop WB -- TO -- Glenvale - Zum Queen Station Stop WB                       370
Vodden - Zum Main Station Stop SB -- TO -- Nelson - Zum Main Station Stop SB                            367
Bramalea - Zum Steeles Station Stop WB -- TO -- Dixie - Zum Steeles Station Stop WB                     366
Bovaird - Zum Main Station Stop SB -- TO -- Williams - Zum Main Station Stop SB                         366
"""
