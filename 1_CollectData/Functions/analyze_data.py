# Name:                                            Renacin Matadeen
# Date:                                               04/03/2024
# Title                          Having collected data, we must now begin to analyze bus traffic
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import gc
import sys
import time
import sqlite3
import datetime
import numpy as np
import pandas as pd
# ----------------------------------------------------------------------------------------------------------------------


# Needed Variables For All Functions
td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
td_s_dt_dsply_frmt = "%d-%m-%Y"


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



def get_bearing(coord1, coord2):
    """
    coord1 = first location reported
    coord2 = current location reported

    This function will calculate the bearing between two coordinates
    Taken from: https://stackoverflow.com/questions/54873868/python-calculate-bearing-between-two-lat-long
    """
    lat1, long1 = coord1[0], coord1[1]
    lat2, long2 = coord2[0], coord2[1]

    dLon = (long2 - long1)
    x = math.cos(math.radians(lat2)) * math.sin(math.radians(dLon))
    y = math.cos(math.radians(lat1)) * math.sin(math.radians(lat2)) - math.sin(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.cos(math.radians(dLon))
    brng = np.arctan2(x,y)
    brng = np.degrees(brng)

    return brng



# ----------------------------------------------------------------------------------------------------------------------

def main_attempt(graphics_path, out_path, fl_data, bus_stp_path, bstp_af, e_out_path, e_fl_data, td_dt_mx):
    """ Is It Possible To Infer Trip Times From GTFS Data? """


    # Make Sure We Have At Least 2 Days Worth Of Data
    if len(fl_data["FILE_NAME"].tolist()) >= 2:

        #===============================================================================
        # Step #1: Gather Yesterday's Bus Location Data
        #===============================================================================
        # Find Days Between Today And Minus 1 Days | Add Fix For Date Formatting
        fl_data["DATE"] = fl_data["DATE"].astype(str)
        fl_data["DATE"] = pd.to_datetime(fl_data["DATE"], format="%Y-%m-%d")


        td_dt_mx = "02-04-2024"   # REMOVE PLS


        td_dt_mx = datetime.datetime.strptime(td_dt_mx, td_s_dt_dsply_frmt)

        # Find Pertinent Data
        fl_data = fl_data[fl_data["DATE"] >= td_dt_mx]
        df = pd.concat([pd.read_csv(path_) for path_ in [f"{out_path}/{x}" for x in fl_data["FILE_NAME"].tolist()]])

        # Format Data To Ints, DT Accessor Took Too Long
        df = df.drop_duplicates(subset=['u_id'])
        df[["YEAR", "MONTH", "DAY"]] = df["dt_colc"].str[:10].str.split('-', expand=True)
        df[["HOUR", "MINUTE", "SECOND"]] = df["dt_colc"].str[11:19].str.split(':', expand=True)

        # We Only Want Data From td_dt_mx Date
        f_day = str(td_dt_mx.day).zfill(2)
        df = df[df["DAY"] == f_day]

        # Find File, If Not Exist, Raise Error
        for file in bstp_af["FILE_NAME"].to_list():
            if "BUS_STP_DATA" in file:
                file_path = f"{bus_stp_path}/{file}"

        # Read In Data & Catch Possible Error
        try:
            bus_stops = pd.read_csv(file_path)

        except NameError:
            now = datetime.datetime.now().strftime(td_l_dt_dsply_frmt)
            print(f"{now}: Error Bus Stop File Does Not Exist")
            sys.exit(1)



        #===============================================================================
        # Step #2: Upload Yesterday's Bus Data & Bus Stop Data To Temp SQL Table, Compare
        #===============================================================================
        con = sqlite3.connect(":memory:")
        bus_stops.to_sql("BusStops", con, if_exists="replace", index=False)
        df.to_sql("TRANSIT_LOCATION_DB", con, if_exists="replace", index=False)
        del df

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

        		COALESCE(LAG(A.C_LAT)
        		OVER (PARTITION BY A.ROUTE_ID, A.TRIP_ID, A.ID, A.AVG_DIR ORDER BY A.EP_TIME), A.C_LAT)                    AS P_LAT,

        		COALESCE(LAG(A.C_LONG)
        		OVER (PARTITION BY A.ROUTE_ID, A.TRIP_ID, A.ID, A.AVG_DIR ORDER BY A.EP_TIME), A.C_LONG)                   AS P_LONG,

        		COALESCE(LAG(A.NXT_STP_ID)
        		OVER (PARTITION BY A.ROUTE_ID, A.TRIP_ID, A.ID, A.AVG_DIR ORDER BY A.EP_TIME), A.NXT_STP_ID)               AS PRV_STP_ID
        	FROM RawData AS A
        ),

        -- Step #2: Merge Bus Stop Information Onto Main Table
        WithStopData AS (
        	SELECT
        		A.ID || '_' || A.ROUTE_ID || '_' || A.TRIP_ID || '_' || A.AVG_DIR       AS U_NAME,
        		A.*,

        		B2.CLEANED_STOP_NAME_                                                            AS PRV_STP_NAME,
        		B2.CLEANED_STOP_LAT_                                                             AS PRV_STP_LAT,
        		B2.CLEANED_STOP_LON_                                                             AS PRV_STP_LONG,

        		B1.CLEANED_STOP_NAME_                                                            AS NXT_STP_NAME,
        		B1.CLEANED_STOP_LAT_                                                             AS NXT_STP_LAT,
        		B1.CLEANED_STOP_LON_                                                             AS NXT_STP_LONG


        	FROM RD AS A
        	LEFT JOIN BusStops AS B1 ON (A.NXT_STP_ID = B1.stop_id)
        	LEFT JOIN BusStops AS B2 ON (A.PRV_STP_ID = B2.stop_id)
        )

        SELECT *
        FROM WithStopData AS A
        '''


        # Read SQL Query & Perform Basic Sorting & Duplicate Removal
        data_pull = pd.read_sql_query(sql_query, con)
        con.close()
        data_pull.sort_values(["ID", "ROUTE_ID", "TRIP_ID", "EP_TIME"], inplace=True)
        data_pull.drop_duplicates(inplace=True)



"""
#===============================================================================
# Step #3: Remove Unneeded Information - Clean Up Dataset!
#===============================================================================
# Remove Entries Where Bus Is Idling, Or Has Kept Transponder Running After The First Occurence At The Last Stop | Append All Dta To New Dataframe
gb = data_pull.groupby("U_NAME")
transit_df = pd.concat([x[1].loc[x[1]["NXT_STP_NAME"].where(x[1]["NXT_STP_NAME"]==x[1]["NXT_STP_NAME"].iloc[0]).last_valid_index():x[1]["PRV_STP_NAME"].where(x[1]["PRV_STP_NAME"]==x[1]["PRV_STP_NAME"].iloc[-1]).first_valid_index()] for x in gb])


# Calculate Distance Between Current Location & Previous Location | Create A Dataframe Elaborating Distance Traveled & Speed
transit_df["DST_BTW_LOCS"] = vec_haversine((transit_df["P_LAT"].values, transit_df["P_LONG"].values), (transit_df["C_LAT"].values, transit_df["C_LONG"].values))
speed_df = transit_df.groupby(["ID", "ROUTE_ID", "TRIP_ID", "AVG_DIR"], as_index=False).agg(
    TRIP_DUR = ("EP_TIME", lambda s: round((((s.iloc[-1] - s.iloc[0])/60))/60, 2)),
    TRIP_LEN = ("DST_BTW_LOCS", lambda s: round(s.sum(), 2)),
)
speed_df["TRIP_SPD"] = speed_df["TRIP_LEN"] / speed_df["TRIP_DUR"]

# If Next Stop Is Equal To Previous Stop, Replace With Blank, Foward Fill Next Stop Values & Replace First
for n_col, p_col in zip(["NXT_STP_ID", "NXT_STP_NAME", "NXT_STP_LAT", "NXT_STP_LONG"], ["PRV_STP_ID", "PRV_STP_NAME", "PRV_STP_LAT", "PRV_STP_LONG"]):
    transit_df.loc[transit_df[n_col] == transit_df[p_col], p_col] = np.nan
    transit_df[p_col] = transit_df.groupby(["ID", "ROUTE_ID", "TRIP_ID", "AVG_DIR"])[p_col].ffill()
    transit_df[p_col] = transit_df[p_col].fillna(transit_df[n_col])
transit_df = transit_df.drop_duplicates(subset=["ID", "ROUTE_ID", "TRIP_ID", "AVG_DIR", "NXT_STP_ID", "PRV_STP_ID"], keep="last")
"""

"""
#===============================================================================
# Step #4: Determine Distance, Speed, and Bearing Between Stops, Determine Trip Type (Weekend, Weekday, Holiday Etc...)
#===============================================================================
transit_df["DST_PSTP_NXTSTP"] = vec_haversine((transit_df["PRV_STP_LAT"].values, transit_df["PRV_STP_LONG"].values), (transit_df["NXT_STP_LAT"].values, transit_df["NXT_STP_LONG"].values))
transit_df["DST_2_PBSTP"] = vec_haversine((transit_df["PRV_STP_LAT"].values, transit_df["PRV_STP_LONG"].values), (transit_df["C_LAT"].values, transit_df["C_LONG"].values))



transit_df = transit_df.merge(speed_df, how="left", on=["ROUTE_ID", "TRIP_ID", "ID", "AVG_DIR"])
transit_df["TME_2_PBSTP"] = ((transit_df["DST_2_PBSTP"] / transit_df["TRIP_SPD"])*60)*60
transit_df["ARV_TME_PBSTP"] = transit_df["EP_TIME"] - transit_df["TME_2_PBSTP"]

transit_df["SEG_BEARING"] = round(transit_df.apply(lambda x: get_bearing((x["PRV_STP_LAT"], x["PRV_STP_LONG"]), (x["NXT_STP_LAT"], x["NXT_STP_LONG"])), axis=1), 0)

transit_df["TRIP_TYPE"] = transit_df["TRIP_ID"].str.split("-").str[-2]
"""


"""
#===============================================================================
# Step #5: Reorganize Data
#===============================================================================
con = sqlite3.connect(":memory:")
transit_df.to_sql("main_data", con, index=False)

sql_query = f'''
-- Step #1: Reorganize Data & Keep Only Needed Columns
SELECT
    A.U_NAME,
    A.ID,
    A.ROUTE_ID,
    A.TRIP_ID,
    A.TRIP_TYPE,

    A.DST_PSTP_NXTSTP                                                                             AS DST_BTW_STPS,
    A.PRV_STP_ID                                                                                  AS CUR_STP_ID,
    A.PRV_STP_NAME                                                                                AS CUR_STP_NM,
    A.PRV_STP_LAT                                                                                 AS CUR_STP_LAT,
    A.PRV_STP_LONG                                                                                AS CUR_STP_LONG,
    A.ARV_TME_PBSTP                                                                               AS CUR_STP_TIME,

    A.PRV_STP_NAME || ' -- TO -- ' || A.NXT_STP_NAME                                              AS SEGMENT_NAME,
    CAST(AVG(A.SEG_BEARING)
    OVER (PARTITION BY A.U_NAME) AS INTERGER)                                                     AS AVG_DIR,

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
for col in ["CUR_STP_TIME", "NXT_STP_TIME"]:
    main_data[col] = main_data[col].astype(int)
main_data["TRVL_TIME"] = round((main_data["NXT_STP_TIME"] - main_data["CUR_STP_TIME"]) / 60, 2)
main_data = main_data[main_data["TRVL_TIME"] > 0]
"""


"""
#===============================================================================
# Step #6: Output Results
#===============================================================================
main_out_path = out_path + "/main_data.csv"
main_data.to_csv(main_out_path, index=False)
"""
