# Name:                                            Renacin Matadeen
# Date:                                               03/03/2024
# Title                           Main Logic Of Data Collector: Version 2 Memory Optimized?
#
# ----------------------------------------------------------------------------------------------------------------------
import gc
import os
import re
import sys
import json
import time
import math
import socket
import sqlite3
import requests
import urllib.request
import dropbox
import dropbox.files
import subprocess
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

import warnings
warnings.filterwarnings("ignore")
# ----------------------------------------------------------------------------------------------------------------------



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
class DataCollector:
    """ This class will gather data on both bus locations as well as weather data """


    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self, skp_dwnld=False):
        """ This function will run when the DataCollector Class is instantiated """

        # Datetime Variables
        self.td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
        self.td_s_dt_dsply_frmt = "%d-%m-%Y"


        # Where Is This Running On?
        if socket.gethostname() == "Renacins-MacBook-Pro.local":
            now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
            print(f"{now}: Running On Macbook Pro")

            db_out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
            self.db_folder = db_out_path
            self.csv_out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
            self.db_path = db_out_path + "/DataStorage.db"
            self.rfresh_tkn_path = r"/Users/renacin/Desktop/Misc/GrabToken.sh"


        elif socket.gethostname() == "raspberrypi":
            now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
            print(f"{now}: Running On RPI3")

            db_out_path = r"/home/pi/Documents/Python/BramptonTransitAnalysis/3_Data"
            self.db_folder = db_out_path
            self.csv_out_path = r"/media/pi/STORAGE"
            self.db_path = db_out_path + "/DataStorage.db"
            self.rfresh_tkn_path = r"/home/pi/Desktop/GrabToken.sh"


        else:
            now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
            print(f"{now}: Invalid Host Name")
            sys.exit(1)


        # Internalize Needed URLs: Bus Location API, Bus Routes, Bus Stops
        self.bus_loc_url    = r"https://nextride.brampton.ca:81/API/VehiclePositions?format=json"
        self.bus_routes_url = r"https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-and-Maps.aspx"
        self.bus_stops_url  = r"https://opendata.arcgis.com/api/v3/datasets/1c9869fd805e45339a9e3373fc10ce08_0/downloads/data?format=csv&spatialRefId=3857&where=1%3D1"

        # Check To See If Appropriate Sub Folders Exist, Where Are We Writting Data?
        self.out_dict = {}
        self.__out_folder_check(self.csv_out_path)

        # Create An Internal Reference To The Database Location
        self.db_path = self.db_path

        # Ensure Database Exists
        self.__db_check()


        # If Optionset Equals False, Grab Recent Bus Stop Info & Grab Route Data
        if skp_dwnld == False:

            try:
                self.__get_routes_nd_stops()

            except KeyboardInterrupt as e:
                now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
                print(f"{now}: Keyboard Interupt")
                sys.exit(1)

            except Exception as e:
                now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
                print(f"{now}: Bus Stop/Bus Route Download Error")
                sys.exit(1)

        # Collect Garbage So Everything Any Unused Memory Is Released
        gc.collect()



    # -------------------------- Private Function 1 ----------------------------
    def __out_folder_check(self, csv_out_path):
        """ On instantiation this function will be called. Check to see which
        operating system this script is running on, additionally check to see
        if the approrpriate folders are available to write to, if not create
        them."""

        # First Check To See If The Main Folder Exists!
        if not os.path.isdir(self.db_folder):
            os.makedirs(self.db_folder)

        # In The Out Directory Provided See If The Appropriate Sub Folders Exist!
        for fldr_nm in ['BUS_STP', 'BUS_LOC', 'FRMTD_DATA', 'BUS_SPEED', 'GRAPHICS', 'ERROR']:
            dir_chk = f"{csv_out_path}/{fldr_nm}"
            self.out_dict[fldr_nm] = dir_chk
            if not os.path.exists(dir_chk):
                os.makedirs(dir_chk)

        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Database & Folders Exist")



    # -------------------------- Private Function 2 ----------------------------
    def __db_check(self):
        """ On instantiation this function will be called. Create a database
        that will store bus location data; as well as basic database functionality
        data. This is a private function. It cannot be called."""

        # Connect to database check if it has data in it | Create If Not There
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS BUS_LOC_DB (
            u_id                  TEXT,
            id                    TEXT, is_deleted            TEXT, trip_update           TEXT,
            alert                 TEXT, trip_id               TEXT, start_time            TEXT,
            start_date            TEXT, schedule_relationship TEXT, route_id              TEXT,
            latitude              TEXT, longitude             TEXT, bearing               TEXT,
            odometer              TEXT, speed                 TEXT, current_stop_sequence TEXT,
            current_status        TEXT, timestamp             TEXT, congestion_level      TEXT,
            stop_id               TEXT, vehicle_id            TEXT, label                 TEXT,
            license_plate         TEXT, dt_colc               TEXT);
            ''')

            conn.commit()
            conn.close()

        except sqlite3.OperationalError as e:
            print(e)
            sys.exit(1)



        # Connect to database check if it has data in it | Create If Not There
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS U_ID_TEMP (
            u_id                    TEXT,
            timestamp               TEXT
            );
            ''')

            conn.commit()
            conn.close()

        except sqlite3.OperationalError as e:
            print(e)
            sys.exit(1)



        # Connect to database check if it has data in it | Create If Not There
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS ERROR_DB (
            timestamp               TEXT,
            e_type                  TEXT,
            delay                   TEXT
            );
            ''')

            conn.commit()
            conn.close()

        except sqlite3.OperationalError as e:
            print(e)
            sys.exit(1)



    # -------------------------- Private Function 3  ----------------------------
    def __get_bus_stops(self):
        """ On instantiation this function will be called. Using Brampton's Open
        Data API Link, Download Bus Stop Data To SQLite3 Database. This function
        should only be run on instantiation. """

        # Find Bus Stops That Are Located At A Main Terminal, Find The Associated Main Bus Terminal
        # Columns Needed stop_id where value is non-numeric, and parent_station where value is not null
        # Find Main Bus Stops In Different Location Types
        bus_stops = pd.read_csv(self.bus_stops_url)
        parent_bus_terminals = bus_stops[~bus_stops["stop_id"].str.isnumeric()]
        stops_in_terminals = bus_stops[~bus_stops["parent_station"].isnull()]
        stops_not_terminals = bus_stops[bus_stops["parent_station"].isnull() & bus_stops["stop_id"].str.isnumeric()]

        # Created A Cleaned Station Name, If Bus Stop Located In Parent Station
        con = sqlite3.connect(":memory:")
        parent_bus_terminals.to_sql("parent_bus_terminals", con, index=False)
        stops_in_terminals.to_sql("stops_in_terminals",     con, index=False)
        stops_not_terminals.to_sql("stops_not_terminals",   con, index=False)

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
        bus_stops = pd.read_sql_query(sql_query, con)
        for col in ["CLEANED_STOP_NAME", "CLEANED_STOP_LAT", "CLEANED_STOP_LON"]:
        	del bus_stops[col]
        con.close()

        dt_string = datetime.now().strftime(self.td_s_dt_dsply_frmt)
        out_path = self.out_dict["BUS_STP"] + f"/BUS_STP_DATA_{dt_string}.csv"
        bus_stops.to_csv(out_path, index=False)

        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Exported Bus Stop Data")

        return bus_stops



    # -------------------------- Private Function 4  ---------------------------
    def __get_rts(self):
        """
        Given a URL, this function navigates to Brampton Transit's Routes & Map Page,
        parses all hrefs related to routes, and returns a pandas dataframe with the
        scraped data.
        """

        # Navigate To WebPage & Grab HTML Data
        page = requests.get(self.bus_routes_url )
        soup = BeautifulSoup(page.content, "html.parser")

        # Parse All HTML Data, Find All HREF Tags
        rt_data = []
        for tag in soup.find_all('a', href=True):
            str_ref = str(tag)
            if "https://www.triplinx.ca/en/route-schedules/6/RouteSchedules/" in str_ref:
                for var in ['<a href="', '</a>']:
                    str_ref = str_ref.replace(var, "")

                # Parse Out Route Name, Direction, Group, Link, Etc...
                raw_link, dir = str_ref.split('">')
                link = raw_link.split('?')[0]
                full_data = [link + "#trips", dir] + link.split("/")[7:10]
                rt_data.append(full_data)

        # Return A Pandas Dataframe With Route Data
        return pd.DataFrame(rt_data, columns=["RT_LINK", "RT_DIR", "RT_GRP", "RT_GRP_NUM", "RT_NAME_RAW"])



    def __get_rt_stops(self, rt_links, rt_names):
        """
        Given a list of links relating to bus stops visited on certain routes, this
        function navigates through each, pulling information regarding each bus stop
        visited. This function returns a pandas dataframe with all the parsed information.
        """

        # Create A Dictionary That Will Store All The Data Gathered
        data_dict = {"stp_data": [], "rt_number_data": [],
                     "rt_name_data": [], "rt_version_data": []}

        # Create Counter For Tracking
        counter = 1
        total_rts = len(rt_names)

        # Ierate Through Each Link And Grab Bus Stop Information
        for link, name in zip(rt_links, rt_names):

            # Keep Trying To Gather Data, Granted If We Try Like 10 Times Just Say Fuck It And Pass
            pass_flag = True
            try_again_counter = 0

            # Use A While Loop To Keep Trying To Gather Data
            while pass_flag:

                # Encapsulate With A Try Except
                try:

                    # Use A Context Manager
                    with urllib.request.urlopen(link) as response:

                        # Navigate To WebPage, Pull All HTML, Convert To String, Use Regex To Pull All Stop Names
                        html = response.read()
                        soup = BeautifulSoup(html, "html.parser")

                        # Find Additional Route Information, Make A Large String And Use RE To Search
                        raw_rt_info = [str(raw_data) for raw_data in soup.find_all(class_="partner-lines-item no-padding bold")]
                        raw_rt_info = "".join(raw_rt_info)

                        # Find Route Number
                        re_pat = r'item-line item-line-color-\d{3,5}">(.{1,5})</span>'
                        route_num = re.search(re_pat, raw_rt_info).group(1)

                        # Find Route Name
                        re_pat = r'item-text no-decoration">(.{1,30})</span></p>'
                        route_name = re.search(re_pat, raw_rt_info).group(1)

                        # Find Route Version
                        route_ver = link.split("/")[-1]
                        route_ver = route_ver.replace("#trips", "")

                        # Create A List Of The Bus Stops Found W/ Name For Join To Main Data
                        hrefs = soup.find_all(class_="link-to-stop")
                        rw_bs = [name + "###" + str(x).split('">')[1].replace("</a>", "") for x in hrefs]

                        # Add Data To Dictionary
                        data_dict["stp_data"].extend(rw_bs)
                        data_dict["rt_number_data"].extend([route_num for x in range(len(rw_bs))])
                        data_dict["rt_name_data"].extend([route_name for x in range(len(rw_bs))])
                        data_dict["rt_version_data"].extend([route_ver for x in range(len(rw_bs))])

                        # Stop The While Loop
                        pass_flag = False

                        # For Logging
                        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
                        print(f"{now}: ({counter}/{total_rts}) - Parsed Bus Route Data: {name}")
                        counter += 1

                # If There Is An Error, Try Again
                except KeyboardInterrupt:
                    now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
                    print(f"{now}: Keyboard Interupt")
                    sys.exit(1)

                # If There Is An Error, Try Again
                except Exception:

                    # If Below The Try Again Limit, Keep Trying
                    if try_again_counter < 10:
                        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
                        print(f"{now}: ({counter}/{total_rts}) - Error Parsing Bus Route Data: {name}")
                        time.sleep(10)
                        try_again_counter += 1

                    # If Not Give Up On This Route
                    else:
                        pass_flag = False


        # Return A Pandas Dataframe With Route Data
        data_dict = pd.DataFrame(data_dict)
        data_dict[["RT_NM", "STP_NM"]] = data_dict["stp_data"].str.split("###", n=1, expand=True)
        data_dict.drop(["stp_data"], axis=1, inplace=True)

        # Add A Column That Shows Row Number For Each Bus Stop In A Route
        data_dict["RT_STP_NUM"] = data_dict.groupby(["RT_NM"]).cumcount() + 1

        # Add A Column That Shows How Many Bus Stops Are In A Given Route
        num_stps_df = data_dict.groupby("RT_NM", as_index=False).agg(RT_NUM_STPS = ("RT_NM", "count"))
        data_dict = data_dict.merge(num_stps_df, on='RT_NM', how='left')
        data_dict["RT_ID_VER"] = data_dict["rt_number_data"].astype(str) + "_" + data_dict["rt_version_data"].astype(str)
        data_dict.rename(columns={"rt_number_data":  "RT_ID",
                                  "rt_name_data":    "RT_NAME",
                                  "rt_version_data": "RT_VER",
                                  "RT_NM":           "RT_NAME_RAW"}, inplace=True)

        del num_stps_df

        # For Logging
        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Downloaded Bus Route Data")

        # Return Data To Variable
        return data_dict



    def __comp_data(self, parsed_df, downld_df):
        """
        Given bus stops parsed from Brampton Transit affiliated links (Gives Direction, & Order),
        and data dowloaded from Brampton Transit's Bus Stop Open Data Catalogue (Gives Exact Location),
        compare the two. Are there any Bus Stops from the parsed list that cannot be found in
        Brampton Transit's Open Data Catalogue.

        Identified Comparison Issues:
            1) In some cases "&" is written as "&amp;"
        """

        # Informed By Comparison, Make Changes
        parsed_df["STP_NM"] = parsed_df["STP_NM"].str.replace('&amp;', '&')

        # Get Unique Bus Stop Names From Parsed Dataframe
        unq_parsed_stps = pd.DataFrame(parsed_df["STP_NM"].unique().tolist(), columns=["Parsed_Bus_Stps"])
        unq_parsed_stps["In_OpenData"] = np.where(unq_parsed_stps["Parsed_Bus_Stps"].isin(downld_df["stop_name"]), "Y", "N")

        # Which Bus Stops Are Missing?
        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        misng_stps = unq_parsed_stps[unq_parsed_stps["In_OpenData"] == "N"]
        print(f"{now}: Parsed DF Len: {len(parsed_df)}, Downloaded DF Len: {len(downld_df)}, Number Of Missing Stops: {len(misng_stps)}")

        return parsed_df



    def __get_routes_nd_stops(self):
        """ On instantiation this function will be called. Using Brampton's Open
        Data API Link, Download, Bus Route Data, And Related Bus Stops, Export To
        SQLite3 Database. This function should only be run on instantiation. """

        # Download All Bus Stops From Brampton Transits Website
        dwnld_stp_data_df = self.__get_bus_stops()

        # Pull Route Info, Then Related Bus Stop Info
        rt_df = self.__get_rts()
        stp_df = self.__get_rt_stops(rt_df["RT_LINK"].to_list(), rt_df["RT_NAME_RAW"].to_list())

        # Merge Data
        stp_data_df = stp_df.merge(rt_df, on='RT_NAME_RAW', how='left')

        # Compare Bus Stop Names, Ensure All Names Are Consistent
        stp_data_df = self.__comp_data(stp_data_df, dwnld_stp_data_df)

        # Export To Folder
        dt_string = datetime.now().strftime(self.td_s_dt_dsply_frmt)
        out_path = self.out_dict["BUS_STP"] + f"/BUS_RTE_DATA_{dt_string}.csv"
        stp_data_df.to_csv(out_path, index=False)

        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Exported Route Data")



    # -------------------------- Private Function 5  ---------------------------
    def __write_error(self, tm_stamp, er_type, er_delay):
        """
        When called this function will take the type of error, it's delay, and
        write it to the error dtabase for tracking.
        """

        sql = f"""
            INSERT INTO ERROR_DB (timestamp, e_type, delay)
            VALUES ('{tm_stamp}', '{er_type}', '{er_delay}');
            """

        conn = sqlite3.connect(self.db_path)
        conn.execute(sql)
        conn.commit()



    # -------------------------- Public Function 1 -----------------------------
    def get_bus_loc(self):
        """
        When called, this function will navigate to Brampton Transit JSON GTFS
        link, scrape, format, and then upload data to the linked database. It
        will merge old data found in the database keeping new and old records.
        """

        # What Is The Start Time
        start_time = time.time()

        # Injest As JSON, and Load Into Pandas Dataframe, Include Timeout
        timeout_val = (1.5, 1.5)
        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)

        try:
            response = requests.get(self.bus_loc_url, timeout=timeout_val)
            data = json.loads(response.text)
            resp_tsmp = data["header"]["timestamp"]

            # Account For Situations Where The data["entity"] == []
            if data["entity"] != []:
                bus_loc_df = pd.json_normalize(data["entity"])

                # Rename Columns With Periods In Name
                bus_loc_df = bus_loc_df.rename(columns={
                    'vehicle.trip.trip_id': 'trip_id', 'vehicle.trip.start_time': 'start_time', 'vehicle.trip.start_date': 'start_date',
                    'vehicle.trip.schedule_relationship': 'schedule_relationship', 'vehicle.trip.route_id': 'route_id',
                    'vehicle.position.latitude': 'latitude', 'vehicle.position.longitude': 'longitude', 'vehicle.position.bearing': 'bearing', 'vehicle.position.odometer': 'odometer', 'vehicle.position.speed': 'speed',
                    'vehicle.current_stop_sequence': 'current_stop_sequence', 'vehicle.current_status': 'current_status', 'vehicle.timestamp': 'timestamp',
                    'vehicle.congestion_level': 'congestion_level', 'vehicle.stop_id': 'stop_id', 'vehicle.vehicle.id': 'vehicle_id', 'vehicle.vehicle.label': 'label',
                    'vehicle.vehicle.license_plate': 'license_plate'})

                # Create A Datetime So We Know The Exact Time In Human Readable Rather Than Timestamp From EPOCH
                bus_loc_df["dt_colc"] = pd.to_datetime(bus_loc_df["timestamp"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')

                # Create A U_ID Column Based On Route ID, Vehicle ID, And Timestamp To Act As A Unique ID For The Table
                bus_loc_df["u_id"] = bus_loc_df["route_id"] + "_" + bus_loc_df["vehicle_id"] + "_" + bus_loc_df["timestamp"].astype(str)

                # Upload New Data To An Intermediary Temp Table, Check If The U_IDs Are In A Cache From 10 Min Ago, If Not Add To Database
                conn = sqlite3.connect(self.db_path)
                bus_loc_df.to_sql('bus_temp', conn, if_exists='replace', index=False)
                conn.execute("""
                    INSERT INTO BUS_LOC_DB(u_id, id, is_deleted, trip_update, alert, trip_id, start_time,
                                           start_date, schedule_relationship, route_id, latitude, longitude, bearing,
                                           odometer, speed, current_stop_sequence, current_status, timestamp, congestion_level,
                                           stop_id, vehicle_id, label, license_plate, dt_colc)
                    SELECT
                        A.u_id,                  A.id,             A.is_deleted,
                        A.trip_update,           A.alert,          A.trip_id,
                        A.start_time,            A.start_date,     A.schedule_relationship,
                        A.route_id,              A.latitude,       A.longitude,
                        A.bearing,               A.odometer,       A.speed,
                        A.current_stop_sequence, A.current_status, A.timestamp,
                        A.congestion_level,      A.stop_id,        A.vehicle_id,
                        A.label,                 A.license_plate,  A.dt_colc

                    FROM
                        bus_temp AS A

                    WHERE NOT EXISTS (
                        SELECT u_id FROM U_ID_TEMP AS B
                        WHERE B.u_id = A.u_id)
                """)
                conn.execute('DROP TABLE IF EXISTS bus_temp')
                conn.commit()
                conn.close()

                # Combine U_IDs From New Data & U_IDs In Most Recent Cache
                conn = sqlite3.connect(self.db_path)
                all_uids = pd.concat([pd.read_sql_query("SELECT * FROM U_ID_TEMP", conn), bus_loc_df[["u_id", "timestamp"]]])
                conn.commit()
                conn.close()

                # Sort, Where The Most Recent U_IDs Are At The Top, Remove Duplicates
                all_uids["timestamp"] = all_uids["timestamp"].astype('int')
                all_uids = all_uids.sort_values(by="timestamp", ascending=False)
                all_uids = all_uids.drop_duplicates()

                # Find The Max Time Stamp, And Only Keep Rows A Couple Of Min Back From That Value
                min_back = 8
                max_timestamp = all_uids["timestamp"].max() - (min_back * 60)
                all_uids = all_uids[all_uids["timestamp"] >= max_timestamp]

                # Now That We Have
                conn = sqlite3.connect(self.db_path)
                all_uids.to_sql('U_ID_TEMP', conn, if_exists='replace', index=False)
                conn.commit()
                conn.close()


        # For Each Error Where Data Was Not Collected Make Add Occurence Time To A Tracking Database!
        except requests.exceptions.Timeout:
            # When Did The Exception Occur?
            self.__write_error(now, "Requests Timeout Exception", "10")
            print(f"{now}: Requests Timeout Exception")
            time.sleep(10)


        except requests.exceptions.ConnectionError:
            # When Did The Exception Occur?
            self.__write_error(now, "Requests Connection Error Exception", "10")
            print(f"{now}: Requests Connection Error Exception")
            time.sleep(10)


        except ConnectionError:
            # When Did The Exception Occur?
            self.__write_error(now, "Connection Error Timeout", "10")
            print(f"{now}: General Connection Error Timeout")
            time.sleep(10)


        except Exception as e:
            # When Did The Exception Occur?
            self.__write_error(now, "General Error", "10")
            print(f"{now}: General Error, (Type {e})")
            time.sleep(10)



    # -------------------------- Public Function 2 -----------------------------
    def xprt_data(self, out_path, out_table, dup_col, input_val=True):
        """
        When called, this function will gather all data in a given table, format
        the data in that data table, export it as a CSV to a given path, and then
        empty out the the chosen table if the appropriate choice is given.

        Only export data if database has data in it. If it's empty then pass.
        """

        # Define Needed Variables
        dt_nw = datetime.now().strftime(self.td_s_dt_dsply_frmt)
        tm_nw = datetime.now().strftime(self.td_l_dt_dsply_frmt)

        # Define Where The File Will Be Written
        out_path = self.out_dict[out_path]
        db_path = out_path + f"/{out_table}_{dt_nw}.csv"

        # Read Data From Defined Database, Remove Duplicates
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f"SELECT * FROM {out_table}", conn)
        df = df.drop_duplicates()
        conn.close()

        # Only Export Data If Dataframe Isn't Empty
        if not df.empty:

            # We Need To Keep An A Version Of The Input Dataframe With No Rows & Just Columns
            empty_df = df.iloc[:0].copy()

            # If List Then All Cols In List
            if type(input_val) == list:
                df = df[input_val]
                df = df.drop_duplicates(subset=[dup_col])

            # If Value = True Then All
            elif input_val == True:
                pass

            # If Invalid, Let The User Know
            else:
                print("Invalid Arguement")
                raise ValueError("Invalid Choice: Choose Either List Of Columns, Or True")

            # Export Data
            df.to_csv(db_path, index=False)
            del df

            # Delete The SQL Table, See If That Helps Drop The Database Size?
            conn = sqlite3.connect(self.db_path)
            conn.execute(f"""DROP TABLE IF EXISTS {out_table}""")
            conn.commit()
            conn.close()

            # Write Over DB Table So It's Now Empty
            conn = sqlite3.connect(self.db_path)
            empty_df.to_sql(f"{out_table}", conn, if_exists="replace", index=False)
            conn.commit()
            conn.close()

            # We Need To Make Sure We Clean Up Everything, Run The Vacuum Command To Clean Up Temp Space
            conn = sqlite3.connect(self.db_path)
            conn.execute(f"""vacuum""")
            conn.commit()
            conn.close()

            # For Logging
            print(f"{tm_nw}: Exported CSV & DB Table - {out_table} Cleaned")



    # ------------------------- Public Function 3 ------------------------------
    def return_files_dates(self, out_path):
        """
        When called, this function will look at all the files in a folder and
        return a formatted pandas dataframe for the user to query in later functions
        """

        # Navigate To Data Folder | Get All Appropriate Files
        out_path = self.out_dict[out_path]

        dir_list = [x for x in os.listdir(out_path) if ".csv" in x]
        df = pd.DataFrame(dir_list, columns=['FILE_NAME'])

        # Create A Dataframe With The Time The File Was Created & Output
        df["DATE"] = df["FILE_NAME"].str.split('_').str[-1]
        df["DATE"] = df["DATE"].str.replace(".csv", "", regex=False)
        df["DATE"] = pd.to_datetime(df["DATE"], format = self.td_s_dt_dsply_frmt)

        return out_path, df



    # ------------------------- Public Function 4 ------------------------------
    def upld_2_dbx(self):
        """
        When called, this function will upload all graphics found in the graphics
        folder and upload them to the connected dropbox application folder.
        """

        # For Logging
        tm_nw = datetime.now().strftime(self.td_l_dt_dsply_frmt)

        try:
            # Navigate To Shell Script Location, And Generate A New Token
            raw_resp = subprocess.check_output(['sh', self.rfresh_tkn_path], stderr=subprocess.DEVNULL)
            raw_resp = raw_resp.decode('ascii')
            json_data = json.loads(raw_resp)

            # Create An Instance Of DBX Class
            dbx = dropbox.Dropbox(json_data["access_token"])

            # If Files In Graphics Folder, Then Upload
            if len(os.listdir(self.out_dict["GRAPHICS"])) > 0:
                for file_ in os.listdir(self.out_dict["GRAPHICS"]):
                    out_path = self.out_dict["GRAPHICS"]
                    file_path = f"{out_path}/{file_}"
                    with open(file_path, "rb") as f:
                        file_data = f.read()
                        dbx.files_upload(file_data, f"/{file_}", mode=dropbox.files.WriteMode.overwrite)
                print(f"{tm_nw}: Uploaded Graphics To DropBox Folder")

            # If No Files Exit
            else:
                print(f"{tm_nw}: No Files To Upload To DropBox Folder")

        except Exception as e:
            # For Logging | Bad
            tm_nw = datetime.now().strftime(self.td_l_dt_dsply_frmt)
            print(f"{tm_nw}: Failure, Could Not Upload Graphics To DropBox Folder")
            print(f"{e}")



    # ---------------- Private Function #1 For Public Function 5 ---------------
    def __frmt_data_s1(self, data_f, td_dt_mx):
        """ In step 1 of cleaning the main bus data, we must first convert variables
        to an appropriate datatype (to save memory usage) and then determine the
        previous and next bus stop that a bus might have visited. """

        # Sanitize Input
        df = data_f.copy()

        # Collect Garbage
        del data_f
        gc.collect()

        # Format Data To Ints, DT Accessor Took Too Long
        df = df.drop_duplicates(subset=['u_id'])
        df[["YEAR", "MONTH", "DAY"]] = df["dt_colc"].str[:10].str.split('-', expand=True)
        df[["HOUR", "MINUTE", "SECOND"]] = df["dt_colc"].str[11:19].str.split(':', expand=True)


        # We Only Want Data From td_dt_mx Date
        f_day = str(td_dt_mx.day).zfill(2)
        df = df[df["DAY"] == f_day]


        df = df.drop(["YEAR", "MONTH", "DAY", "MINUTE", "SECOND", "u_id"], axis=1)
        df.rename(columns = {"timestamp" :  "EP_TIME",
                             "route_id"  :  "ROUTE_ID",
                             "trip_id"   :  "TRIP_ID",
                             "vehicle_id":  "V_ID",
                             "bearing"   :  "DIR",
                             "latitude"  :  "C_LAT",
                             "longitude" :  "C_LONG",
                             "stop_id"   :  "NXT_STP_ID",
                             "dt_colc"   :  "DATE_TM"}, inplace=True)

        # Try To Reduce Memory Usage
        df["C_LAT"] = df["C_LAT"].astype(np.float32)
        df["C_LONG"] = df["C_LONG"].astype(np.float32)
        df["NXT_STP_ID"] = df["NXT_STP_ID"].astype("Int32")
        df["EP_TIME"] = df["EP_TIME"].astype("Int32")
        df["V_ID"] = df["V_ID"].astype("Int16")
        df["DIR"] = df["DIR"].astype("Int16")
        df["TRIP_ID"] = df["TRIP_ID"].astype("category")
        df["ROUTE_ID"] = df["ROUTE_ID"].astype("category")
        df["HOUR"] = df["HOUR"].astype("category")


        # We Need To Determine Average DIR For Each Trip
        avg_dir = df[["TRIP_ID", "DIR"]].copy()
        avg_dir = avg_dir.groupby(["TRIP_ID"], as_index=False).agg(AVG_DIR = ("DIR", "mean"))
        avg_dir["AVG_DIR"] = avg_dir["AVG_DIR"].astype(int)
        df = df.merge(avg_dir, how="left", on=["TRIP_ID"])
        df.sort_values(["TRIP_ID", "EP_TIME"], inplace=True)

        # Reduce Memory Usage Of New Fields Created
        df["AVG_DIR"] = df["AVG_DIR"].astype("Int16")

        # Collect Garbage
        del avg_dir
        gc.collect()

        # For Each Stop Entry, We Need To Know The Previous Lat, Long, And STP_ID,
        for col in [("P_LAT", "C_LAT"), ("P_LONG", "C_LONG"), ("PRV_STP_ID", "NXT_STP_ID")]:
            df[col[0]] = df.groupby(['ROUTE_ID', 'TRIP_ID', 'AVG_DIR'])[col[1]].shift(1)
            df[col[0]].fillna(df[col[1]], inplace=True)

        # Find Bus Stop Data
        for file in os.listdir(self.out_dict["BUS_STP"]):
            if "BUS_STP_DATA" in file:
                file_path = f'{self.out_dict["BUS_STP"]}/{file}'

        # Read In Bus Stop Data
        needed_cols = ["CLEANED_STOP_NAME_", "CLEANED_STOP_LAT_", "CLEANED_STOP_LON_", "stop_id"]
        bus_stops = pd.read_csv(file_path, usecols=needed_cols)

        # Reduce Size Of Columns
        bus_stops["CLEANED_STOP_LAT_"] = bus_stops["CLEANED_STOP_LAT_"].astype(np.float32)
        bus_stops["CLEANED_STOP_LON_"] = bus_stops["CLEANED_STOP_LON_"].astype(np.float32)
        bus_stops["stop_id"] = bus_stops["stop_id"].astype("Int32")

        # Create Unique Identifier, And Merge Bus Stop Information Onto Main Table
        df["U_NAME"] = df["ROUTE_ID"].astype(str) + "_" + df["TRIP_ID"].astype(str) + "_" + df["AVG_DIR"].astype(str)
        df = df.merge(bus_stops, how="left", left_on=["NXT_STP_ID"], right_on=["stop_id"]).rename(columns = {"CLEANED_STOP_NAME_": "NXT_STP_NAME",
                                                                                                             "CLEANED_STOP_LAT_": "NXT_STP_LAT",
                                                                                                             "CLEANED_STOP_LON_": "NXT_STP_LONG"
                                                                                                             }).drop(["stop_id"], axis=1)

        df = df.merge(bus_stops, how="left", left_on=["PRV_STP_ID"], right_on=["stop_id"]).rename(columns = {"CLEANED_STOP_NAME_": "PRV_STP_NAME",
                                                                                                             "CLEANED_STOP_LAT_": "PRV_STP_LAT",
                                                                                                             "CLEANED_STOP_LON_": "PRV_STP_LONG"
                                                                                                             }).drop(["stop_id"], axis=1)

        # Collect Garbage
        del bus_stops
        gc.collect()

        # Final Bits Of Formatting
        df.sort_values(["ROUTE_ID", "TRIP_ID", "EP_TIME"], inplace=True)
        df.drop_duplicates(inplace=True)

        # For Logging
        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Data Formatting Step #1 - Complete")

        return df



    # ---------------- Private Function #2 For Public Function 5 ---------------
    def __frmt_data_s2(self, data_f, td_dt_mx):
        """ In step 2 of cleaning the main bus data, remove unneccesary rows within a
        trip, we only need the row closest to a given bus stop in a trip - this reduces
        the amount of data recording idling time. Once complete we then calculate the
        speed of the bus, given only the observations from that trip (no inferences). Once
        we have the speed of the bus, we determine the arrival time to the next bus stop. """

        # Sanitize Input
        data_pull = data_f.copy()
        del data_f
        gc.collect()

        # Remove Entries Where Bus Is Idling, Or Has Kept Transponder Running After The First Occurence At The Last Stop | Append All Dta To New Dataframe
        gb = data_pull.groupby("U_NAME")
        transit_df = pd.concat([x[1].loc[x[1]["NXT_STP_NAME"].where(x[1]["NXT_STP_NAME"]==x[1]["NXT_STP_NAME"].iloc[0]).last_valid_index():x[1]["PRV_STP_NAME"].where(x[1]["PRV_STP_NAME"]==x[1]["PRV_STP_NAME"].iloc[-1]).first_valid_index()] for x in gb])
        del data_pull, gb
        gc.collect()


        # Calculate Distance Between Current Location & Previous Location | Create A Dataframe Elaborating Distance Traveled & Speed
        transit_df["DST_BTW_LOCS"] = vec_haversine((transit_df["P_LAT"].values, transit_df["P_LONG"].values), (transit_df["C_LAT"].values, transit_df["C_LONG"].values))

        # First Create A Copy Of Main Data Table | We Only Need Certain Columns, Not All!
        speed_df = transit_df[['U_NAME', 'TRIP_ID', 'ROUTE_ID', 'V_ID', 'AVG_DIR', 'EP_TIME', 'HOUR', 'DST_BTW_LOCS']].copy()


        # Find The Previous Travel Time, Determine The Trip Speed & Trip Duration
        speed_df["P_EP_TIME"] = speed_df.groupby(["U_NAME"])["EP_TIME"].shift(+1)
        speed_df.dropna(subset=["P_EP_TIME"], inplace=True)
        speed_df["TRIP_DUR"] = (speed_df["EP_TIME"] - speed_df["P_EP_TIME"]) / 3600
        speed_df["TRIP_SPD"] = speed_df["DST_BTW_LOCS"] / speed_df["TRIP_DUR"]

        # Round & Convert Data Types To Save Memory?
        speed_df["TRIP_DUR"] = speed_df["TRIP_DUR"].astype(np.float16)
        speed_df["TRIP_SPD"] = speed_df["TRIP_SPD"].astype(np.float16)
        gc.collect()

        # Create A Data Frame Elaborating The Average Speed For A Trip, This May Be Useful In The Future Keep It!
        for col in ["ROUTE_ID", "TRIP_ID", "HOUR"]:
            speed_df[col] = speed_df[col].astype("object")
        speed_df = speed_df.groupby(["ROUTE_ID", "TRIP_ID", "AVG_DIR", "V_ID"], as_index=False).agg(TRIP_SPD = ("TRIP_SPD", "mean" ),
                                                                                                    HOUR     = ("HOUR"    , "first")
                                                                                                    )

        # The GroupBy Drops Data Types For Some Reason, Convert Back To Appropriate Datatype
        speed_df["TRIP_ID"] = speed_df["TRIP_ID"].astype("category")
        speed_df["ROUTE_ID"] = speed_df["ROUTE_ID"].astype("category")
        speed_df["AVG_DIR"] = speed_df["AVG_DIR"].astype("Int16")
        speed_df["V_ID"] = speed_df["V_ID"].astype("Int16")

        # Export Speed DF To Folder
        cleaned_dt = f"{td_dt_mx.day:0>2}-{td_dt_mx.month:0>2}-{td_dt_mx.year}"
        dt_string = datetime.now().strftime(self.td_s_dt_dsply_frmt)
        out_path = self.out_dict["BUS_SPEED"] + f"/BUS_SPEED_DATA_{cleaned_dt}.csv"
        speed_df.to_csv(out_path, index=False)

        # If Next Stop Is Equal To Previous Stop, Replace With Blank, Foward Fill Next Stop Values & Replace First
        for n_col, p_col in zip(["NXT_STP_ID", "NXT_STP_NAME", "NXT_STP_LAT", "NXT_STP_LONG"], ["PRV_STP_ID", "PRV_STP_NAME", "PRV_STP_LAT", "PRV_STP_LONG"]):
            transit_df.loc[transit_df[n_col] == transit_df[p_col], p_col] = np.nan
            transit_df[p_col] = transit_df.groupby(["ROUTE_ID", "TRIP_ID", "AVG_DIR"])[p_col].ffill()
            transit_df[p_col] = transit_df[p_col].fillna(transit_df[n_col])

        # Only Keep The Last Occurence If Duplicates Are Found
        transit_df = transit_df.drop_duplicates(subset=["ROUTE_ID", "TRIP_ID", "AVG_DIR", "NXT_STP_ID", "PRV_STP_ID"], keep="last")

        # Just Want To Know Te Time The Bus Arrived At It's Next Stop Given Average Speed
        transit_df = transit_df.drop(["HOUR", "P_LAT", "P_LONG", "PRV_STP_ID", "PRV_STP_NAME", "PRV_STP_LAT", "PRV_STP_LONG", "DST_BTW_LOCS"], axis=1)
        transit_df["DTS_2_NXT_STP"] = vec_haversine((transit_df["C_LAT"].values, transit_df["C_LONG"].values), (transit_df["NXT_STP_LAT"].values, transit_df["NXT_STP_LONG"].values))
        transit_df["DTS_2_NXT_STP"] = round(transit_df["DTS_2_NXT_STP"], 2)

        # Merge both Tables Together
        transit_df = transit_df.merge(speed_df, how="left", on=["ROUTE_ID", "TRIP_ID", "AVG_DIR", "V_ID"])
        del speed_df
        gc.collect()

        # Calculate Speed
        transit_df["SEC_2_NXT_STP"]  = (transit_df["DTS_2_NXT_STP"] / transit_df["TRIP_SPD"]) * 3600
        transit_df["NXT_STP_ARV_TM"] = transit_df["EP_TIME"] + transit_df["SEC_2_NXT_STP"]
        transit_df["NXT_STP_ARV_TM"] = transit_df["NXT_STP_ARV_TM"].astype(dtype = int, errors = 'ignore')
        transit_df["NXT_STP_ARV_DTTM"] = pd.to_datetime(transit_df["NXT_STP_ARV_TM"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')

        # Remove Zeros From NXT_STP_ARV_TM
        transit_df = transit_df[transit_df["NXT_STP_ARV_TM"] != 0]

        # For Logging
        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Data Formatting Step #2 - Complete")

        return transit_df



    # ---------------- Private Function #3 For Public Function 5 ---------------
    def __frmt_data_s3(self, data_f):
        """ In step 3 of cleaning the main bus data, we infer the bus stops in between observed
        bus stops. A bus doesn't always stop at every bus stop, but we still need to have that data.
        Mark data that we have as informed data (closest we have to actual observation). Merge onto
        a list of routes in order, and use ReGex to find the clusters of bus stops that were not
        visited. From there use the information we have to determine when the bus might have passed
        that bus stop. """

        # Sanitize Input
        transit_df = data_f.copy()
        del data_f
        gc.collect()

        # We Only Need Certain Bits Of Data, Also For The Data That We Have Mark It As IE (Informed Estimation)
        transit_df["DATA_TYPE"] = "IE"
        transit_df = transit_df[['TRIP_ID', 'ROUTE_ID',
                                 'V_ID', 'NXT_STP_NAME',
                                 'NXT_STP_ARV_TM', 'NXT_STP_ARV_DTTM',
                                 'NXT_STP_ID', 'DATA_TYPE']].rename(columns={'NXT_STP_NAME': "STP_NM",
                                                                             'NXT_STP_ARV_TM': "STP_ARV_TM",
                                                                             'NXT_STP_ARV_DTTM': "STP_ARV_DTTM",
                                                                             'NXT_STP_ID': "STP_ID"})

        # We Need To Join The Trip Data To The Bus Stops (In Order) For A Given Route
        trips_obs = transit_df.groupby(["TRIP_ID"], as_index=False).agg(TRIP_ID = ("TRIP_ID", "first"),
                                                                        RT_ID   = ("ROUTE_ID", "first")
                                                                        )

        trips_obs["RT_ID"] = trips_obs["RT_ID"].str.split("-").str[0]
        trips_obs["RT_ID"] = trips_obs["RT_ID"].astype(dtype = int, errors = 'ignore')
        trips_obs["RT_ID"] = trips_obs["RT_ID"].astype("Int16")

        # Read In Routes Data, Only Look At Needed Data, Convert To Smaller Data Type
        for file in os.listdir(self.out_dict["BUS_STP"]):
            if "BUS_RTE_DATA" in file:
                stp_file_path = f'{self.out_dict["BUS_STP"]}/{file}'

        needed_cols = ['RT_ID', 'RT_NAME',
                       'RT_VER', 'STP_NM',
                       'RT_ID_VER', 'RT_DIR',
                       'RT_GRP', 'RT_GRP_NUM']

        bus_routes = pd.read_csv(stp_file_path, usecols = needed_cols)

        # Before Using Bus Routes, We Need To Make Sure The Data Contains No Duplicates As Identified In Previous Attemps
        bus_routes["U_ID"] = bus_routes["RT_ID"].astype(str) + "_" + bus_routes["RT_NAME"].astype(str) + "_" + bus_routes["RT_VER"].astype(str)
        bus_routes = bus_routes.drop_duplicates()

        # Recreate Stop Num & Total Number Of Stops
        bus_routes["RT_STP_NUM"] = bus_routes.groupby(["U_ID"]).cumcount() + 1
        num_stps_df = bus_routes.groupby("U_ID", as_index=False).agg(RT_NUM_STPS = ("U_ID", "count"))
        bus_routes = bus_routes.merge(num_stps_df, on='U_ID', how='left')
        del num_stps_df, bus_routes["U_ID"]
        gc.collect()

        # Only Keep Data That Is In Scope
        bus_routes = bus_routes[bus_routes["RT_ID"].isin(trips_obs["RT_ID"])]

        # Try To Reduce Memory Usage
        bus_routes["RT_ID"]       = bus_routes["RT_ID"].astype("Int16")
        bus_routes["RT_VER"]      = bus_routes["RT_VER"].astype("Int16")
        bus_routes["RT_STP_NUM"]  = bus_routes["RT_STP_NUM"].astype("Int16")
        bus_routes["RT_NUM_STPS"] = bus_routes["RT_NUM_STPS"].astype("Int16")
        bus_routes["RT_GRP_NUM"]  = bus_routes["RT_GRP_NUM"].astype("Int16")

        # Left Join Bus Route Data Onto Unique Trip Data Using RT_ID As A Key
        trips_obs = trips_obs.merge(bus_routes, how="left", on=["RT_ID"])
        trips_obs = trips_obs.merge(transit_df, how="left", on=["TRIP_ID", "STP_NM"])
        del transit_df, bus_routes
        gc.collect()

        # Create A Column That Identifies The Trip ID, RT_ID, And RT_VER, Remove U_IDs That Have No Data In Them
        trips_obs["U_ID"] = trips_obs["TRIP_ID"].astype(str) + "_" + trips_obs["RT_ID"].astype(str) + "_" + trips_obs["RT_VER"].astype(str)
        count_df = trips_obs.groupby(["U_ID"], as_index=False).agg(U_ID  = ("U_ID", "first"),
                                                                   COUNT = ("DATA_TYPE", "count")
                                                                   )

        count_df[["TRIP_ID", "RT_ID", "RT_VER"]] = count_df["U_ID"].str.split('_', expand=True)
        count_df["COUNT_NAME"] = count_df["TRIP_ID"].astype(str) + "_" + count_df["COUNT"].astype(str)

        # Only Keep Trips That Actually Happened, Remove Eroneous Data
        max_obs = count_df.groupby(["TRIP_ID", "RT_ID"], as_index=False).agg(COUNT = ("COUNT", "max"))
        max_obs["COUNT_NAME"] = max_obs["TRIP_ID"].astype(str) + "_" + max_obs["COUNT"].astype(str)
        max_obs.drop(["TRIP_ID", "RT_ID", "COUNT"], axis=1, inplace=True)

        # Merge Back To Count DF, Make Sure We Are Looking At Correct U_IDs
        count_df = max_obs.merge(count_df, how="left", on=["COUNT_NAME"])
        trips_obs = trips_obs[trips_obs["U_ID"].isin(count_df["U_ID"])]
        del max_obs, count_df
        gc.collect()


        # Need To Solve The Later Later Stop Earlier Time Issue
        # Make An Indicator For Each Row
        trips_obs["ROW_ID"] = range(len(trips_obs))


        # Make A Copy Of The Database, Keep Only Needed Columns
        test_df = trips_obs[["STP_ARV_TM", "U_ID", "ROW_ID"]].copy()
        test_df = test_df.dropna(subset=["STP_ARV_TM"])


        # If U_ID Not In Test_DF Don't Keep It In Main DF
        trips_obs = trips_obs[trips_obs["U_ID"].isin(test_df["U_ID"])]

        # We Only Want To Keep Bus Trips With More Than One 1 Observation, Need To Know Next STP Time, It Must Be Bigger
        test_df = pd.concat([x[1] for x in test_df.groupby("U_ID") if len(x[1]) > 1])
        test_df['NXT_STP_ARV_TM'] = test_df.groupby(['U_ID'])['STP_ARV_TM'].shift(-1)
        test_df['TM_DIFF'] = test_df['NXT_STP_ARV_TM'] - test_df['STP_ARV_TM']


        # We Need To Know Which Rows To Remove Only Keep Data Between First Observation Of Negative Value And Everything Else
        gb = test_df.groupby("U_ID")
        data = []
        for x in gb:

            # Get All Time Diffs As List
            td_df_lst = x[1]["TM_DIFF"].tolist()

            # If Negative Value In TM_DIFF Column
            if any(tm_stampt < 0 for tm_stampt in td_df_lst):
                data.append(x[1].loc[x[1]["TM_DIFF"].where(x[1]["TM_DIFF"] < 0).first_valid_index():])

        # Put Everything Together
        test_df = pd.concat(data)
        test_df.drop(["NXT_STP_ARV_TM", "TM_DIFF", "STP_ARV_TM", "U_ID"], axis=1, inplace=True)
        test_df["ERASE_DATA"] = "YES"


        # Merge Onto Main Table, Clean Up
        trips_obs = trips_obs.merge(test_df, how="left", on=["ROW_ID"])
        trips_obs.loc[trips_obs["STP_ARV_TM"].isna(), "ERASE_DATA"] = "YES"
        del test_df
        gc.collect()


        # Convert Column To String, Remove Erroneous Data
        for col in ["ROUTE_ID", "STP_ARV_DTTM", "DATA_TYPE"]:
            trips_obs[col] = trips_obs[col].astype(str)
        for col in ["ROUTE_ID", "STP_ARV_DTTM", "DATA_TYPE"]:
            trips_obs.loc[trips_obs["ERASE_DATA"] == "YES", col] = ""
        for col in ["V_ID", "STP_ARV_TM"]:
            trips_obs.loc[trips_obs["ERASE_DATA"] == "YES", col] = np.nan

        # Remove Unneeded Column
        trips_obs.drop(["ERASE_DATA", "ROW_ID"], axis=1, inplace=True)


        # We Only Want Data Between The First Occurence, And The Last Of A Given Trip, Remove Occurences With No Data
        data = []
        gb = trips_obs.groupby("TRIP_ID")
        for x in gb:

            # If There Is Data In The DATA_TYPE Column, Then Append Later, Else Pass
            if any(d_tp == "IE" for d_tp in x[1]["DATA_TYPE"].to_list()):
                data.append(x[1].loc[x[1]["DATA_TYPE"].where(x[1]["DATA_TYPE"]=="IE").first_valid_index():x[1]["DATA_TYPE"].where(x[1]["DATA_TYPE"]=="IE").last_valid_index()])

        trips_obs = pd.concat(data)
        del gb, data, trips_obs["U_ID"]


        # Drop Duplicates Again, This Time Based On TRIP_ID, And Stop Number
        trips_obs = trips_obs.drop_duplicates(subset=["TRIP_ID", "RT_STP_NUM"])


        # Create An Encoding, For A New Column. If There Is Data In The Timestampt Then 1, Else 0
        trips_obs["DATA_FLG"] = "1"
        trips_obs.loc[trips_obs["STP_ARV_TM"].isna(), "DATA_FLG"] = "0"

        # Create A Temp Row Index, We Will Use This To Reorganize Data
        trips_obs["TEMP_IDX"] = range(len(trips_obs))

        # Find Bus Loc Data
        for file in os.listdir(self.out_dict["BUS_STP"]):
            if "BUS_STP_DATA" in file:
                file_path = f'{self.out_dict["BUS_STP"]}/{file}'

        # Read In Bus Loc Data & Merge To Trips Obs DF
        needed_cols = ['stop_id', 'stop_name', 'CLEANED_STOP_LAT_', 'CLEANED_STOP_LON_']
        bus_locs = pd.read_csv(file_path, usecols = needed_cols)
        bus_locs.rename(columns = {"stop_name": "STP_NM",
                                   "stop_id": "STP_ID",
                                   "CLEANED_STOP_LAT_": "STP_LAT",
                                   "CLEANED_STOP_LON_": "STP_LON"}
                                   ,inplace = True)

        # Split Methodolgies To Merge Data. If We Have Stop ID, Join On Stop ID, If We Don't Have Stop ID, Join On Name, But Keep The First One
        stp_id_merge = trips_obs[trips_obs["DATA_TYPE"] == "IE"]
        stp_id_merge = stp_id_merge.merge(bus_locs[["STP_ID", "STP_LAT", "STP_LON"]].copy(), how="left", on=["STP_ID"])

        stp_nm_merge = trips_obs[trips_obs["DATA_TYPE"] != "IE"]
        stp_nm_merge = stp_nm_merge.merge(bus_locs[["STP_NM", "STP_LAT", "STP_LON"]].copy(), how="left", on=["STP_NM"])
        stp_nm_merge = stp_nm_merge.drop_duplicates(subset=['TRIP_ID', 'RT_STP_NUM'])

        # Merge Data Together
        trips_obs = pd.concat([stp_id_merge, stp_nm_merge])
        trips_obs = trips_obs.sort_values("TEMP_IDX")
        del needed_cols, bus_locs, stp_id_merge, stp_nm_merge, trips_obs["TEMP_IDX"]
        gc.collect()


        # Determine Distance To Next Bus Stop
        trips_obs['NXT_STP_LAT'] = trips_obs['STP_LAT'].shift(-1)
        trips_obs['NXT_STP_LON'] = trips_obs['STP_LON'].shift(-1)
        trips_obs['NXT_STP_NM'] = trips_obs.groupby('TRIP_ID')['STP_NM'].shift(-1)

        trips_obs["DTS_2_NXT_STP"] = vec_haversine((trips_obs["STP_LAT"].values, trips_obs["STP_LON"].values), (trips_obs["NXT_STP_LAT"].values, trips_obs["NXT_STP_LON"].values))
        trips_obs.drop(columns=["STP_LAT", "STP_LON", "NXT_STP_LAT", "NXT_STP_LON"], inplace = True)

        # There Are Situations Where The DTS_2_NXT_STP Is Null, Fill With 0
        trips_obs["DTS_2_NXT_STP"] = trips_obs["DTS_2_NXT_STP"].fillna(0)


        # Iterate Through The Data Looking Patterns, Find Clusters Of Missing Data, Use Regex To Find All Matches
        re_pat = r"(?:1)0{1,100}"

        # Iterate Through Matches & Find Corresponding Pattern In String & Index List, Create An Index That Will Help Identify Order
        trips_obs["IDX_R"] = np.arange(len(trips_obs))
        trips_obs = trips_obs.reset_index()
        df_flag_str = "".join(trips_obs["DATA_FLG"].tolist())
        for cntr, x in enumerate(re.finditer(re_pat, df_flag_str)):

            # Convert To List, & Fix
            grp_mtch_idx = list(x.span())

            # Find The Needed Time Data
            time_data = list(trips_obs.iloc[grp_mtch_idx[0]:grp_mtch_idx[1]+1]["STP_ARV_TM"].to_numpy())

            # Find The Total Duration Of The Trip (Find Time At Begining -1 Of Cluster & Time At Ending +1 Of Cluster)
            total_distance = sum(trips_obs.iloc[grp_mtch_idx[0]:grp_mtch_idx[1]]["DTS_2_NXT_STP"].to_numpy())
            total_time = time_data[-1] - time_data[0]
            total_time = total_time / 3600

            # Determine Average Speed
            c_svg_spd = total_distance / total_time

            # Time Between Last Observation Before Outage, And First Observation After Outage
            time_btw_stops = trips_obs.iloc[grp_mtch_idx[1]]["STP_ARV_TM"] - trips_obs.iloc[grp_mtch_idx[0]]["STP_ARV_TM"]

            # Set Value Between Index As Cluster # ID
            trips_obs.at[grp_mtch_idx[0] + 1:grp_mtch_idx[1] -1, "TRIP_CLUSTER_ID"] = f"C{cntr}"
            trips_obs.at[grp_mtch_idx[0] + 1:grp_mtch_idx[1] -1, "CLUSTER_AVG_SPD"] = c_svg_spd

            # We Need To Know The Timestamp Before The Error Cluster Began
            trips_obs.at[grp_mtch_idx[0] + 1:grp_mtch_idx[1] -1, "ERROR_START_TIME"] = trips_obs.iloc[grp_mtch_idx[0]]["STP_ARV_TM"]


        # Determine The Time It Took Given The Speed And Distance
        trips_obs["SECS_TRVL_DSTNC"] = (trips_obs["DTS_2_NXT_STP"] / trips_obs["CLUSTER_AVG_SPD"]) * 3600
        trips_obs["SECS_TRVL_DSTNC"] = trips_obs["SECS_TRVL_DSTNC"].fillna(0)

        # Make A Copy Of Certain Columns, And Determine The Running Sum Of Time Traveled For Distance, Add Back To Original Time And Merge To Main DF
        cm_sum_df = trips_obs[["IDX_R", "TRIP_CLUSTER_ID", "CLUSTER_AVG_SPD", "ERROR_START_TIME", "SECS_TRVL_DSTNC"]].copy()
        cm_sum_df = cm_sum_df.dropna(subset=["TRIP_CLUSTER_ID"])
        cm_sum_df["TRV_TM_CUMSUM"] = cm_sum_df.groupby(["TRIP_CLUSTER_ID"])["SECS_TRVL_DSTNC"].cumsum()
        cm_sum_df["TRL_ARV_TM_EST"] = cm_sum_df["ERROR_START_TIME"] + cm_sum_df["TRV_TM_CUMSUM"]

        cm_sum_df.drop(columns=["CLUSTER_AVG_SPD", "ERROR_START_TIME", "SECS_TRVL_DSTNC", "TRV_TM_CUMSUM"], inplace = True)
        trips_obs.drop(columns=['CLUSTER_AVG_SPD', 'ERROR_START_TIME', 'DATA_FLG', 'SECS_TRVL_DSTNC'], inplace = True)


        # Merge Data Together
        trips_obs = trips_obs.merge(cm_sum_df, how="left", on=["IDX_R", "TRIP_CLUSTER_ID"])
        trips_obs.drop(columns=["IDX_R", "TRIP_CLUSTER_ID"], inplace = True)
        for col in ["ROUTE_ID", "V_ID"]:
            trips_obs[col] = trips_obs[col].ffill()

        # Make Note Of Type Of Data, CE = Complete Estimation
        trips_obs.loc[trips_obs["STP_ARV_TM"].isna(), "STP_ARV_TM"] = trips_obs["TRL_ARV_TM_EST"]
        trips_obs.loc[trips_obs["DATA_TYPE"] == "", "DATA_TYPE"] = "CE"

        # Remove Unneeded Columns
        trips_obs.drop(columns=["STP_ARV_DTTM", "TRL_ARV_TM_EST"], inplace = True)

        # Forward Fill Data
        trips_obs["STP_ARV_TM"] = round(trips_obs["STP_ARV_TM"], 0)
        trips_obs["STP_ARV_DTTM"] = pd.to_datetime(trips_obs["STP_ARV_TM"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')

        trips_obs.loc[trips_obs["ROUTE_ID"] == "", "ROUTE_ID"] = np.nan
        trips_obs["ROUTE_ID"] = trips_obs.groupby(["TRIP_ID"])["ROUTE_ID"].ffill()

        # Round A Few Columns
        for col in ["DTS_2_NXT_STP"]:
            trips_obs[col] = trips_obs[col].round(2)

        # Final Bits Of Formatting, Drop Duplicates, And If No Data For Trip ID Don't Keep
        trips_obs = trips_obs.drop_duplicates()
        gb = trips_obs.groupby("TRIP_ID")
        data = []
        for x in gb:

            # Get All Time Diffs As List
            td_df_lst = x[1]["STP_ARV_TM"].tolist()

            # If All Are Not NaN, And Lenght Is Larger Than 1
            if all(tm_stampt != np.nan for tm_stampt in td_df_lst) and (len(td_df_lst) >= 2):
                data.append(x[1])

        # Put Everything Together
        trips_obs = pd.concat(data)
        del trips_obs["STP_ID"]

        # Drop Duplicates Again, This Time Based On TRIP_ID, And Stop Number
        trips_obs = trips_obs.drop_duplicates(subset=["TRIP_ID", "RT_STP_NUM"])

        # If Last Stop & No Data Remove Erroneous Data From Next Stop
        trips_obs["NXT_STP_NM"] = trips_obs["NXT_STP_NM"].fillna("--")
        trips_obs.loc[trips_obs["NXT_STP_NM"] == "--", "DTS_2_NXT_STP"] = 0

        # For Logging
        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Data Formatting Step #3 - Complete")

        return trips_obs






    # ------------------------- Public Function 5 ------------------------------
    def frmt_rwbslc_data(self, td_dt_mx):
        """
        When called, this function will read the bus data collected, and exported
        from the day before, format the data - determining speed, and time when it arrived
        at a given bus stop, and keep only needed entries. The formatted data will then
        be exported as a CSV to an output folder.
        """

        # Step #0: Gather Yesterday's Bus Location Data
        try:
            td_dt_mx = "19-05-2024"

            dir_list = [x for x in os.listdir(self.out_dict["BUS_LOC"]) if ".csv" in x]
            df = pd.DataFrame(dir_list, columns=['FILE_NAME'])

            # Create A Dataframe With The Time The File Was Created & Output
            df["DATE"] = df["FILE_NAME"].str.split('_').str[-1]
            df["DATE"] = df["DATE"].str.replace(".csv", "", regex=False)
            df["DATE"] = pd.to_datetime(df["DATE"], format = self.td_s_dt_dsply_frmt)


            # We Only Need Certain Columns On Data Ingest
            td_dt_mx = datetime.strptime(td_dt_mx, self.td_s_dt_dsply_frmt)
            df = df[df["DATE"] >= td_dt_mx]
            needed_cols = ['u_id', 'timestamp', 'route_id', 'trip_id', 'vehicle_id', 'bearing', 'latitude', 'longitude', 'stop_id', 'dt_colc']
            df = pd.concat([pd.read_csv(path_, usecols = needed_cols) for path_ in [f'{self.out_dict["BUS_LOC"]}/{x}' for x in df["FILE_NAME"].tolist()]])
            del needed_cols

            # Format Data
            trips_obs = self.__frmt_data_s3(self.__frmt_data_s2(self.__frmt_data_s1(df, td_dt_mx), td_dt_mx))

            # Export Speed DF To Folder
            cleaned_dt = f"{td_dt_mx.day:0>2}-{td_dt_mx.month:0>2}-{td_dt_mx.year}"
            dt_string = datetime.now().strftime(self.td_s_dt_dsply_frmt)
            out_path = self.out_dict["FRMTD_DATA"] + f"/FRMTD_DATA_{cleaned_dt}.csv"
            trips_obs.to_csv(out_path, index=False)

        except Exception as e:
            print(e)
            pass
