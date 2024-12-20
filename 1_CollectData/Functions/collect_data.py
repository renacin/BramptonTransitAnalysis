# Name:                                            Renacin Matadeen
# Date:                                               11/02/2024
# Title                                     Main Logic Of Data Collector
#
# ----------------------------------------------------------------------------------------------------------------------
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import requests
import sqlite3
import json
import time
import sys
import re

# ----------------------------------------------------------------------------------------------------------------------


class DataCollector:
    """ This class will gather data on both bus locations as well as weather data """


    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self, skp_dwnld=False):
        """ This function will run when the DataCollector Class is instantiated """

        # Datetime Variables
        self.td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
        self.td_s_dt_dsply_frmt = "%d-%m-%Y"

        # Set Debugging Value 1 = Print LOGS, 0 = Print Nothing
        self.DEBUG_VAL = 1

        # Need Socket Library Quickly
        import socket

        # Where Is This Running On?
        if socket.gethostname() == "Renacins-MacBook-Pro.local":
            db_out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
            self.db_folder = db_out_path
            self.csv_out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
            self.db_path = db_out_path + "/DataStorage.db"
            self.rfresh_tkn_path = r"/Users/renacin/Desktop/Misc/GrabToken.sh"


        elif socket.gethostname() == "raspberrypi":
            db_out_path = r"/home/pi/Documents/Python/BramptonTransitAnalysis/3_Data"
            self.db_folder = db_out_path
            self.csv_out_path = r"/media/pi/STORAGE"
            self.db_path = db_out_path + "/DataStorage.db"
            self.rfresh_tkn_path = r"/home/pi/Desktop/GrabToken.sh"


        elif socket.gethostname() == "RenacinDesktop":
            db_out_path = r"C:\Users\renac\Documents\Programming\Python\BramptonTransitAnalysis\3_Data"
            self.db_folder = db_out_path
            self.csv_out_path = r"E:\STORAGE"
            self.db_path = db_out_path + "\DataStorage.db"
            self.rfresh_tkn_path = r"C:\Users\renac\Desktop\DropboxInfo\GrabToken.sh"


        else:
            now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
            print(f"{now}: Invalid Host Name")
            sys.exit(1)


        # No Longer Need Socket Library
        del socket


        # Quickly Need The OS Library
        import os

        if os.name == 'nt': self.slash = r"\\"
        else:               self.slash = r"/"

        del os


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



    # -------------------------- Private Function 1 ----------------------------
    def __out_folder_check(self, csv_out_path):
        """ On instantiation this function will be called. Check to see which
        operating system this script is running on, additionally check to see
        if the approrpriate folders are available to write to, if not create
        them."""

        # Only Need Library For This Step
        import os

        # First Check To See If The Main Folder Exists!
        if not os.path.isdir(self.db_folder):
            os.makedirs(self.db_folder)

        # In The Out Directory Provided See If The Appropriate Sub Folders Exist!
        for fldr_nm in ['BUS_STP', 'BUS_LOC', 'FRMTD_DATA', 'BUS_SPEED', 'GRAPHICS', 'ERROR']:
            dir_chk = f"{csv_out_path}{self.slash}{fldr_nm}"
            self.out_dict[fldr_nm] = dir_chk
            if not os.path.exists(dir_chk):
                os.makedirs(dir_chk)


        del os




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


        # For Debugging
        if self.DEBUG_VAL == 1:
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

        # We Only Need The UrlLib Request Library Here
        import urllib.request

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

                        # For Debugging
                        if self.DEBUG_VAL == 1:
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


        # For Debugging
        if self.DEBUG_VAL == 1:
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

        import numpy as np

        # Informed By Comparison, Make Changes
        parsed_df["STP_NM"] = parsed_df["STP_NM"].str.replace('&amp;', '&')

        # Get Unique Bus Stop Names From Parsed Dataframe
        unq_parsed_stps = pd.DataFrame(parsed_df["STP_NM"].unique().tolist(), columns=["Parsed_Bus_Stps"])
        unq_parsed_stps["In_OpenData"] = np.where(unq_parsed_stps["Parsed_Bus_Stps"].isin(downld_df["stop_name"]), "Y", "N")

        # Which Bus Stops Are Missing?
        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        misng_stps = unq_parsed_stps[unq_parsed_stps["In_OpenData"] == "N"]


        # For Debugging
        if self.DEBUG_VAL == 1:
            now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
            print(f"{now}: Parsed DF Len: {len(parsed_df)}, Downloaded DF Len: {len(downld_df)}, Number Of Missing Stops: {len(misng_stps)}")

        # Remove Unneeded
        del np
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


        # For Debugging
        if self.DEBUG_VAL == 1:
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
        db_path = out_path + f"{self.slash}{out_table}_{dt_nw}.csv"

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

            # For Debugging
            if self.DEBUG_VAL == 1:
                print(f"{tm_nw}: Exported CSV & DB Table - {out_table} Cleaned")

        else:
            return



    # ------------------------- Private Function 6 ------------------------------
    def __return_files_dates(self, out_path):
        """
        When called, this function will look at all the files in a folder and
        return a formatted pandas dataframe for the user to query in later functions
        """

        # We Quickly Need OS For This. Will Delete ASAP
        import os

        # Navigate To Data Folder | Get All Appropriate Files
        var_name = out_path
        out_path = self.out_dict[out_path]
        dir_list = [x for x in os.listdir(out_path) if ".csv" in x]
        df = pd.DataFrame(dir_list, columns=['FILE_NAME'])

        if var_name == "BUS_LOC":
            df["DATE"] = df["FILE_NAME"].str.split('_').str[-1]

        elif var_name == "BUS_SPEED":
            df["DATE"] = df["FILE_NAME"].str.split('_').str[3]

        else:
            raise ValueError

        # Create A Dataframe With The Time The File Was Created & Output
        df["DATE"] = df["DATE"].str.replace(".csv", "", regex=False)
        df["DATE"] = pd.to_datetime(df["DATE"], format = self.td_s_dt_dsply_frmt)

        del os
        return out_path, df



    # -------------------------- Public Function 3 -----------------------------
    def frmt_speed_data(self):
        """
        When called, this function will read 30 days worth of data from today's date.
        Using the bus data collected, it will determine the average speed for each route.
        If no data is collected this function will not run.
        """

        # For Now Import Numpy
        import numpy as np

        today_date = str((datetime.now() + timedelta(days=-30)).strftime(self.td_s_dt_dsply_frmt))

        # Find Files In Folder
        out_path, date_df = self.__return_files_dates("BUS_LOC")

        # Format File Data For Easier Manipulation
        date_df["DATE"] = date_df["DATE"].astype(str)
        date_df["DATE"] = pd.to_datetime(date_df["DATE"], format='%Y-%m-%d')
        date_df = date_df.sort_values(by="DATE")
        date_df = date_df.reset_index()
        del date_df["index"]

        # Format td_dt_mx For Easier Manipulation
        new_filter_dt = pd.to_datetime(f'''{today_date.split("-")[-1]}-{today_date.split("-")[1]}-{today_date.split("-")[0]}''', format='%Y-%m-%d')

        # Filter Data Based On Cleaned Date
        date_df = date_df[date_df["DATE"] >= new_filter_dt]

        # Check To See If There Is Data To Use Return Also If Debugging Mode On Print Issue
        if date_df.empty:
            if self.DEBUG_VAL == 1:
                print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: No Data To Determine Average Speed - Skipping Run")
            return


        # If There Is Valid Data, Combine Files Into One Dataframe
        # Read Back In "timestamp" When Ready & "timestamp": np.int32,
        needed_cols = ["route_id", "speed", "vehicle_id", "dt_colc"]
        def_d_types = {
                       "speed":                 np.float16,
                       "vehicle_id":            np.int16
                       }

        # Create Main Large Dataframe
        df = pd.concat([pd.read_csv(path_, usecols = needed_cols, dtype = def_d_types) for path_ in [f"{self.out_dict['BUS_LOC']}{self.slash}{x}" for x in date_df["FILE_NAME"].tolist()]])
        df["day_name"] = pd.to_datetime(df["dt_colc"]).dt.day_name()
        df["day_num"] = pd.to_datetime(df["dt_colc"]).dt.dayofweek
        del df["dt_colc"]

        # Determine The Average Speed For Each Bus Route Irregardless Of Day Of The Week, How Many Observations?
        avg_spd_gen = df.groupby(["route_id"], as_index=False).agg(avg_speed = ("speed", "mean"),
                                                                   std_speed = ("speed", "std"),
                                                                   var_speed = ("speed", "var"),
                                                                   num_obs   = ("speed", "count")
                                                                   )
        avg_spd_gen["day_name"] = "Average"
        avg_spd_gen["day_num"]  = 0

        # Determine The Average Speed For Each Bus Route Irregardless Of Day Of The Week, How Many Observations?
        avg_spd_day = df.groupby(["route_id", "day_name", "day_num"], as_index=False).agg(avg_speed = ("speed", "mean"),
                                                                                          std_speed = ("speed", "std"),
                                                                                          var_speed = ("speed", "var"),
                                                                                          num_obs   = ("speed", "count")
                                                                                          )
        avg_spd_df = pd.concat([avg_spd_gen, avg_spd_day])
        avg_spd_df = avg_spd_df.sort_values(["route_id", "day_num"])
        del avg_spd_df["day_num"]

        # Export Data
        out_path = self.out_dict["BUS_SPEED"]
        db_path = out_path + f"{self.slash}AVG_BUS_SPEED_{datetime.now().strftime(self.td_s_dt_dsply_frmt)}_M30DAYS.csv"
        avg_spd_df.to_csv(db_path, index=False)

        # For Debugging
        if self.DEBUG_VAL == 1:
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: Exported Bus Route Average Speed DF")

        del np



    # ---------------- Private Function #1 For Public Function 4 ---------------
    def __frmt_data_s1(self, data_f, td_dt_mx):
        """ In step 1 of cleaning the main bus data, we must first convert variables
        to an appropriate datatype (to save memory usage) and then determine the
        previous and next bus stop that a bus might have visited. """

        # Sanitize Inputs & Delete These Imports After Usage!
        import numpy as np
        df = data_f.copy()
        del data_f


        # Format Data To Ints, DT Accessor Took Too Long
        df = df.drop_duplicates(subset=['u_id'])
        df[["YEAR", "MONTH", "DAY"]] = df["dt_colc"].str[:10].str.split('-', expand=True)
        df[["HOUR", "MINUTE", "SECOND"]] = df["dt_colc"].str[11:19].str.split(':', expand=True)


        # We Only Want Data From td_dt_mx Date
        f_day = str((td_dt_mx + timedelta(days=-1)).day).zfill(2)
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
        df["C_LAT"]      = df["C_LAT"].astype(np.float32)
        df["C_LONG"]     = df["C_LONG"].astype(np.float32)
        df["NXT_STP_ID"] = df["NXT_STP_ID"].astype("Int32")
        df["EP_TIME"]    = df["EP_TIME"].astype("Int32")
        df["V_ID"]       = df["V_ID"].astype("Int16")
        df["DIR"]        = df["DIR"].astype("Int16")
        df["TRIP_ID"]    = df["TRIP_ID"].astype("category")
        df["ROUTE_ID"]   = df["ROUTE_ID"].astype("category")
        df["HOUR"]       = df["HOUR"].astype("category")


        # We Need To Determine Average DIR For Each Trip
        avg_dir = df[["TRIP_ID", "DIR"]].copy()
        avg_dir = avg_dir.groupby(["TRIP_ID"], as_index=False).agg(AVG_DIR = ("DIR", "mean"))
        avg_dir["AVG_DIR"] = avg_dir["AVG_DIR"].astype(int)
        df = df.merge(avg_dir, how="left", on=["TRIP_ID"])
        df.sort_values(["TRIP_ID", "EP_TIME"], inplace=True)


        # Reduce Memory Usage Of New Fields Created
        df["AVG_DIR"] = df["AVG_DIR"].astype("Int16")
        del avg_dir

        # For Each Stop Entry, We Need To Know The Previous Lat, Long, And STP_ID,
        for col in [("P_LAT", "C_LAT"), ("P_LONG", "C_LONG"), ("PRV_STP_ID", "NXT_STP_ID")]:
            df[col[0]] = df.groupby(['ROUTE_ID', 'TRIP_ID', 'AVG_DIR'])[col[1]].shift(1)
            df[col[0]].fillna(df[col[1]], inplace=True)


        # Find Bus Stop Data
        import os
        for file in os.listdir(self.out_dict["BUS_STP"]):
            if "BUS_STP_DATA" in file:
                file_path = f'{self.out_dict["BUS_STP"]}{self.slash}{file}'
        del os


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

        # Final Bits Of Formatting
        df.sort_values(["ROUTE_ID", "TRIP_ID", "EP_TIME"], inplace=True)
        df.drop_duplicates(inplace=True)

        # For Logging
        if self.DEBUG_VAL == 1:
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: Data Formatting Step #1 - Complete")

        # Clean Up & Return Dataframe
        del np
        return df




    # ---------------- Private Function #2 For Public Function 5 ---------------
    def __frmt_data_s2(self, data_f, td_dt_mx, avg_rt_speed):
        """ In step 2 of cleaning the main bus data, remove unneccesary rows within a
        trip, we only need the row closest to a given bus stop in a trip - this reduces
        the amount of data recording idling time. Once complete we then calculate the
        speed of the bus, given only the observations from that trip (no inferences). Once
        we have the speed of the bus, we determine the arrival time to the next bus stop. """

        # Sanitize Inputs & Delete These Imports After Usage!
        import numpy as np
        data_pull = data_f.copy()
        spd_pull  = avg_rt_speed.copy()
        del data_f, avg_rt_speed

        # Define The Haversine Function That Will Calculate Distance Between Locations
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


        # ----------------------------------------------------------------------

        # Remove Entries Where Bus Is Idling, Or Has Kept Transponder Running After The First Occurence At The Last Stop | Append All Dta To New Dataframe
        gb = data_pull.groupby("U_NAME")
        transit_df = pd.concat([x[1].loc[x[1]["NXT_STP_NAME"].where(x[1]["NXT_STP_NAME"]==x[1]["NXT_STP_NAME"].iloc[0]).last_valid_index():x[1]["PRV_STP_NAME"].where(x[1]["PRV_STP_NAME"]==x[1]["PRV_STP_NAME"].iloc[-1]).first_valid_index()] for x in gb])
        del data_pull, gb


        # Calculate Distance Between Current Location & Previous Location | Create A Dataframe Elaborating Distance Traveled & Speed
        transit_df["DST_BTW_LOCS"] = vec_haversine((transit_df["P_LAT"].values, transit_df["P_LONG"].values), (transit_df["C_LAT"].values, transit_df["C_LONG"].values))

        # First Create A Copy Of Main Data Table | We Only Need Certain Columns, Not All!
        speed_df = transit_df[['U_NAME', 'TRIP_ID', 'ROUTE_ID', 'V_ID', 'AVG_DIR', 'EP_TIME', 'HOUR', 'DST_BTW_LOCS']].copy()

        # Find The Previous Travel Time, Determine The Trip Speed & Trip Duration
        speed_df["P_EP_TIME"]  = speed_df.groupby(["U_NAME"])["EP_TIME"].shift(+1)
        speed_df.dropna(subset = ["P_EP_TIME"], inplace = True)
        speed_df["TRIP_DUR"]   = (speed_df["EP_TIME"] - speed_df["P_EP_TIME"]) / 3600
        speed_df["TRIP_SPD"]   = speed_df["DST_BTW_LOCS"] / speed_df["TRIP_DUR"]

        # Round & Convert Data Types To Save Memory?
        speed_df["TRIP_DUR"]   = speed_df["TRIP_DUR"].astype(np.float16)
        speed_df["TRIP_SPD"]   = speed_df["TRIP_SPD"].astype(np.float16)


        # Create A Data Frame Elaborating The Average Speed For A Trip, This May Be Useful In The Future Keep It!
        for col in ["ROUTE_ID", "TRIP_ID", "HOUR"]:
            speed_df[col] = speed_df[col].astype("object")
        speed_df = speed_df.groupby(["ROUTE_ID", "TRIP_ID", "AVG_DIR", "V_ID"], as_index=False).agg(TRIP_SPD = ("TRIP_SPD", "mean" ), HOUR = ("HOUR" , "first"))

        # The GroupBy Drops Data Types For Some Reason, Convert Back To Appropriate Datatype
        speed_df["TRIP_ID"]  = speed_df["TRIP_ID"].astype("category")
        speed_df["ROUTE_ID"] = speed_df["ROUTE_ID"].astype("category")
        speed_df["AVG_DIR"]  = speed_df["AVG_DIR"].astype("Int16")
        speed_df["V_ID"]     = speed_df["V_ID"].astype("Int16")


        # UPDATE: 2024-12-18, Instead Of Calculating Speed, Just Use The Average Speed For A Route, Maybe That's More Accurate?
        # del speed_df["TRIP_SPD"]
        speed_df = speed_df.merge(spd_pull, how="left", left_on=["ROUTE_ID"], right_on=["route_id"]).rename(columns={'avg_speed': 'AVG_RT_SPD'})
        speed_df["AVG_RT_SPD"] = speed_df["AVG_RT_SPD"].astype(np.float16)
        speed_df["AVG_RT_SPD"] = speed_df["AVG_RT_SPD"].fillna(6.5)
        del speed_df["route_id"]


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

        # Calculate Speed
        transit_df["SEC_2_NXT_STP"]  = (transit_df["DTS_2_NXT_STP"] / transit_df["AVG_RT_SPD"]) * 3600
        transit_df["NXT_STP_ARV_TM"] = transit_df["EP_TIME"] + transit_df["SEC_2_NXT_STP"]
        transit_df["NXT_STP_ARV_TM"] = transit_df["NXT_STP_ARV_TM"].astype(dtype = int, errors = 'ignore')
        transit_df["NXT_STP_ARV_DTTM"] = pd.to_datetime(transit_df["NXT_STP_ARV_TM"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')

        # Remove Zeros From NXT_STP_ARV_TM
        transit_df = transit_df[transit_df["NXT_STP_ARV_TM"] != 0]

        # For Logging
        if self.DEBUG_VAL == 1:
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: Data Formatting Step #2 - Complete")

        del np
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
        import numpy as np
        np.seterr(divide='ignore', invalid='ignore')
        import os
        transit_df = data_f.copy()
        del data_f

        # Define The Haversine Function That Will Calculate Distance Between Locations
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


        # ----------------------------------------------------------------------

        # We Only Need Certain Bits Of Data, Also For The Data That We Have Mark It As IE (Informed Estimation)
        transit_df["DATA_TYPE"] = "IE"
        transit_df = transit_df[['TRIP_ID', 'ROUTE_ID', 'V_ID', 'NXT_STP_NAME', 'NXT_STP_ARV_TM',
                                 'NXT_STP_ARV_DTTM', 'NXT_STP_ID', 'DATA_TYPE']].rename(columns={'NXT_STP_NAME': "STP_NM",
                                                                                                 'NXT_STP_ARV_TM': "STP_ARV_TM",
                                                                                                 'NXT_STP_ARV_DTTM': "STP_ARV_DTTM",
                                                                                                 'NXT_STP_ID': "STP_ID"})


        # We Need To Join The Trip Data To The Bus Stops (In Order) For A Given Route
        trips_obs          = transit_df.groupby(["TRIP_ID"], as_index=False).agg(TRIP_ID = ("TRIP_ID", "first"), RT_ID = ("ROUTE_ID", "first"))
        trips_obs["RT_ID"] = trips_obs["RT_ID"].str.split("-").str[0]
        trips_obs["RT_ID"] = trips_obs["RT_ID"].astype(dtype = int, errors = 'ignore')
        trips_obs["RT_ID"] = trips_obs["RT_ID"].astype("Int16")


        # Read In Routes Data, Only Look At Needed Data, Convert To Smaller Data Type
        for file in os.listdir(self.out_dict["BUS_STP"]):
            if "BUS_RTE_DATA" in file:
                stp_file_path = f'{self.out_dict["BUS_STP"]}{self.slash}{file}'


        # Before Using Bus Routes, We Need To Make Sure The Data Contains No Duplicates As Identified In Previous Attemps
        bus_routes = pd.read_csv(stp_file_path, usecols = ['RT_ID', 'RT_NAME', 'RT_VER', 'STP_NM', 'RT_ID_VER', 'RT_DIR', 'RT_GRP', 'RT_GRP_NUM'])
        bus_routes["U_ID"] = bus_routes["RT_ID"].astype(str) + "_" + bus_routes["RT_NAME"].astype(str) + "_" + bus_routes["RT_VER"].astype(str)
        bus_routes = bus_routes.drop_duplicates()


        # Recreate Stop Num & Total Number Of Stops
        bus_routes["RT_STP_NUM"] = bus_routes.groupby(["U_ID"]).cumcount() + 1
        num_stps_df = bus_routes.groupby("U_ID", as_index=False).agg(RT_NUM_STPS = ("U_ID", "count"))
        bus_routes = bus_routes.merge(num_stps_df, on='U_ID', how='left')
        del num_stps_df, bus_routes["U_ID"]


        # Only Keep Data That Is In Scope & Try To Reduce Memory Usage
        bus_routes = bus_routes[bus_routes["RT_ID"].isin(trips_obs["RT_ID"])]
        bus_routes["RT_ID"]       = bus_routes["RT_ID"].astype("Int16")
        bus_routes["RT_VER"]      = bus_routes["RT_VER"].astype("Int16")
        bus_routes["RT_STP_NUM"]  = bus_routes["RT_STP_NUM"].astype("Int16")
        bus_routes["RT_NUM_STPS"] = bus_routes["RT_NUM_STPS"].astype("Int16")
        bus_routes["RT_GRP_NUM"]  = bus_routes["RT_GRP_NUM"].astype("Int16")


        # Left Join Bus Route Data Onto Unique Trip Data Using RT_ID As A Key
        trips_obs = trips_obs.merge(bus_routes, how="left", on=["RT_ID"])
        trips_obs = trips_obs.merge(transit_df, how="left", on=["TRIP_ID", "STP_NM"])
        del transit_df, bus_routes


        # Create A Column That Identifies The Trip ID, RT_ID, And RT_VER, Remove U_IDs That Have No Data In Them
        trips_obs["U_ID"]                        = trips_obs["TRIP_ID"].astype(str) + "_" + trips_obs["RT_ID"].astype(str) + "_" + trips_obs["RT_VER"].astype(str)
        count_df                                 = trips_obs.groupby(["U_ID"], as_index=False).agg(U_ID  = ("U_ID", "first"), COUNT = ("DATA_TYPE", "count"))
        count_df[["TRIP_ID", "RT_ID", "RT_VER"]] = count_df["U_ID"].str.split('_', expand=True)
        count_df["COUNT_NAME"]                   = count_df["TRIP_ID"].astype(str) + "_" + count_df["COUNT"].astype(str)


        # Only Keep Trips That Actually Happened, Remove Eroneous Data
        max_obs               = count_df.groupby(["TRIP_ID", "RT_ID"], as_index=False).agg(COUNT = ("COUNT", "max"))
        max_obs["COUNT_NAME"] = max_obs["TRIP_ID"].astype(str) + "_" + max_obs["COUNT"].astype(str)
        max_obs.drop(["TRIP_ID", "RT_ID", "COUNT"], axis=1, inplace=True)


        # Merge Back To Count DF, Make Sure We Are Looking At Correct U_IDs
        count_df = max_obs.merge(count_df, how="left", on=["COUNT_NAME"])
        trips_obs = trips_obs[trips_obs["U_ID"].isin(count_df["U_ID"])]
        del max_obs, count_df


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
                file_path = f'{self.out_dict["BUS_STP"]}{self.slash}{file}'

        # Read In Bus Loc Data & Merge To Trips Obs DF
        bus_locs = pd.read_csv(file_path, usecols = ['stop_id', 'stop_name', 'CLEANED_STOP_LAT_', 'CLEANED_STOP_LON_'])
        bus_locs.rename(columns = {"stop_name"        : "STP_NM",
                                   "stop_id"          : "STP_ID",
                                   "CLEANED_STOP_LAT_": "STP_LAT",
                                   "CLEANED_STOP_LON_": "STP_LON"}
                                   , inplace = True)

        # Split Methodolgies To Merge Data. If We Have Stop ID, Join On Stop ID, If We Don't Have Stop ID, Join On Name, But Keep The First One
        stp_id_merge = trips_obs[trips_obs["DATA_TYPE"] == "IE"]
        stp_id_merge = stp_id_merge.merge(bus_locs[["STP_ID", "STP_LAT", "STP_LON"]].copy(), how="left", on=["STP_ID"])

        stp_nm_merge = trips_obs[trips_obs["DATA_TYPE"] != "IE"]
        stp_nm_merge = stp_nm_merge.merge(bus_locs[["STP_NM", "STP_LAT", "STP_LON"]].copy(), how="left", on=["STP_NM"])
        stp_nm_merge = stp_nm_merge.drop_duplicates(subset=['TRIP_ID', 'RT_STP_NUM'])

        # Merge Data Together
        trips_obs = pd.concat([stp_id_merge, stp_nm_merge])
        trips_obs = trips_obs.sort_values("TEMP_IDX")
        del bus_locs, stp_id_merge, stp_nm_merge, trips_obs["TEMP_IDX"]


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
        cm_sum_df["TRV_TM_CUMSUM"]  = cm_sum_df.groupby(["TRIP_CLUSTER_ID"])["SECS_TRVL_DSTNC"].cumsum()
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
        if self.DEBUG_VAL == 1:
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: Data Formatting Step #3 - Complete")


        del np, os
        return trips_obs



    # -------------------------- Public Function 4 -----------------------------
    def frmt_bus_data(self):
        """
        When called, this function will use the most recent bus location data, and
        the most speed data last derived, to determine the arrival time in between
        recorded bus locations. If no data is collected this function will not run.
        """

        # We Need The Most Recent Bus Speed Data (If Less Than X Days Old & Yesterday's Bus Locations)
        # PLEASE CHANGE BACK TO 0 WHEN IN PRODUCTION!
        day_before = str((datetime.now() + timedelta(days=-0)).strftime(self.td_s_dt_dsply_frmt))
        day_before = datetime.strptime(day_before,'%d-%m-%Y')

        # Find Files In Both Bus Loc & Bus Speed Folders
        bus_loc_out_path, bus_loc_date_df = self.__return_files_dates("BUS_LOC")
        bus_spd_out_path, bus_spd_date_df = self.__return_files_dates("BUS_SPEED")

        # Format Date Column For Each Dataframe
        bus_loc_date_df["DATE"] = bus_loc_date_df["DATE"].astype(str)
        bus_loc_date_df["DATE"] = pd.to_datetime(bus_loc_date_df["DATE"], format='%Y-%m-%d')
        bus_loc_date_df = bus_loc_date_df[bus_loc_date_df["DATE"] == f"{day_before.year}-{day_before.month}-{day_before.day}"]

        bus_spd_date_df["DATE"] = bus_spd_date_df["DATE"].astype(str)
        bus_spd_date_df["DATE"] = pd.to_datetime(bus_spd_date_df["DATE"], format='%Y-%m-%d')
        bus_spd_date_df = bus_spd_date_df[bus_spd_date_df["DATE"] == f"{day_before.year}-{day_before.month}-{day_before.day}"]


        # If A Bus Location & Speed File Wasn't Generated The Day Before Skip Today's Data Formatting
        if len(bus_spd_date_df) == 0 | len(bus_loc_date_df) == 0:
            if self.DEBUG_VAL == 1:
                print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: No Data To Format Bus Locations")
                return


        # Else, Start Performing The Bus Location Data Formatting, First Grab Needed Files
        bus_loc_name = bus_loc_date_df["FILE_NAME"].iloc[0]
        bus_spd_name = bus_spd_date_df["FILE_NAME"].iloc[0]

        # Get Rid Of Unneeded Files
        del bus_spd_date_df, bus_loc_date_df

        # Read Needed Files As Pandas DFs, Be Mindful Of Slashes For Windows & *NIX
        bus_loc_df = pd.read_csv(f"{bus_loc_out_path}{self.slash}{bus_loc_name}")
        bus_spd_df = pd.read_csv(f"{bus_spd_out_path}{self.slash}{bus_spd_name}")


        # There Is A Min Threshold For Average Speed Currently Need At Least 50'000 Data Points
        bus_spd_df = bus_spd_df[bus_spd_df["num_obs"] >= 50_000]
        bus_spd_df = bus_spd_df[["route_id", "avg_speed"]]

        # Format Data & Export Speed DF To Folder
        try:

            trips_obs = self.__frmt_data_s3(self.__frmt_data_s2(self.__frmt_data_s1(bus_loc_df, day_before), day_before, bus_spd_df))
            cleaned_dt = f"{day_before.day:0>2}-{day_before.month:0>2}-{day_before.year}"
            out_path = self.out_dict["FRMTD_DATA"] + f"{self.slash}FRMTD_DATA_{cleaned_dt}.csv"
            trips_obs.to_csv(out_path, index=False)

        except ValueError:
            pass


        # For Logging
        if self.DEBUG_VAL == 1:
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: Completed Data Formatting")






# ----------------------------------------------------------------------------------------------------------------------
def data_viz_3(graphics_path, fmted_path, f_af, bus_stp_path, bstp_af, e_out_path, e_fl_data, td_dt_mx, num_days):
    """
    Using the formatted data, this function will derive general statistics about
    when buses arrived at certain bus stops on a given route. And if any variation
    is present between routes.
    """
    graphics_path
    fmted_path
    f_af
    bus_stp_path
    bstp_af
    e_out_path
    e_fl_data
    td_dt_mx
    num_days


    #             bus_loc_path, b_af    =  Collector.return_files_dates("BUS_LOC")
    #             bus_stp_path, bstp_af =  Collector.return_files_dates("BUS_STP")
    #             fmted_path, f_af      =  Collector.return_files_dates("FRMTD_DATA")
    #             error_path, e_af      =  Collector.return_files_dates("ERROR")
    #             graphics_path, g_af   =  Collector.return_files_dates("GRAPHICS")


    #             data_viz_3(graphics_path,
    #                         fmted_path,
    #                         f_af,
    #                         bus_stp_path,
    #                         bstp_af,
    #                         error_path,
    #                         e_af,
    #                         str((datetime.datetime.now() + datetime.timedelta(days=-10)).strftime(td_s_dt_dsply_frmt)))
    #


    
    #
    # import matplotlib as plt
    #
    # # Format File Data For Easier Manipulation
    # f_af["DATE"] = f_af["DATE"].astype(str)
    # f_af["DATE"] = pd.to_datetime(f_af["DATE"], format='%Y-%m-%d')
    # f_af = f_af.sort_values(by="DATE")
    # f_af = f_af.reset_index()
    # del f_af["index"]
    #
    # # Format td_dt_mx For Easier Manipulation
    # new_filter_dt = pd.to_datetime(f'''{td_dt_mx.split("-")[-1]}-{td_dt_mx.split("-")[1]}-{td_dt_mx.split("-")[0]}''', format='%Y-%m-%d')
    #
    # # Filter Data Based On Cleaned Date
    # f_af = f_af[f_af["DATE"] >= new_filter_dt]
    #
    # # If Number Of Files Smaller Than Number Of Days Looking Back, Raise An Error
    # if len(f_af) >= num_days:
    #     print("True")
    #
    # # Combine Files Into One
    # df = pd.concat([pd.read_csv(path_) for path_ in [f"{fmted_path}/{x}" for x in f_af["FILE_NAME"].tolist()]])
    # del f_af, df["index"], df["RT_GRP_NUM"], df["RT_GRP"]
    #
    #
    # # Create A Lagged Column, So We Can See The Next Arrival Time, & Determine The Time Between A Segment
    # df['NXT_STP_ARV_TM'] = df.groupby(['TRIP_ID'])['STP_ARV_TM'].shift(-1)
    # df['NXT_DATA_TYPE'] = df.groupby(['TRIP_ID'])['DATA_TYPE'].shift(-1)
    # df['TM_DIFF'] = df['NXT_STP_ARV_TM'] - df['STP_ARV_TM']
    # df["SEG_NAME"] = df['STP_NM'] + " To " + df['NXT_STP_NM']
    # df["SEG_DATA_TYPE"] = df['DATA_TYPE'] + " To " + df['NXT_DATA_TYPE']
    #
    # # Read In Bus Routes Data, Create A Matching Segment Name Dataframe
    # for file in os.listdir(bus_stp_path):
    #     if "BUS_RTE_DATA" in file:
    #         stp_file_path = f'{bus_stp_path}{self.slash}{file}'
    #
    # needed_cols = ['RT_ID', 'RT_NAME', 'RT_VER', 'STP_NM', 'RT_ID_VER', 'RT_DIR', 'RT_GRP', 'RT_GRP_NUM']
    # bus_routes = pd.read_csv(stp_file_path, usecols = needed_cols)
    #
    # # Before Using Bus Routes, We Need To Make Sure The Data Contains No Duplicates As Identified In Previous Attemps
    # bus_routes["U_ID"] = bus_routes["RT_ID"].astype(str) + "_" + bus_routes["RT_NAME"].astype(str) + "_" + bus_routes["RT_VER"].astype(str)
    # bus_routes = bus_routes.drop_duplicates()
    #
    # # Recreate Stop Num & Total Number Of Stops
    # bus_routes["RT_STP_NUM"] = bus_routes.groupby(["U_ID"]).cumcount() + 1
    # num_stps_df = bus_routes.groupby("U_ID", as_index=False).agg(RT_NUM_STPS = ("U_ID", "count"))
    # bus_routes = bus_routes.merge(num_stps_df, on='U_ID', how='left')
    # del num_stps_df, bus_routes["U_ID"]
    #
    #
    # # Only Keep Data That Is In Scope
    # bus_routes = bus_routes[bus_routes["RT_ID"].isin(df["RT_ID"])]
    #
    #
    # # We Need To Join The Trip Data To The Bus Stops (In Order) For A Given Route
    # trips_obs = df.groupby(["TRIP_ID"], as_index=False).agg(TRIP_ID = ("TRIP_ID", "first"),
    #                                                         RT_ID   = ("ROUTE_ID", "first")
    #                                                         )
    #
    # trips_obs["RT_ID"] = trips_obs["RT_ID"].str.split("-").str[0]
    # trips_obs["RT_ID"] = trips_obs["RT_ID"].astype(dtype = int, errors = 'ignore')
    # trips_obs["RT_ID"] = trips_obs["RT_ID"].astype("Int16")
    #
    #
    # # In Order To Standardize Comparisons We Need Each Trip To Have An Entire Set Of Segment IDs, Even If It Only Had A Couple Of Stops
    # trips_obs = trips_obs.merge(bus_routes, how="left", on=["RT_ID"])
    # trips_obs = trips_obs.merge(df, how="left", on=["TRIP_ID", "STP_NM", "RT_ID",
    #                                                 "RT_NAME", "RT_VER", "RT_ID_VER",
    #                                                 "RT_DIR", "RT_STP_NUM", "RT_NUM_STPS"])
    # del df
    #
    #
    # # We Only Want Trip IDs With Data, Remove Those That Don't
    # trips_obs["U_ID"] = trips_obs["TRIP_ID"] + "_" + trips_obs["RT_ID_VER"]
    # count_df = trips_obs.groupby(["U_ID"], as_index=False).agg(TM_SUM = ("TM_DIFF", "sum"))
    # count_df = count_df[count_df["TM_SUM"] > 0]
    # trips_obs = trips_obs[trips_obs["U_ID"].isin(count_df["U_ID"])]
    # del count_df, trips_obs["U_ID"]
    #
    #
    # # For Now Find The Route With The Most Trips | It Varies From Day To Day
    # max_route = trips_obs.copy()
    # max_route = max_route.dropna()
    # max_route = max_route.groupby(["RT_ID_VER"], as_index=False).agg(RT_ID_VER_COUNT = ("RT_ID_VER", "count"))
    # max_route = max_route.sort_values(['RT_ID_VER_COUNT'], ascending=[False])
    #
    # max_route = max_route.head(3)
    # max_routes = max_route["RT_ID_VER"].to_list()
    #
    #
    # # Iterate Through Each Max Route:
    # for max_route in max_routes:
    #     # Focus On Just Route Data
    #     u_id_data = trips_obs[trips_obs["RT_ID_VER"] == max_route].copy()
    #
    #
    #     # We Need The Bus Stops In A Given Route
    #     trip_details = bus_routes[bus_routes["RT_ID_VER"] == max_route].copy()
    #     trip_details['NXT_STP_NAME'] = trip_details.groupby(['RT_ID_VER'])['STP_NM'].shift(-1)
    #     trip_details["SEG_NAME"] = trip_details['STP_NM'] + " To " + trip_details['NXT_STP_NAME']
    #     trip_details.dropna(inplace=True)
    #     del trip_details["NXT_STP_NAME"]
    #
    #
    #     # Looking At The Entire Database, Grab Data Where The Segment Name Is In The List Of Unique Segment IDs For The Given Trip
    #     route_data = trips_obs[trips_obs["SEG_NAME"].isin(trip_details["SEG_NAME"].unique())].copy()
    #
    #     # Group Data, Find Average Time Between Segments, And The Variance Between Them
    #     needed_cols = ["RT_ID", "RT_NAME", "RT_VER", "RT_DIR", "RT_STP_NUM", "RT_NUM_STPS", "V_ID", "STP_ARV_TM", "DATA_TYPE", "STP_ARV_DTTM", "TM_DIFF", "SEG_NAME", "SEG_DATA_TYPE", "DTS_2_NXT_STP"]
    #     route_data = route_data[needed_cols].copy()
    #
    #
    #     # Sort By TIME DIFF, Should Be Relatively The Same, Values Should Be Greater Than 0 Obviously, And Smaller Than Mean + 4 * STD
    #     max_val = (route_data["TM_DIFF"].mean()) + (route_data["TM_DIFF"].std() * 3)
    #     route_data = route_data[(route_data["TM_DIFF"] > 0) & (route_data["TM_DIFF"] < max_val)]
    #
    #     # route_data = route_data[route_data["SEG_DATA_TYPE"].isin(["IE To IE"])]
    #
    #     # Create Additional Columns
    #     route_data["STP_ARV_DTTM"] = pd.to_datetime(route_data["STP_ARV_DTTM"])
    #     route_data["WEEK_DAY"] = route_data["STP_ARV_DTTM"].dt.day_name()
    #     route_data["HOUR"] = route_data["STP_ARV_DTTM"].dt.hour
    #     route_data["MINUTE"] = route_data["STP_ARV_DTTM"].dt.minute
    #
    #     # Remove Columns We Don't Need, And Change The Data In Others So It's More Accurate For This Given Route
    #     merge_seg_num = trip_details[["SEG_NAME", "RT_STP_NUM"]].copy()
    #     route_data = route_data.drop(["RT_ID", "RT_NAME", "RT_VER", "RT_DIR", "RT_STP_NUM", "RT_NUM_STPS", "V_ID", "DATA_TYPE", "SEG_DATA_TYPE", "DTS_2_NXT_STP", "STP_ARV_TM", "STP_ARV_DTTM"], axis=1)
    #     route_data = route_data.merge(merge_seg_num, on='SEG_NAME', how='left')
    #
    #
    #     # Create Time Segments
    #     route_data['TIME_SEGMT'] = ""
    #     route_data.loc[((route_data['HOUR'] >= 23) | (route_data['HOUR'] <= 5)),  'TIME_SEGMT']  = 'Off Peak (11PM - 5AM)'
    #     route_data.loc[((route_data['HOUR'] >= 6)  & (route_data['HOUR'] <= 22)), 'TIME_SEGMT']  = 'On Peak  (6AM - 10PM)'
    #
    #
    #     # Does The Average Time Per Segment Change Every Hour?
    #     route_data_stats = route_data.groupby(["SEG_NAME", "TIME_SEGMT"], as_index=False).agg(RT_STP_NUM         = ("RT_STP_NUM", "first"),
    #                                                                                           OBS_COUNT          = ("TM_DIFF", "count"),
    #                                                                                           TIME_BTW_SEG_AVG   = ("TM_DIFF", "mean"),
    #                                                                                           TIME_BTW_SEG_STD   = ("TM_DIFF", "std")
    #                                                                                           )
    #
    #     route_data_stats = route_data_stats.sort_values(by=["TIME_SEGMT", "RT_STP_NUM"])
    #
    #     # We Need A Lot Of Data Before Doing An Analysis
    #     route_data_stats = route_data_stats[route_data_stats["OBS_COUNT"] > 3]
    #
    #     for segment in route_data_stats["TIME_SEGMT"].unique().tolist():
    #
    #         # Colour For This Sample
    #         clr_ = np.random.rand(3, )
    #
    #         # Plot Sample Data
    #         sample_data = route_data_stats[route_data_stats["TIME_SEGMT"] == segment]
    #         plt.scatter(sample_data["RT_STP_NUM"], sample_data["TIME_BTW_SEG_STD"], c = clr_, marker="x", alpha=0.3)
    #
    #         # Plot A Curve Of Best Fit
    #         curve = np.polyfit(sample_data["RT_STP_NUM"], sample_data["TIME_BTW_SEG_STD"], 5)
    #         poly = np.poly1d(curve)
    #         yy = poly(sample_data["RT_STP_NUM"])
    #         plt.plot(sample_data["RT_STP_NUM"], yy, c = clr_, alpha=0.8, label=f"Best Fit: {segment}")
    #
    #     # Print Segment Names
    #     sample_data = sample_data[["RT_STP_NUM", "SEG_NAME"]]
    #     u_names = sample_data.drop_duplicates()
    #     print(u_names)
    #
    #     # To show the plot
    #     plt.title(f"STD Of Segment Route Times: {max_route}")
    #     plt.legend()
    #     plt.show()
    #



        #
