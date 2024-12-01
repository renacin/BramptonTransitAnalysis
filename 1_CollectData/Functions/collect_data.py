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

            # # Delete The SQL Table, See If That Helps Drop The Database Size?
            # conn = sqlite3.connect(self.db_path)
            # conn.execute(f"""DROP TABLE IF EXISTS {out_table}""")
            # conn.commit()
            # conn.close()
            #
            # # Write Over DB Table So It's Now Empty
            # conn = sqlite3.connect(self.db_path)
            # empty_df.to_sql(f"{out_table}", conn, if_exists="replace", index=False)
            # conn.commit()
            # conn.close()
            #
            # # We Need To Make Sure We Clean Up Everything, Run The Vacuum Command To Clean Up Temp Space
            # conn = sqlite3.connect(self.db_path)
            # conn.execute(f"""vacuum""")
            # conn.commit()
            # conn.close()

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
        out_path = self.out_dict[out_path]
        dir_list = [x for x in os.listdir(out_path) if ".csv" in x]
        df = pd.DataFrame(dir_list, columns=['FILE_NAME'])

        # Create A Dataframe With The Time The File Was Created & Output
        df["DATE"] = df["FILE_NAME"].str.split('_').str[-1]
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
        , today_date
        """

        # For Now Import Numpy
        import numpy as np

        # This Is A Temporary Step, We Need To Iterate Through Each Day Of Data Collected For Bus Locs, Begining - 30 Days To Begining, Then Begining +1 - 30 Days To Begining +1, Etc ...
        # We Need To Determine The Averages As If We Were Collecting More And More Data Each Day For Data Vizualization Purposes. This Will Be Deleted

        # Find Files In Folder
        out_path, date_df = self.__return_files_dates("BUS_LOC")

        # Determine The Oldest Data In The Data Collected & Determine 30 Days Before
        date_df["DATE"] = date_df["DATE"].astype(str)
        date_df["DATE"] = pd.to_datetime(date_df["DATE"], format='%Y-%m-%d')
        date_df["30DAYS_BEF"] = date_df["DATE"] + pd.Timedelta(days = -30)

        # Read All Bus Loc Data Into One Giant Pandas Dataframe
        import glob
        import os

        path = r'E:\STORAGE\BUS_LOC'

        # Get A List Of All Files In Storage Folder
        frames = []
        for f_nm in date_df["FILE_NAME"].tolist():
            df = pd.read_csv(f"{path}\{f_nm}")
            frames.append(df)

        main_df = pd.concat(frames)
        main_df = main_df[["route_id", "speed", "vehicle_id", "dt_colc"]]

        # Starting With The First Date When Data Was Collected Make A New CSV With Average, Varaince, Standard Deviation, Etc...
        # Get A Cut Of The Data For Observations
        data_cut = []
        for dt_, dt_m30 in zip(date_df["DATE"].tolist(), date_df["30DAYS_BEF"].tolist()):

            # Filter Data To Time Range
            df = main_df.copy()
            df = df[(df["dt_colc"] <= str(dt_)   ) &
                    (df["dt_colc"] >= str(dt_m30))
                    ]

            # Create Main Large Dataframe
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
            avg_spd_df = avg_spd_df.round(2)
            del avg_spd_df["day_num"]

            # Append Data Cut
            data_cut.append(avg_spd_df[(avg_spd_df["route_id"] == "501-349") &
                                       (avg_spd_df["day_name"] == "Average")
                                       ])

            # Export Data
            out_path = self.out_dict["BUS_SPEED"]

            dt_list = str(dt_).split(" ")
            dt_list = dt_list[0].split("-")
            dt_str  = f"{dt_list[2]}-{dt_list[1]}-{dt_list[0]}"

            db_path = out_path + f"{self.slash}AVG_BUS_SPEED_{dt_str}_M30DAYS.csv"
            avg_spd_df.to_csv(db_path, index=False)

            # For Testing
            print(f"Completed: {dt_}")

        data_cut = pd.concat(data_cut)
        data_cut.to_csv("Testing.csv", index=False)

        # Route: 501-349 (Overall - With Stops) Number Of Observations Vs Average Speed














        #
        # today_date = str((datetime.now() + timedelta(days=-30)).strftime(self.td_s_dt_dsply_frmt))
        #
        # # Find Files In Folder
        # out_path, date_df = self.__return_files_dates("BUS_LOC")
        #
        # # Format File Data For Easier Manipulation
        # date_df["DATE"] = date_df["DATE"].astype(str)
        # date_df["DATE"] = pd.to_datetime(date_df["DATE"], format='%Y-%m-%d')
        # date_df = date_df.sort_values(by="DATE")
        # date_df = date_df.reset_index()
        # del date_df["index"]
        #
        # # Format td_dt_mx For Easier Manipulation
        # new_filter_dt = pd.to_datetime(f'''{today_date.split("-")[-1]}-{today_date.split("-")[1]}-{today_date.split("-")[0]}''', format='%Y-%m-%d')
        #
        # # Filter Data Based On Cleaned Date
        # date_df = date_df[date_df["DATE"] >= new_filter_dt]
        #
        # # Check To See If There Is Data To Use Return Also If Debugging Mode On Print Issue
        # if date_df.empty:
        #     if self.DEBUG_VAL == 1:
        #         print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: No Data To Determine Average Speed - Skipping Run")
        #     return
        #
        #
        # # If There Is Valid Data, Combine Files Into One Dataframe
        # # Read Back In "timestamp" When Ready & "timestamp": np.int32,
        # needed_cols = ["route_id", "speed", "vehicle_id", "dt_colc"]
        # def_d_types = {
        #                "speed":                 np.float16,
        #                "vehicle_id":            np.int16
        #                }
        #
        # # Create Main Large Dataframe
        # df = pd.concat([pd.read_csv(path_, usecols = needed_cols, dtype = def_d_types) for path_ in [f"{self.out_dict['BUS_LOC']}{self.slash}{x}" for x in date_df["FILE_NAME"].tolist()]])
        # df["day_name"] = pd.to_datetime(df["dt_colc"]).dt.day_name()
        # df["day_num"] = pd.to_datetime(df["dt_colc"]).dt.dayofweek
        # del df["dt_colc"]
        #
        # # Determine The Average Speed For Each Bus Route Irregardless Of Day Of The Week, How Many Observations?
        # avg_spd_gen = df.groupby(["route_id"], as_index=False).agg(avg_speed = ("speed", "mean"),
        #                                                            num_obs   = ("speed", "count")
        #                                                            )
        # avg_spd_gen["day_name"] = "Average"
        # avg_spd_gen["day_num"]  = 0
        #
        # # Determine The Average Speed For Each Bus Route Irregardless Of Day Of The Week, How Many Observations?
        # avg_spd_day = df.groupby(["route_id", "day_name", "day_num"], as_index=False).agg(avg_speed = ("speed", "mean"),
        #                                                                                   num_obs   = ("speed", "count")
        #                                                                                   )
        # avg_spd_df = pd.concat([avg_spd_gen, avg_spd_day])
        # avg_spd_df = avg_spd_df.sort_values(["route_id", "day_num"])
        # del avg_spd_df["day_num"]
        #
        # # Export Data
        # out_path = self.out_dict["BUS_SPEED"]
        # db_path = out_path + f"{self.slash}AVG_BUS_SPEED_{datetime.now().strftime(self.td_s_dt_dsply_frmt)}_M30DAYS.csv"
        # avg_spd_df.to_csv(db_path, index=False)
        #
        # # For Debugging
        # if self.DEBUG_VAL == 1:
        #     print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: Exported Bus Route Average Speed DF")
        #
        # del np
