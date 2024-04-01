# Name:                                            Renacin Matadeen
# Date:                                               03/03/2024
# Title                           Main Logic Of Data Collector: Version 2 Memory Optimized?
#
# ----------------------------------------------------------------------------------------------------------------------
import gc
import os
import sys
import json
import time
import socket
import sqlite3
import requests
import dropbox
import dropbox.files
import subprocess
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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
        for fldr_nm in ['BUS_STP', 'BUS_LOC', 'GRAPHICS', 'ERROR']:
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

        # Download Bus Stop Data & Export To Folder
        dwnld_stp_data_df = pd.read_csv(self.bus_stops_url)
        dt_string = datetime.now().strftime(self.td_s_dt_dsply_frmt)
        out_path = self.out_dict["BUS_STP"] + f"/BUS_STP_DATA_{dt_string}.csv"
        dwnld_stp_data_df.to_csv(out_path, index=False)

        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Exported Bus Stop Data")

        return dwnld_stp_data_df



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
        return pd.DataFrame(rt_data, columns=["RT_LINK", "RT_DIR", "RT_GRP", "RT_NUM", "RT_NM"])



    def __get_rt_stops(self, rt_links, rt_names):
        """
        Given a list of links relating to bus stops visited on certain routes, this
        function navigates through each, pulling information regarding each bus stop
        visited. This function returns a pandas dataframe with all the parsed information.
        """

        stp_data = []
        num_rts = len(rt_names)
        counter = 1
        for link, name in zip(rt_links, rt_names):

            # Navigate To WebPage, Pull All HTML, Convert To String, Use Regex To Pull All Stop Names
            page = requests.get(link)
            soup = BeautifulSoup(page.content, "html.parser")
            hrefs = soup.find_all(class_="link-to-stop")

            # Create A List Of The Bus Stops Found W/ Name For Join To Main Data
            rw_bs = [name + "###" + str(x).split('">')[1].replace("</a>", "") for x in hrefs]
            stp_data.extend(rw_bs)
            counter += 1

        # Return A Pandas Dataframe With Route Data
        stp_df = pd.DataFrame(stp_data, columns=["RAW_DATA"])
        stp_df[["RT_NM", "STP_NM"]] = stp_df["RAW_DATA"].str.split("###", n=1, expand=True)
        stp_df.drop(["RAW_DATA"], axis=1, inplace=True)

        # Add A Column That Shows Row Number For Each Bus Stop In A Route
        stp_df["RT_STP_NUM"] = stp_df.groupby(["RT_NM"]).cumcount() + 1

        # Add A Column That Shows How Many Bus Stops Are In A Given Route
        num_stps_df = stp_df.groupby("RT_NM", as_index=False).agg(RT_NUM_STPS = ("RT_NM", "count"))
        stp_df = stp_df.merge(num_stps_df, on='RT_NM', how='left')

        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Downloaded Bus Route Data")

        return stp_df



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
        print(f"{now}: (Parsed DF Len: {len(parsed_df)}), (Downloaded DF Len: {len(downld_df)}), (Number Of Missing Stops: {len(misng_stps)})")

        return parsed_df



    def __get_routes_nd_stops(self):
        """ On instantiation this function will be called. Using Brampton's Open
        Data API Link, Download, Bus Route Data, And Related Bus Stops, Export To
        SQLite3 Database. This function should only be run on instantiation. """

        # Download All Bus Stops From Brampton Transits Website
        dwnld_stp_data_df = self.__get_bus_stops()

        # Pull Route Info, Then Related Bus Stop Info
        rt_df = self.__get_rts()
        stp_df = self.__get_rt_stops(rt_df["RT_LINK"].to_list(), rt_df["RT_NM"].to_list())

        # Merge Data
        stp_data_df = stp_df.merge(rt_df, on='RT_NM', how='left')

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



    # ------------------------- Public Function 3 ------------------------------
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
