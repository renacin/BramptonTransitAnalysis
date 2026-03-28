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
import subprocess
import urllib.request
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")
# ----------------------------------------------------------------------------------------------------------------------



class Janitor:
    """ This class will set up databases, perform maintenance, and ensure working order of system  """


    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):
        """ This function will run when the DataCollector Class is instantiated """

        # Internalize Needed URLs: Bus Location API, Bus Routes, Bus Stops
        self.bus_routes_url = r"https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-and-Maps.aspx"
        self.bus_stops_url  = r"https://opendata.arcgis.com/api/v3/datasets/1c9869fd805e45339a9e3373fc10ce08_0/downloads/data?format=csv&spatialRefId=3857&where=1%3D1"
        
        # Check To See If Appropriate Sub Folders Exist, Where Are We Writting Data?
        self.out_dict = {}

        # Datetime Variables
        self.td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
        self.td_s_dt_dsply_frmt = "%d-%m-%Y"


        # Find What Operating System And Create A Temp Working Space In Downloads Folder
        self.__find_op_sys()
        self.__find_downloads_folder()
        self.__define_paths()
        self.__create_folders()
        self.__db_check()
        self.__get_bus_stops()
        print(self.__get_rts())


    # -------------------- Private Function #1 ---------------------------------
    def __find_op_sys(self):
        """ Find The Hostname & The Operating System On Where This Code Is Running, Paths Will Be Different For Each Op-Sys"""

        # Get Host Name
        host_name = socket.gethostname()

        # Get Operating System
        if   sys.platform == 'win32': self.op_sys = 'Windows'
        elif sys.platform == 'darwin': self.op_sys = 'MacOS'
        elif sys.platform.startswith('linux'): self.op_sys = 'Linux'



    # -------------------- Private Function #2 ---------------------------------
    def __find_downloads_folder(self):
        """ Find The Hostname & The Operating System On Where This Code Is Running, Paths Will Be Different For Each Op-Sys"""

        # Get USer Path & Downloads 
        self.user_path  = os.path.expanduser('~')
        self.dwnld_path = os.path.join(os.path.expanduser('~'), 'Downloads')

        # Verify That The Path Exists Raise Error!
        if os.path.exists(self.dwnld_path) != True:
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: [ERROR] Download Folder Does Not Exist")
            sys.exit(1)



    # -------------------- Private Function #3 ---------------------------------
    def __define_paths(self):
        """ Given The Validity Of Downloads Folder Define Needed Paths For Each Folder (OS Specific) """

        # If *UNIX Based
        if self.op_sys in ["MacOS", "Linux"]:
            db_out_path          = fr"{self.dwnld_path}/BramptonTransitAnalysis/3_Data"
            self.db_folder       = db_out_path
            self.csv_out_path    = fr"{self.dwnld_path}/BramptonTransitAnalysis/4_Storage"
            self.db_path         = db_out_path + fr"/DataStorage.db"
            self.rfresh_tkn_path = fr"{self.dwnld_path}/DropboxInfo/GrabToken.sh"

        # If Windows Based
        elif self.op_sys in ["Windows"]:
            db_out_path          = fr"{self.dwnld_path}\BramptonTransitAnalysis\3_Data"
            self.db_folder       = db_out_path
            self.csv_out_path    = fr"{self.dwnld_path}\BramptonTransitAnalysis\4_Storage"
            self.db_path         = db_out_path + fr"\DataStorage.db"
            self.rfresh_tkn_path = fr"{self.dwnld_path}\DropboxInfo\GrabToken.sh"

        # Else Throw An Error
        else:
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: [ERROR] Can't Create Folders")
            sys.exit(1)



    # -------------------- Private Function #4 ---------------------------------
    def __create_folders(self):
        """ Create Needed Folders If They Don't Exist Already """

        # First Check To See If The Main Folder Exists!
        if not os.path.isdir(self.db_folder):
            os.makedirs(self.db_folder)

        # In The Out Directory Provided See If The Appropriate Sub Folders Exist!
        for fldr_nm in ['BUS_STP', 'BUS_LOC', 'FRMTD_DATA', 'BUS_SPEED', 'GRAPHICS', 'ERROR']:
            dir_chk = f"{self.csv_out_path}/{fldr_nm}"
            self.out_dict[fldr_nm] = dir_chk 
            if not os.path.exists(dir_chk):
                os.makedirs(dir_chk)



    # -------------------------- Private Function 5 ----------------------------
    def __db_check(self):
        """ Create a database that will store bus location data; as well as basic database inter data """

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
        conn = sqlite3.connect(":memory:")
        parent_bus_terminals.to_sql("parent_bus_terminals", conn, index=False)
        stops_in_terminals.to_sql("stops_in_terminals",     conn, index=False)
        stops_not_terminals.to_sql("stops_not_terminals",   conn, index=False)

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
        bus_stops = pd.read_sql_query(sql_query, conn)

        # Drop temporary columns
        bus_stops = bus_stops.drop(columns=["CLEANED_STOP_NAME", "CLEANED_STOP_LAT", "CLEANED_STOP_LON"])

        # Save to CSV
        dt_string = datetime.now().strftime(self.td_s_dt_dsply_frmt)
        out_path = f"{self.out_dict['BUS_STP']}/BUS_STP_DATA_{dt_string}.csv"
        bus_stops.to_csv(out_path, index=False)

        # Log export
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






# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    janitor = Janitor()
