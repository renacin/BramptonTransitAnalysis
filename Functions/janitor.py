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
import urllib.request
import subprocess
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



# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    janitor = Janitor()
