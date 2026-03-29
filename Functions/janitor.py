# Name:                                            Renacin Matadeen
# Date:                                               03/028/2026
# Title                           Main Logic Of Data Janitor: Version 3 Memory Optimized?
#
# ----------------------------------------------------------------------------------------------------------------------
import gc
import os
import re
import sys
import time
import socket
import sqlite3
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")
# ----------------------------------------------------------------------------------------------------------------------



class Janitor():
    """ This class will set up databases, perform maintenance, and ensure working order of system  """


    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self, log_level = 0):
        """ This function will run when the DataCollector Class is instantiated """

        # Check To See If Appropriate Sub Folders Exist, Where Are We Writting Data?
        self.out_dict = {}

        # Datetime Variables
        self.td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
        self.td_s_dt_dsply_frmt = "%d-%m-%Y"

        # Print For Logging
        self.log_level = log_level
        if self.log_level == 1:
            print(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | Janitor Ready")


        # Find What Operating System And Create A Temp Working Space In Downloads Folder
        self.__find_op_sys()
        self.__find_downloads_folder()
        self.__define_paths()
        self.__create_folders()
        self.__db_check()
        self.__get_gtfs_data()
        self.__upld_gtfs_data()



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
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: Data Janitor | [ERROR] Download Folder Does Not Exist")
            sys.exit(1)


    # -------------------- Private Function #3 ---------------------------------
    def __define_paths(self):
        """ Given The Validity Of Downloads Folder Define Needed Paths For Each Folder (OS Specific) """

        # Backslash Or Forward Slash?
        if   self.op_sys in ["MacOS", "Linux"]: self.fldr_sep  = "/"
        elif self.op_sys in ["Windows"]:        self.fldr_sep  = "\\"
        else:
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: Data Janitor |[ERROR] Can't Create Folders")
            sys.exit(1)

        # Create Folders
        sp                   = self.fldr_sep
        db_out_path          = fr"{self.dwnld_path}{sp}BramptonTransitAnalysis{sp}3_Data"
        self.db_folder       = db_out_path
        self.db_path         = db_out_path + fr"{sp}DataStorage.db"
        self.rfresh_tkn_path = fr"{self.dwnld_path}{sp}DropboxInfo/GrabToken.sh"
        self.csv_out_path    = fr"{self.dwnld_path}{sp}BramptonTransitAnalysis{sp}4_Storage"
        self.rfresh_tkn_path = fr"{self.dwnld_path}{sp}DropboxInfo/GrabToken.sh"


    # -------------------- Private Function #4 ---------------------------------
    def __create_folders(self):
        """ Create Needed Folders If They Don't Exist Already """

        # First Check To See If The Main Folder Exists!
        if not os.path.isdir(self.db_folder):
            os.makedirs(self.db_folder)

        # In The Out Directory Provided See If The Appropriate Sub Folders Exist!
        sp = self.fldr_sep
        for fldr_nm in ['GTFS', 'BUS_STP', 'BUS_LOC', 'FRMTD_DATA', 'BUS_SPEED', 'GRAPHICS', 'ERROR']:
            dir_chk = fr"{self.csv_out_path}{sp}{fldr_nm}"
            self.out_dict[fldr_nm] = dir_chk 
            if not os.path.exists(dir_chk):
                os.makedirs(dir_chk)

        # Log export
        if self.log_level == 1:
            print(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | Folders Prepared")


    # -------------------- Private Function #5 ---------------------------------
    def __db_check(self):
        """ Create a database that will store bus location data; as well as basic database inter data """

        # Try To Create A Table For Each Item In The Following Database
        table_dict = {
            "BUS_LOC_DB":     ["u_id", "id", "is_deleted", "trip_update", "alert", "trip_id", "start_time", "start_date", "schedule_relationship", "route_id", "latitude", "longitude", 
                               "bearing", "odometer", "speed", "current_stop_sequence", "current_status", "timestamp", "congestion_level", "stop_id", "vehicle_id", "label", "license_plate", "dt_colc"],
            "U_ID_TEMP":      ["u_id", "timestamp"],
            "ERROR_DB":       ["timestamp", "e_type", "delay"],
            "GTFS_FEED":      ["feed_publisher_name", "feed_lang", "feed_start_date", "feed_end_date", "feed_version"],
            "ROUTES":         ["route_id", "route_short_name", "route_long_name", "feed_version"],
            "TRIPS":          ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "block_id", "shape_id", "feed_version"],
            "STOPS":          ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "zone_id", "stop_url", "parent_station", "feed_version"],
            "STOP_TIMES":     ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "timepoint", "feed_version"]
        }


        # Iterate Through Table Dictionary And Create Tables If They Don't Exist Already
        for table_ in table_dict:
            with sqlite3.connect(self.db_path) as conn:
                sql_string = ", ".join(table_dict[table_])
                conn.execute(f'''CREATE TABLE IF NOT EXISTS {table_} ({sql_string});''')
                conn.commit()


        # Log export
        if self.log_level == 1:
            print(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | Databases Ready")


    # -------------------- Private Function #6 ---------------------------------
    def __get_gtfs_data(self):
        """
        When run this function will navigate to the City of Brampton's GTFS data repository
        and download all needed data. With Respects To Effective Range, Upload GTFS Data To A Database.
        """

        # Import Needed Libaries
        import requests
        import shutil

        # Internalize URL, And Use Requests To Get Data
        self.gtfs_url = r'https://www.arcgis.com/sharing/rest/content/items/a355aabd5a8c490186bdce559c9c75fb/data'
        response = requests.get(self.gtfs_url)

        # Define Needed Variables First
        sp = self.fldr_sep
        zip_path        = f"{self.csv_out_path}{sp}GTFS{sp}GTFS.zip"
        self.foldr_path = f"{self.csv_out_path}{sp}GTFS"

        # Try To Get GTFS Zip Data
        if response.status_code == 200:
            with open(zip_path, 'wb') as f:
                f.write(response.content)

            try:
                # Extract Data
                shutil.unpack_archive(zip_path, self.foldr_path)
                if self.log_level == 1:
                    print(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | Extracted GTFS Data")

                # Remove Unneeded Files & Folders
                try:
                    os.remove(zip_path)
                except OSError as e:
                    raise Exception(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | [ERROR] Could Not Remove Zip")
            
            except shutil.ReadError as e:
                raise Exception(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | [ERROR] Could Not Extract GTFS Data")

        else:
            raise Exception(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | [ERROR] Could Not Extract GTFS Data")


    # -------------------- Private Function #7 ---------------------------------
    def __upld_gtfs_data(self):
        """
        Having Pulled GTFS Data, Check The Effective Date Range Of The Data, If New Upload To The Database, Else Pass
        """

        # Import Needed Libaries
        import pandas as pd

        # First Find The GTFS Feed_Info.txt File
        sp              = self.fldr_sep
        feed_df         = pd.read_csv(f"{self.csv_out_path}{sp}GTFS{sp}feed_info.txt")[["feed_publisher_name", "feed_lang", "feed_start_date", "feed_end_date", "feed_version"]]

        # Pull All GTFS Version
        with sqlite3.connect(self.db_path) as conn:
            all_gtfs_ver = pd.read_sql_query("SELECT DISTINCT feed_version FROM GTFS_FEED", conn)

        # If The Current Edit Is Not In The Database Add All Data
        if str(feed_df['feed_version'].iloc[0]) not in all_gtfs_ver["feed_version"].to_list():

            # Upload [GTFS Feed Information]
            with sqlite3.connect(self.db_path) as conn:
                feed_df.to_sql("GTFS_FEED", conn, if_exists="append", index=False)
                if self.log_level == 1:
                    print(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | New GTFS Data Uploaded -> FEEDS")
        
            # Create A Dictionary That Will Store The Columns We Want To Keep With The name of The File
            file_dict = {"routes":      ["route_id", "route_short_name", "route_long_name"],
                         "trips":       ['route_id', 'service_id', 'trip_id', 'trip_headsign', 'direction_id', 'block_id', 'shape_id'],
                         "stops":       ['stop_id', 'stop_code', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id', 'stop_url', 'parent_station'],
                         "stop_times":  ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence', 'pickup_type', 'drop_off_type', 'timepoint']}

            # Upload The Rest Of The Data
            for file_name in file_dict:
                with sqlite3.connect(self.db_path) as conn:
                    temp_df                 = pd.read_csv(f"{self.csv_out_path}{sp}GTFS{sp}{file_name}.txt")
                    temp_df                 = temp_df[file_dict[file_name]]
                    temp_df["feed_version"] = str(feed_df['feed_version'].iloc[0])

                    temp_df.to_sql(file_name.upper(), conn, if_exists="append", index=False)
                    if self.log_level == 1:
                        print(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | New GTFS Data Uploaded -> {file_name.upper()}")


        # Delete All .TXT Files After
        for file_ in os.listdir(self.foldr_path):
            full_path = os.path.join(self.foldr_path, file_)
            if file_.endswith('.txt'):
                try:
                    os.remove(full_path)

                except OSError as e:
                    print(e)
                    sys.exit(1)



# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    janitor = Janitor(log_level = 1)



