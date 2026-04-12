# Name:                                            Renacin Matadeen
# Date:                                               03/028/2026
# Title                           Main Logic Of Data Janitor: Version 3 Memory Optimized?
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import sys
import sqlite3
import requests
import shutil
import numpy as np
import pandas as pd
from datetime import datetime

# ----------------------------------------------------------------------------------------------------------------------



class Janitor():
    """ This class will set up databases, perform maintenance, and ensure working order of system  """


    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self, log_level = 0):
        """ This function will run when the DataCollector Class is instantiated """

        # Try To Create A Table For Each Item In The Following Database
        self.GTFS_URL     = r'https://www.arcgis.com/sharing/rest/content/items/a355aabd5a8c490186bdce559c9c75fb/data'
        self.GATHER_TABLE = {"BUS_LOC_DB", "U_ID_TEMP", "ERROR_DB", "ROUTE_SPEED"}
        self.table_dict   = {
            "BUS_LOC_DB":     ["u_id", "id", "is_deleted", "trip_update", "alert", "trip_id", "start_time", "start_date", "schedule_relationship", "route_id", "latitude", "longitude", "bearing", "odometer", "speed", "current_stop_sequence", "current_status", "timestamp", "congestion_level", "stop_id", "vehicle_id", "label", "license_plate", "dt_colc"],
            "U_ID_TEMP":      ["u_id", "timestamp"],
            "ERROR_DB":       ["timestamp", "e_type", "delay"],
            "FEED_INFO":      ["feed_publisher_name", "feed_lang", "feed_start_date", "feed_end_date", "feed_version"],
            "ROUTES":         ["route_id", "route_short_name", "route_long_name", "feed_version"],
            "TRIPS":          ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "block_id", "shape_id", "feed_version"],
            "STOPS":          ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "zone_id", "stop_url", "parent_station", "feed_version"],
            "STOP_TIMES":     ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "timepoint", "feed_version"],
            "ROUTE_SPEED":    ["route_id", "route_short_name", "route_long_name", "speed", "feed_version"]
        }

        # Define All Needed Paths
        self.user_path       = os.path.expanduser('~')
        self.dwnld_path      = os.path.join(os.path.expanduser('~'), 'Downloads')
        db_out_path          = os.path.join(self.dwnld_path, "BramptonTransitAnalysis", "3_Data")
        self.db_folder       = db_out_path
        self.db_path         = os.path.join(db_out_path, "DataStorage.db")
        self.csv_out_path    = os.path.join(self.dwnld_path, "BramptonTransitAnalysis", "4_Storage")
        self.rfresh_tkn_path = os.path.join(self.dwnld_path, "DropboxInfo", "GrabToken.sh")
        self.zip_path        = os.path.join(self.csv_out_path, "GTFS", "GTFS.zip")
        self.foldr_path      = os.path.join(self.csv_out_path, "GTFS")
        self.out_dict        = {}

        # Datetime Variables
        self.td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
        self.td_s_dt_dsply_frmt = "%d-%m-%Y"
        self.log_level          = log_level

        # Print For Logging
        self.__logger("Data Janitor | Janitor Ready")



    # ~~~~~~~~~~~~~~~~~~~~~ Public Function #1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def setup(self):
        """ Find The Hostname & The Operating System On Where This Code Is Running, Paths Will Be Different For Each Op-Sys"""

        # Find What Operating System And Create A Temp Working Space In Downloads Folder
        self.__find_downloads_folder()
        self.__create_folders()
        self.__db_check()
        self.__get_gtfs_data()
        self.__upld_gtfs_data()
        self.__calc_trip_avg_speed()



    # -------------------- Private Function #1 ---------------------------------
    def __logger(self, message = ""):
        """ Find The Location Of The Downloads Folder """

        # Verify That The Path Exists Raise Error!
        if self.log_level == 1:
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: {message}")



    # -------------------- Private Function #1 ---------------------------------
    def __delete_files(self, file_ext = "", path = ""):
        """ Delete All Files In Path """

        # Verify That The Path Exists Raise Error!
        for file_ in os.listdir(path):
            full_path = os.path.join(path, file_)
            if file_.endswith(file_ext):
                try:
                    os.remove(full_path)
                except OSError as e:
                    raise Exception(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | [ERROR] Could Not Remove {file_ext} Files")



    # -------------------- Private Function #1 ---------------------------------
    def __find_downloads_folder(self):
        """ Find The Location Of The Downloads Folder """

        # Verify That The Path Exists Raise Error!
        if os.path.exists(self.dwnld_path) != True:
            raise Exception(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | [ERROR] Download Folder Does Not Exist")



    # -------------------- Private Function #2 ---------------------------------
    def __create_folders(self):
        """ Create Needed Folders If They Don't Exist Already """

        # First Check To See If The Main Folder Exists!
        if not os.path.isdir(self.db_folder):
            os.makedirs(self.db_folder)

        # In The Out Directory Provided See If The Appropriate Sub Folders Exist!
        for fldr_nm in ['GTFS', 'BUS_STP', 'BUS_LOC', 'FRMTD_DATA', 'BUS_SPEED', 'GRAPHICS', 'ERROR']:
            dir_chk = os.path.join(self.csv_out_path, fldr_nm)
            self.out_dict[fldr_nm] = dir_chk 
            if not os.path.exists(dir_chk):
                os.makedirs(dir_chk)

        # Log export
        self.__logger("Data Janitor | Folders Prepared")



    # -------------------- Private Function #3 ---------------------------------
    def __db_check(self):
        """ Create a database that will store bus location data; as well as basic database inter data """

        # Iterate Through Table Dictionary And Create Tables If They Don't Exist Already
        with sqlite3.connect(self.db_path) as conn:
            for table_ in self.table_dict:
                sql_string = ", ".join(self.table_dict[table_])
                conn.execute(f'''CREATE TABLE IF NOT EXISTS {table_} ({sql_string});''')
                conn.commit()

        # Log export
        self.__logger("Data Janitor | Databases Ready")



    # -------------------- Private Function #4 ---------------------------------
    def __get_gtfs_data(self):
        """
        When run this function will navigate to the City of Brampton's GTFS data repository
        and download all needed data. With Respects To Effective Range, Upload GTFS Data To A Database.
        """

        # Internalize URL, And Use Requests To Get Data
        response = requests.get(self.GTFS_URL)

        # Try To Get GTFS Zip Data
        if response.status_code == 200:
            with open(self.zip_path, 'wb') as f:
                f.write(response.content)

            try:
                # Extract Data
                shutil.unpack_archive(self.zip_path, self.foldr_path)
                self.__logger("Data Janitor | Extracted GTFS Data")

                # Remove Unneeded Files & Folders
                try:
                    os.remove(self.zip_path)
                except OSError as e:
                    raise Exception(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | [ERROR] Could Not Remove Zip")
            
            except shutil.ReadError as e:
                raise Exception(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | [ERROR] Could Not Extract GTFS Data")

        else:
            raise Exception(f"[{datetime.now().strftime(self.td_l_dt_dsply_frmt)}]: Data Janitor | [ERROR] Bad Response")



    # -------------------- Private Function #5 ---------------------------------
    def __upld_gtfs_data(self):
        """
        Having Pulled GTFS Data, Check The Effective Date Range Of The Data, If New Upload To The Database, Else Pass
        """

        # Make A Connection To The Database
        with sqlite3.connect(self.db_path) as conn:

            # First Find The GTFS Feed_Info.txt File
            feed_df          = pd.read_csv(os.path.join(self.csv_out_path, "GTFS", "feed_info.txt"), usecols=["feed_version"])
            feed_cur_version = str(feed_df['feed_version'].iloc[0])
            all_gtfs_ver     = pd.read_sql_query("SELECT DISTINCT feed_version FROM FEED_INFO", conn)
            del feed_df
        
            # If The Current Edit Is Not In The Database Add All Data
            if feed_cur_version not in all_gtfs_ver["feed_version"].values:

                # Upload The Rest Of The Data | Only Update GTFS Files
                for file_name in self.table_dict:
                    if file_name not in self.GATHER_TABLE:
                        temp_df                 = pd.read_csv(os.path.join(self.csv_out_path, "GTFS", f"{file_name.lower()}.txt"))
                        temp_df["feed_version"] = feed_cur_version
                        temp_df                 = temp_df[self.table_dict[file_name]]
                        temp_df.to_sql(file_name, conn, if_exists="append", index=False)
                        self.__logger(f"Data Janitor | New GTFS Data Uploaded -> {file_name}")

            # Delete All Text Files In Folder
            self.__delete_files(".txt", self.foldr_path)



    # -------------------- Private Function #6 ---------------------------------
    def __calc_trip_avg_speed(self):
        """
        This function will look at the GTFS Data pulled, if the feed version is new it will pull all stop locations and determine the 
        lenght of the trip, and the average speed. The results will then be uploaded into a corresponding table in the database.
        """

        # There are some time stamps that can't be recognized (24:00:00) breaks for somereason
        def parse_transit_time(time_str):
            time_str = str(time_str)
            h, m, s = map(int, time_str.split(':'))
            total_seconds = h * 3600 + m * 60 + s
            return pd.Timedelta(seconds = total_seconds)
            

        # Define Function That Will Determine The Distance Between Two Points
        def hvrsn_dist(coord1, coord2):
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

            return R * c  # Returns Distance In KM
        

        self.__logger(f"TESTING | EVERYTHING IMPORTED")


        # Start Small Read In GTFS Data - STOPS & STOP_TIMES
        with sqlite3.connect(self.db_path) as conn:
            stops           = pd.read_sql_query(f""" SELECT B.stop_id,
                                                            B.stop_code,
                                                            B.stop_name,
                                                            B.stop_lat,
                                                            B.stop_lon,
                                                            B.feed_version
                                                     FROM STOPS AS B 
                                                     WHERE 1=1""", conn)
            
            stops_times     = pd.read_sql_query(f""" SELECT A.trip_id, 
                                                            A.stop_sequence, 
                                                            A.stop_id, 
                                                            A.arrival_time, 
                                                            A.departure_time, 
                                                            A.feed_version 
                                                     FROM STOP_TIMES AS A 
                                                     WHERE 1=1""", conn)
            
        self.__logger(f"TESTING | READ TABLES")

        # Cast Columns As Int
        stops_times["stop_id"]      = stops_times["stop_id"].astype(str).str.lstrip("0")
        stops["stop_id"]            = stops["stop_id"].astype(str).str.lstrip("0")
        stops_times["feed_version"] = stops_times["feed_version"].astype(int)
        stops["feed_version"]       = stops["feed_version"].astype(int)

        self.__logger(f"TESTING | CONVERTED TABLES")

        # Merge Data
        stops_times = stops_times.merge(stops[["stop_id", "feed_version", "stop_code", "stop_name", "stop_lat", "stop_lon"]], on=["stop_id", "feed_version"], how="left").sort_values(["trip_id", "stop_sequence"])
        del stops

        self.__logger(f"TESTING | MERGE TABLES")

        # Order Data
        stops_times = stops_times.sort_values(by=["trip_id", "stop_sequence"])

        # Create Lagged Columns
        stops_times['nxt_stop_id']         = stops_times.groupby('trip_id')['stop_id'].shift(-1)
        stops_times['nxt_stop_name']       = stops_times.groupby('trip_id')['stop_name'].shift(-1)
        stops_times['nxt_stop_lat']        = stops_times.groupby('trip_id')['stop_lat'].shift(-1)
        stops_times['nxt_stop_lon']        = stops_times.groupby('trip_id')['stop_lon'].shift(-1)
        stops_times['nxt_arrival_time']    = stops_times.groupby('trip_id')['arrival_time'].shift(-1)
        stops_times['nxt_departure_time']  = stops_times.groupby('trip_id')['departure_time'].shift(-1)

        self.__logger(f"TESTING | GROUPED TABLES")

        # Sort Columns For Readability
        stops_times = stops_times[['trip_id', 'stop_sequence', 'stop_id', 'arrival_time', 
                                    'departure_time', 'stop_code', 'stop_name', 'stop_lat', 
                                    'stop_lon', 'nxt_stop_id', 'nxt_stop_name', 'nxt_stop_lat', 
                                    'nxt_stop_lon', 'nxt_arrival_time', 'nxt_departure_time', 
                                    'feed_version']]

        # Determine As The Crows Fly Distance Between Bus Stops
        stops_times['km2nxtstp'] = hvrsn_dist((stops_times['stop_lat'].values, stops_times['stop_lon'].values), (stops_times['nxt_stop_lat'].values, stops_times['nxt_stop_lon'].values))

        # Deal With The Missing Data At The End Of A Trip
        for col in ["stop_lat", "stop_lon", "stop_name", "arrival_time", "departure_time"]:
            stops_times[f"nxt_{col}"] = stops_times[f"nxt_{col}"].fillna(stops_times[f"{col}"])
        stops_times["km2nxtstp"] = stops_times["km2nxtstp"].fillna(0)

        # Convert Time To Operable Time Stamp
        for col in ["arrival_time", "departure_time", "nxt_arrival_time", "nxt_departure_time"]:
            stops_times[col] = stops_times[col].apply(parse_transit_time)

        # Determine Time Between Arrival & Departure & Time To Next Stop
        stops_times["idle_time"] = (stops_times["departure_time"] - stops_times["arrival_time"]).dt.total_seconds()
        stops_times["trvl_time"] = (stops_times["nxt_arrival_time"] - stops_times["departure_time"]).dt.total_seconds()


        # Test Data
        test = stops_times[stops_times["trip_id"] == 24914446]
        test.to_csv(r'C:\Users\renac\Desktop\TestData.csv')

        # Logger
        self.__logger(f"Data Janitor | Speed Data Calculated")







# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    janitor = Janitor(log_level = 1)
    janitor.setup()
