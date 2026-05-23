# Name:                                            Renacin Matadeen
# Date:                                               03/28/2026
# Title                                       Main Logic Of Database Setup
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import sqlite3
import requests
import shutil
import pandas as pd
from datetime import datetime

from helper     import *
from env_config import Config
# ----------------------------------------------------------------------------------------------------------------------


class EnvConfig():
    """ This class will set up databases  """

    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):
        """ On Instantiation Pull Config Settings """
        # Grab Config Files
        self.cfg = Config()


    # ~~~~~~~~~~~~~~~~~~~~~ Public Function #1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def setup(self):
        """ Find The Hostname & The Operating System On Where This Code Is Running, Paths Will Be Different For Each Op-Sys"""

        # Find What Operating System And Create A Temp Working Space In Downloads Folder
        self.__find_downloads_folder()
        self.__create_folders()
        self.__db_check()
        self.__get_gtfs_data()
        self.__upld_gtfs_data()
        self.__upld_trip_avg_speed()


    # -------------------- Private Function #1 ---------------------------------
    def __logger(self, message = ""):
        """ Find The Location Of The Downloads Folder """

        # Verify That The Path Exists Raise Error!
        print(f"{datetime.now().strftime(self.cfg.td_l_dt_dsply_frmt)}: {message}")


    # -------------------- Private Function #2 ---------------------------------
    def __delete_files(self, file_ext = "", path = ""):
        """ Delete All Files In Path """

        # Verify That The Path Exists Raise Error!
        for file_ in os.listdir(path):
            full_path = os.path.join(path, file_)
            if file_.endswith(file_ext):
                try:
                    os.remove(full_path)
                except OSError as e:
                    raise Exception(f"[{datetime.now().strftime(self.cfg.td_l_dt_dsply_frmt)}]: Data Janitor | [ERROR] Could Not Remove {file_ext} Files")


    # -------------------- Private Function #3 ---------------------------------
    def __find_downloads_folder(self):
        """ Find The Location Of The Downloads Folder """

        # Verify That The Path Exists Raise Error!
        if os.path.exists(self.cfg.dwnld_path) != True:
            raise Exception(f"[{datetime.now().strftime(self.cfg.td_l_dt_dsply_frmt)}]: Data Janitor | [ERROR] Download Folder Does Not Exist")


    # -------------------- Private Function #4 ---------------------------------
    def __create_folders(self):
        """ Create Needed Folders If They Don't Exist Already """

        # First Check To See If The Main Folder Exists!
        if not os.path.isdir(self.cfg.db_folder):
            os.makedirs(self.cfg.db_folder)

        # In The Out Directory Provided See If The Appropriate Sub Folders Exist!
        for fldr_nm in self.cfg.FOLDERs:
            dir_chk = os.path.join(self.cfg.csv_out_path, fldr_nm)
            self.cfg.out_dict[fldr_nm] = dir_chk 
            if not os.path.exists(dir_chk):
                os.makedirs(dir_chk)

        # Log export
        self.__logger("Data Janitor   | Folders Prepared")


    # -------------------- Private Function #5 ---------------------------------
    def __db_check(self):
        """ Create a database that will store bus location data; as well as basic database inter data """

        # Iterate Through Table Dictionary And Create Tables If They Don't Exist Already
        with sqlite3.connect(self.cfg.db_path) as conn:
            for table_ in self.cfg.table_dict:
                sql_string = ", ".join(self.cfg.table_dict[table_])
                conn.execute(f'''CREATE TABLE IF NOT EXISTS {table_} ({sql_string});''')
                conn.commit()

        # Log export
        self.__logger("Data Janitor   | Databases Ready")


    # -------------------- Private Function #6 ---------------------------------
    def __get_gtfs_data(self):
        """
        When run this function will navigate to the City of Brampton's GTFS data repository
        and download all needed data. With Respects To Effective Range, Upload GTFS Data To A Database.
        """

        # Internalize URL, And Use Requests To Get Data
        response = requests.get(self.cfg.GTFS_URL)

        # Try To Get GTFS Zip Data
        if response.status_code == 200:
            with open(self.cfg.zip_path, 'wb') as f:
                f.write(response.content)

            try:
                # Extract Data
                shutil.unpack_archive(self.cfg.zip_path, self.cfg.foldr_path)
                self.__logger("Data Janitor   | Extracted GTFS Data")

                # Remove Unneeded Files & Folders
                try:
                    os.remove(self.cfg.zip_path)
                except OSError as e:
                    raise Exception(f"[{datetime.now().strftime(self.cfg.td_l_dt_dsply_frmt)}]: Data Janitor   | [ERROR] Could Not Remove Zip")
            
            except shutil.ReadError as e:
                raise Exception(f"[{datetime.now().strftime(self.cfg.td_l_dt_dsply_frmt)}]: Data Janitor   | [ERROR] Could Not Extract GTFS Data")

        else:
            raise Exception(f"[{datetime.now().strftime(self.cfg.td_l_dt_dsply_frmt)}]: Data Janitor   | [ERROR] Bad Response")


    # -------------------- Private Function #7 ---------------------------------
    def __upld_gtfs_data(self):
        """
        Having Pulled GTFS Data, Check The Effective Date Range Of The Data, If New Upload To The Database, Else Pass
        """

        # Make A Connection To The Database
        with sqlite3.connect(self.cfg.db_path) as conn:

            # First Find The GTFS Feed_Info.txt File
            feed_df          = pd.read_csv(os.path.join(self.cfg.csv_out_path, "GTFS", "feed_info.txt"), usecols=["feed_version"])
            feed_cur_version = str(feed_df['feed_version'].iloc[0])
            all_gtfs_ver     = pd.read_sql_query("SELECT DISTINCT feed_version FROM FEED_INFO", conn)
            del feed_df
        
            # If The Current Edit Is Not In The Database Add All Data
            if feed_cur_version not in all_gtfs_ver["feed_version"].values:

                # Upload The Rest Of The Data | Only Update GTFS Files
                for file_name in self.cfg.table_dict:
                    if file_name not in self.cfg.GATHER_TABLE:
                        temp_df                 = pd.read_csv(os.path.join(self.cfg.csv_out_path, "GTFS", f"{file_name.lower()}.txt"))
                        temp_df["feed_version"] = feed_cur_version
                        temp_df                 = temp_df[self.cfg.table_dict[file_name]]
                        temp_df.to_sql(file_name, conn, if_exists="append", index=False)
                        self.__logger(f"Data Janitor   | New GTFS Data Uploaded -> {file_name}")

            # Delete All Text Files In Folder
            self.__delete_files(".txt", self.cfg.foldr_path)


    # -------------------- Private Function #8 ---------------------------------
    def __upld_trip_avg_speed(self):
        """
        This function will look at the GTFS Data pulled, if the feed version is new it will pull all stop locations and determine the 
        lenght of the trip, and the average speed. The results will then be uploaded into a corresponding table in the database.
        """

        # Are There Different Feed Versions In The Data That We're Pulling?
        with sqlite3.connect(self.cfg.db_path) as conn:
            max_colctd_feed_id     = pd.read_sql_query(""" SELECT DISTINCT 
                                                                MAX(feed_version) AS MAX_FEED_VER 
                                                            FROM FEED_INFO """,   conn)['MAX_FEED_VER'].fillna(0).iloc[0]
            
            max_routes_feed_id     = pd.read_sql_query(""" SELECT DISTINCT 
                                                                MAX(feed_version) AS MAX_FEED_VER 
                                                            FROM ROUTE_SPEED """, conn)['MAX_FEED_VER'].fillna(0).iloc[0]


            # If No New Data Back Out
            if int(max_colctd_feed_id) <= int(max_routes_feed_id):
                self.__logger(f"Data Janitor   | Speed Table Is Current")
                return


            # If New Data Present Calculate New Data Given New Feed Version
            stops                           = pd.read_sql_query(f""" 
                                                                    SELECT 
                                                                        B.stop_id, 
                                                                        B.stop_code, 
                                                                        B.stop_name, 
                                                                        B.stop_lat, 
                                                                        B.stop_lon,  
                                                                        B.feed_version
                                                                    FROM STOPS      AS B  
                                                                    WHERE B.feed_version = ? """, conn, params=(max_colctd_feed_id,))
            
            stops_times                     = pd.read_sql_query(f""" 
                                                                    SELECT 
                                                                        A.trip_id, 
                                                                        A.stop_sequence, 
                                                                        A.stop_id, 
                                                                        A.arrival_time,  
                                                                        A.departure_time, 
                                                                        A.feed_version 
                                                                    FROM STOP_TIMES AS A  
                                                                    WHERE A.feed_version = ? """, conn, params=(max_colctd_feed_id,))
            
            stops_times["stop_id"]          = stops_times["stop_id"].astype(str).str.lstrip("0")
            stops["stop_id"]                = stops["stop_id"].astype(str).str.lstrip("0")
            stops_times["feed_version"]     = stops_times["feed_version"].astype(int)
            stops["feed_version"]           = stops["feed_version"].astype(int)
            stops_times                     = stops_times.merge(stops[["stop_id", "feed_version", "stop_code", "stop_name", "stop_lat", "stop_lon"]], on=["stop_id", "feed_version"], how="left").sort_values(["trip_id", "stop_sequence"])
            del stops


            # Create Lagged Columns & Determine Distance Between
            shift_cols = ["stop_id", "stop_name", "stop_lat", "stop_lon", "arrival_time", "departure_time"]
            shifted                  = stops_times.groupby("trip_id")[shift_cols].shift(-1).rename(columns={c: f"nxt_{c}" for c in shift_cols})
            stops_times              = pd.concat([stops_times, shifted], axis=1)[['trip_id', 'stop_sequence', 'stop_id', 'arrival_time', 'departure_time', 'stop_code', 'stop_name', 'stop_lat', 'stop_lon', 'nxt_stop_id', 'nxt_stop_name', 'nxt_stop_lat', 'nxt_stop_lon', 'nxt_arrival_time', 'nxt_departure_time', 'feed_version']]
            stops_times['km2nxtstp'] = hvrsn_dist((stops_times['stop_lat'].values, stops_times['stop_lon'].values), (stops_times['nxt_stop_lat'].values, stops_times['nxt_stop_lon'].values))


            # Deal With The Missing Data At The End Of A Trip
            for col in ["stop_lat", "stop_lon", "stop_name", "arrival_time", "departure_time"]:
                stops_times[f"nxt_{col}"] = stops_times[f"nxt_{col}"].fillna(stops_times[f"{col}"])
            stops_times["km2nxtstp"] = stops_times["km2nxtstp"].fillna(0)


            # Convert Time To Operable Time Stamp
            for col in ["arrival_time", "departure_time", "nxt_arrival_time", "nxt_departure_time"]:
                stops_times[['h', 'm', 's']] = stops_times[col].str.split(':', expand=True)
                stops_times[['h', 'm', 's']] = stops_times[['h', 'm', 's']].astype(int)
                stops_times[f"{col}_sec"]    = (stops_times['h'] * 3600) + (stops_times['m'] * 60) + stops_times['s']
                stops_times.drop(columns     = ['h', 'm', 's'], inplace=True)


            # Determine Time Between Arrival & Departure & Time To Next Stop
            stops_times["idle_time"] = (stops_times["departure_time_sec"]   - stops_times["arrival_time_sec"])
            stops_times["trvl_time"] = (stops_times["nxt_arrival_time_sec"] - stops_times["departure_time_sec"])
            stops_times.drop(columns = ["arrival_time_sec", "departure_time_sec", "nxt_arrival_time_sec", "nxt_departure_time_sec"], inplace = True)


            # Determine Total Travel Time, Idle Time, Average Speed For Trip, Average Speed For Section
            avg_spd_df                     = stops_times.groupby(["trip_id", "feed_version"], as_index=False).agg(tot_dist = ("km2nxtstp", "sum"), tot_idle_time = ("idle_time", "sum"), tot_trvl_time = ("trvl_time", "sum"))
            avg_spd_df["tot_trip_time"]    = avg_spd_df["tot_idle_time"] +  avg_spd_df["tot_trvl_time"]
            avg_spd_df["avg_trip_speed"]   = (avg_spd_df["tot_dist"] / (avg_spd_df["tot_trip_time"] / 3600).replace(0, float("nan")))
            avg_spd_df["avg_trvl_speed"]   = (avg_spd_df["tot_dist"] / (avg_spd_df["tot_trvl_time"] / 3600).replace(0, float("nan")))


            # Round The Following Columns & Return Data
            avg_spd_df[["tot_dist", "avg_trip_speed", "avg_trvl_speed"]] = avg_spd_df[["tot_dist", "avg_trip_speed", "avg_trvl_speed"]].round(2)
        

            # Upload The Speed Dataframe Data To Respective Table
            avg_spd_df.to_sql("ROUTE_SPEED", conn, if_exists="append", index=False)
            self.__logger(f"Data Janitor   | New Route Speed Data Uploaded")



# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    env_setup = EnvConfig()
    env_setup.setup()
