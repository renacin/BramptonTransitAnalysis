# Name:                                            Renacin Matadeen
# Date:                                               06/13/2026
# Title                                       Main Logic Of GTFS Data Downloader
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import sqlite3
import requests
import shutil
import pandas as pd
from datetime import datetime

from Functions.data_helper import *
from Functions.env_config  import Config
# ----------------------------------------------------------------------------------------------------------------------


class GTFS_Downloader():
    """ This class will set up the tools needed to download GTFS data  """

    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):
        """ On Instantiation Pull Config Settings """
        # Grab Config Files
        self.cfg = Config()



    # ~~~~~~~~~~~~~~~~~~~~~ Public Function #1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def gather_GTFS(self):

        """ Gather GTFS Data"""
        # self.__get_gtfs_data()
        # self.__upld_gtfs_data()
        self.__create_routemasterkey()



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
                    raise OSError(f"[ERROR] Could Not Remove {file_ext} Files")



    # -------------------- Private Function #3 ---------------------------------
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
                shared_logger("Data Janitor  ", f"Checking For New GTFS Data", 1, self.cfg.dblog_path)

                # Remove Unneeded Files & Folders
                try:
                    os.remove(self.cfg.zip_path)
                except OSError as e:
                    shared_logger("Data Janitor  ", f"[ERROR] Could Not Remove Zip", 3, self.cfg.dblog_path)
                    raise e
            
            except shutil.ReadError as e:
                shared_logger("Data Janitor  ", f"[ERROR] Could Not Extract GTFS Data", 3, self.cfg.dblog_path)
                raise e

        else:
            shared_logger("Data Janitor  ", f"[ERROR] Bad Response", 3, self.cfg.dblog_path)
            raise requests.exceptions.HTTPError(f"Bad response: {response.status_code}")
        
        

    # -------------------- Private Function #4 ---------------------------------
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
                        shared_logger("Data Janitor  ", f"New GTFS Data Uploaded -> {file_name}", 1, self.cfg.dblog_path)


            # Delete All Text Files In Folder
            self.__delete_files(".txt", self.cfg.foldr_path)



    # -------------------- Private Function #4 ---------------------------------
    @time_it
    def __create_routemasterkey(self):
        """
        Having Pulled GTFS Data, Create A Master Key, A CSV File Containing The Each Routes, Each Stop, In Sequence, With All Needed Details
        We Are Creating This Once (When New Data Is Present) As It Is Too Expensive To Do In SQL Everyday.
        """

        # Define Out Folder
        out_path = os.path.join(self.cfg.csv_out_path, f"ROUTES_MASTERKEY")

        # GTFS Downlaoder Will Run Before This, Check The Feed Versions Of Each Table
        with sqlite3.connect(self.cfg.db_path, timeout=30, isolation_level=None) as conn:

            try:

                # Set PRAGMAs BEFORE Any Transactions, This Is Not An Urgent Connection
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=30000")

                # Find Most Current Feed Version
                unique_feed_version = pd.read_sql_query(f"""SELECT MAX(MAX_FEED) AS MAX_FEED_VER
                                                            FROM (SELECT   MAX(feed_version) AS MAX_FEED FROM TRIPS
                                                                  UNION ALL
                                                                  SELECT   MAX(feed_version) AS MAX_FEED FROM STOPS
                                                                  UNION ALL
                                                                  SELECT   MAX(feed_version) AS MAX_FEED FROM STOP_TIMES)
                                                        """, conn)
                

                # Grab Needed Data For Coparison
                gtfs_feed_version    = int(unique_feed_version['MAX_FEED_VER'].iloc[0])
                focus_raw_csv        = [file_ for file_ in list(os.listdir(out_path)) if file_[9:] == "ROUTEMASTERKEY.csv"]


                # May Run Into An Value Error, Nest In Try/Except
                try:
                    max_file_version = max([int(file_[:8]) for file_ in focus_raw_csv])
                except ValueError:
                    max_file_version = 0


                # Look Into Feed, Run Only If No File, Or Current Masterkey Is Older Than GTFS Feed
                if (len(focus_raw_csv) == 0) or (gtfs_feed_version > max_file_version):
                
                    # Read In Stops Data With Order Of Sequence
                    stops_seq = pd.read_sql_query(f"""SELECT
                                                        A.*,
                                                        B.stop_sequence,
                                                        B.stop_id
                                                    
                                                        FROM       (SELECT
                                                                    route_id,
                                                                    service_id,
                                                                    trip_id,
                                                                    trip_headsign,
                                                                    direction_id
                                                                    FROM       TRIPS
                                                                    WHERE feed_version = (SELECT MAX(feed_version) FROM TRIPS)
                                                                ) AS A
                                                    
                                                        LEFT JOIN  (SELECT
                                                                    trip_id,
                                                                    stop_id,
                                                                    stop_sequence
                                                                    FROM       STOP_TIMES
                                                                    WHERE feed_version = (SELECT MAX(feed_version) FROM STOP_TIMES)
                                                                ) AS B
                                                        ON (A.trip_id = B.trip_id)
                                                    """, conn)
                    

                    # Read In Stops Data With Order Of Sequence
                    stops_names = pd.read_sql_query(f"""SELECT
                                                        C.stop_id,
                                                        C.stop_name,
                                                        C.stop_lat,
                                                        C.stop_lon
                                                    
                                                        FROM       (SELECT
                                                                    stop_id,
                                                                    stop_name,
                                                                    stop_lat,
                                                                    stop_lon
                                                                    FROM       STOPS
                                                                    WHERE feed_version = (SELECT MAX(feed_version) FROM STOPS)
                                                                ) AS C
                                                    """, conn)
                    
                    # Merge Data In Pandas
                    stops_seq['stop_id']           = stops_seq['stop_id'].astype(str)
                    stops_seq["stop_id"]           = stops_seq["stop_id"].str.replace(r"\.0$", "",   regex=True)
                    stops_names['stop_id']         = stops_names['stop_id'].astype(str)
                    stops_names["stop_id"]         = stops_names["stop_id"].str.replace(r"\.0$", "", regex=True)
                    stops_df                       = pd.merge(stops_seq, stops_names, on='stop_id', how='left')

                    # Export Data
                    out_file = os.path.join(out_path, f"{gtfs_feed_version}_ROUTEMASTERKEY.csv")
                    stops_df.to_csv(out_file, index=False)
                    shared_logger("Data Janitor", f"New Route Masterkey Created: {gtfs_feed_version}", 1, self.cfg.dblog_path)


            # If Something Happens Rollback To Begin, Inform User, And Wait
            except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Janitor", f"Failed To Create New Route Masterkey: {e}", 2, self.cfg.dblog_path)

            except KeyboardInterrupt:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Janitor", f"Keyboard Interrupt", 2, self.cfg.dblog_path)

            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Janitor", f"Failed To Create New Route Masterkey: {e}", 2, self.cfg.dblog_path)




# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":

    # For Testing Remove Once Finished
    gtfs_ = GTFS_Downloader()
    gtfs_.gather_GTFS()
