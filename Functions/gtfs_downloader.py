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
        self.__get_gtfs_data()
        self.__upld_gtfs_data()


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
                        shared_logger("Data Janitor  ", f"New GTFS Data Uploaded -> {file_name}", 1, self.cfg.dblog_path)


            # Delete All Text Files In Folder
            self.__delete_files(".txt", self.cfg.foldr_path)



# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    pass
