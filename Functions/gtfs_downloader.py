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

from data_helper import *
from env_config import Config
# ----------------------------------------------------------------------------------------------------------------------


class GTFS_Downloader():
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
        self.__dblog_check()
        self.__db_check()
        self.__get_gtfs_data()
        self.__upld_gtfs_data()
        self.__upld_trip_avg_speed()



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
                    raise f"[ERROR] Could Not Remove {file_ext} Files"



    # -------------------- Private Function #3 ---------------------------------
    def __find_downloads_folder(self):
        """ Find The Location Of The Downloads Folder """

        # Verify That The Path Exists Raise Error!
        if os.path.exists(self.cfg.dwnld_path) != True:
            raise f"[ERROR] Download Folder Does Not Exist"



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


    # -------------------- Private Function #5 ---------------------------------
    def __dblog_check(self):
        """ Create a database that will store all log and status information of the main data collection database """

        # Iterate Through Table Dictionary And Create Tables If They Don't Exist Already
        with sqlite3.connect(self.cfg.dblog_path) as conn:

            # Make Sure The Database Is In WAL Mode To Allow For Concurrent Writes & Read | Verify It Worked
            conn.execute("PRAGMA journal_mode=WAL")
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]

            # Make Needed Tables
            for table_ in self.cfg.log_dict:
                sql_string = ", ".join(self.cfg.log_dict[table_])
                conn.execute(f'''CREATE TABLE IF NOT EXISTS {table_} ({sql_string});''')
                conn.commit()

        # Log export
        shared_logger("Data Janitor  ", f"Log Database Ready", 1, self.cfg.dblog_path)



    # -------------------- Private Function #5 ---------------------------------
    def __db_check(self):
        """ Create a database that will store bus location data; as well as basic database inter data """

        # Iterate Through Table Dictionary And Create Tables If They Don't Exist Already
        with sqlite3.connect(self.cfg.db_path) as conn:

            # Make Sure The Database Is In WAL Mode To Allow For Concurrent Writes & Read | Verify It Worked
            conn.execute("PRAGMA journal_mode=WAL")
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]

            # Make Needed Tables
            for table_ in self.cfg.table_dict:
                sql_string = ", ".join(self.cfg.table_dict[table_])
                conn.execute(f'''CREATE TABLE IF NOT EXISTS {table_} ({sql_string});''')
                conn.commit()

        # Log export
        shared_logger("Data Janitor  ", f"Data Database Ready", 1, self.cfg.dblog_path)


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
                shared_logger("Data Janitor  ", f"Extracted GTFS Data", 1, self.cfg.dblog_path)

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
            raise e



# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    pass
