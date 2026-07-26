# Name:                                            Renacin Matadeen
# Date:                                               07/26/2026
# Title                                      Main Logic Of Data Exporter
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import re
import shutil
import time as time

from Functions.env_config  import Config
from Functions.data_helper import *

from contextlib import closing
from pathlib import Path
# ----------------------------------------------------------------------------------------------------------------------


class Deleter():
    """ This class will delete old data from the main storage folders  """

    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):
        """ On Instantiation Pull Config Settings """
        # Grab Config Files
        self.cfg = Config()



    # -------------------- Public Function #1 ---------------------------------
    def delete_all(self):
        """
        When Called This Function Delete From All Pertinent Folders
        """

        # Run All Private Functions
        self.__delete_old_graphics()
        self.__delete_raw_bus_locs()



    # -------------------- Private Function #1 ---------------------------------
    def __delete_old_graphics(self):
        """
        When Called This Function Delete Old Graphics In The Graphics Folder, Keep Only The 7 Most Recent
        """

        # Items 2 Keep?
        num_graphcs_2_keep = 7

        # All Content In Graphics Folder
        all_folders = list(os.listdir(self.cfg.out_graphics_path))

        # Remove Folder That Don't Match Date Naming Standard
        date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        not_dates    = [item for item in all_folders if not date_pattern.match(item)]
        all_folders  = [folder_ for folder_ in all_folders if folder_ not in not_dates]


        # Delete Folders That Don't Match Naming Convention
        if len(not_dates) > 0:
            for foldr_ in not_dates:
                shutil.rmtree(os.path.join(self.cfg.out_graphics_path, foldr_))
            shared_logger("Data Deleter", "Deleted Graphics Folders That Didn't Match Convention", 1, self.cfg.dblog_path)


        # Only Keep X Most Recent Files
        all_folders = sorted(all_folders, reverse=True)
        if len(all_folders) > num_graphcs_2_keep:
            for foldr_ in all_folders[num_graphcs_2_keep:]:
                shutil.rmtree(os.path.join(self.cfg.out_graphics_path, foldr_))

            # Deleted Old Folders
            shared_logger("Data Deleter", f"Deleted {len(all_folders[num_graphcs_2_keep:])} Old Graphics Folders", 1, self.cfg.dblog_path)



    # -------------------- Private Function #2 ---------------------------------
    def __delete_raw_bus_locs(self):
        """
        When Called This Function Delete Old CSVs Containing Raw Bus Location Data
        """
        # NOTE FOR THE FUTURE, COMPUTERS SORT BEST WITH YYYYMMDD

        # Items 2 Keep?
        num_raw_csvs_2_keep = 30

        # How Many Files In The Raw Bus Loc Folder, How Many CSVs That Fall Within Scope
        csv_path      = os.path.join(self.cfg.csv_out_path, "BUS_LOC_DB")
        all_files     = [str(x.name) for x in Path(csv_path).glob("*")]
        all_csvs      = [str(x.name) for x in sorted(Path(csv_path).glob("BUS_LOC_DB_*.csv"), key=os.path.getmtime)[-num_raw_csvs_2_keep:]]

        # Only Do Work If There Is Data In The Folders
        if len(all_files) > 0:

            # Files To Drop
            to_drop = list(set(all_files) - set(all_csvs))

            # Iterate Through Each File & Delete Them
            for file_ in to_drop:
                path_ = os.path.join(csv_path, file_)
                os.remove(path_) 

            # Update Logger
            shared_logger("Data Deleter", f"Deleted {len(to_drop)} Old Raw Bus Locations", 1, self.cfg.dblog_path)






# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    pass
