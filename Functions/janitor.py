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
    def __init__(self, skp_dwnld=False):
        """ This function will run when the DataCollector Class is instantiated """

        # Datetime Variables
        self.td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
        self.td_s_dt_dsply_frmt = "%d-%m-%Y"

        # Find What Operating System And Create A Temp Working Space In Downloads Folder
        self.__find_op_sys()
        self.__find_downloads_folder()
        self.__define_paths()



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
        """ Given The Validity Of Downloads Folder Define Needed Paths For Each Folder (OS Specific)"""

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





# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    janitor = Janitor()
