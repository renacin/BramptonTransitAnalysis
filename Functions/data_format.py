# Name:                                            Renacin Matadeen
# Date:                                               07/10/2026
# Title                                      Main Logic Of Data Formatter
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import sys
import sqlite3
import pandas as pd
import time as time
from datetime import datetime

from Functions.env_config  import Config
from Functions.data_helper import shared_logger, time_it
# ----------------------------------------------------------------------------------------------------------------------


class Formatter():
    """ This class will format & clean data collected. This creates Silver Layer Data  """

    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):
        """ On Instantiation Pull Config Settings """
        # Grab Config Files
        self.cfg = Config()


    # -------------------- Public Function #1 ---------------------------------
    def format_all(self):
        """
        When Called This Function Will Format & Clean Appropriate Data.
        """

        # Run Private Functions
        print("Hello World")


# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    
    # For Testing
    MainFormatter = Formatter()
    MainFormatter.format_all()


