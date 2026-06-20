# Name:                                            Renacin Matadeen
# Date:                                               05/30/2026
# Title                                      Main Logic Of Data Visualizer
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import sys
import sqlite3
import pandas as pd
import time as time
from datetime import datetime

from Functions.env_config  import Config
from Functions.data_helper import shared_logger
# ----------------------------------------------------------------------------------------------------------------------


class Visualizer():
    """ This class will create visualizations from the data collected  """

    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):
        """ On Instantiation Pull Config Settings """
        # Grab Config Files
        self.cfg = Config()



    # -------------------- Public Function #1 ---------------------------------
    def visualize_all(self):
        """
        When Called This Function Will Visualize All Charts.
        """

        # Run Private Functions
        self.__visualize_logs()



    # -------------------- Private Function #1 ---------------------------------
    def __visualize_logs(self):
        """
        When Called This Function Will Visualize The Log Data From The Day Before
        """

        print("Hello World")




# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":

    # Create An Instance For Testing
    DataVisualizer = Visualizer()
    DataVisualizer.visualize_all()