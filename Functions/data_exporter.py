# Name:                                            Renacin Matadeen
# Date:                                               05/30/2026
# Title                                      Main Logic Of Data Exporter
#
# ----------------------------------------------------------------------------------------------------------------------
import sys
import sqlite3
import pandas as pd
import time as time
from datetime import datetime

from env_config import Config
from data_helper import shared_logger
# ----------------------------------------------------------------------------------------------------------------------


class Exporter():
    """ This class will collect data from GTFS Live Feed  """

    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):
        """ On Instantiation Pull Config Settings """
        # Grab Config Files
        self.cfg = Config()


    # -------------------- Public Function #1 ---------------------------------
    def export_all(self):
        """
        When Called This Function Will Export All Old Data From Database Into CSVs In Respective Folders. 
        This function is simply a shell running private functions that will seperatly focus on
            Exporting:
                + Entire Table Containing Collected Bus Locations (Daily)
                + All GTFS Feed Data                              (If Old Data Is Detected - More Than 1 Cycle Old)
                + Calculated Speed Table                          (If Old Data Is Detected - More Than 1 Cycle Old)
                + LOG Table                                       (Daily)
        """

        # Try To Hold A Lock On The Database
        with sqlite3.connect(self.cfg.db_path, timeout=120, isolation_level=None) as conn:
            try:
                # Set PRAGMAs BEFORE any transaction, while no lock is held
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=120000")

                # Now manually begin the exclusive transaction
                conn.execute("BEGIN EXCLUSIVE")
                shared_logger("Data Exporter", "Exclusive Lock Obtained", 1, self.cfg.db_path)

                # --- export work goes here ---
                print("Did This Shit Work?")

                conn.execute("COMMIT")
                shared_logger("Data Exporter", "Exclusive Lock Released", 1, self.cfg.db_path)




            # If Something Happens Rollback To Begin, Inform User, And Wait
            except sqlite3.IntegrityError as e:
                conn.execute("ROLLBACK")
                shared_logger("Data Collector", f"Duplicate Key Error: {e}", 2, self.cfg.db_path)
                time.sleep(self.cfg.timeout_time * 2)

            except sqlite3.OperationalError as e:
                conn.execute("ROLLBACK")
                shared_logger("Data Collector", f"Database Operational Error: {e}", 2, self.cfg.db_path)
                time.sleep(self.cfg.timeout_time * 2)

            except KeyboardInterrupt:
                conn.execute("ROLLBACK")
                shared_logger("Data Collector", f"Keyboard Interrupt", 3, self.cfg.db_path)
                sys.exit()

            except Exception as e:
                conn.execute("ROLLBACK")
                shared_logger("Data Collector", f"Unexpected Error: {e}", 2, self.cfg.db_path)
                time.sleep(self.cfg.timeout_time * 2)




# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    exporter = Exporter()
    exporter.export_all()

