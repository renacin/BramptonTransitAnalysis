# Name:                                            Renacin Matadeen
# Date:                                               05/30/2026
# Title                                      Main Logic Of Data Exporter
#
# ----------------------------------------------------------------------------------------------------------------------
import os
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
    def export_bus_locs(self):
        """
        When Called This Function Will Export All Old Data From Database That Looks At Bus Locations.
        This Function Should Run Daily.
        """

        # Define Needed Variables
        dt_nw = datetime.now().strftime(self.cfg.td_s_dt_dsply_frmt)
        bus_locs_out_path = os.path.join(self.cfg.out_bus_loc_path, f"BUS_LOC_DB_{dt_nw}.csv")


        # Try To Hold A Lock On The Database
        with sqlite3.connect(self.cfg.db_path, timeout=120, isolation_level=None) as conn:
            # try:

            # Set PRAGMAs BEFORE any transaction, while no lock is held
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=120000")

            try:
                conn.execute("BEGIN IMMEDIATE")

                # Grab All Data & Export
                df = pd.read_sql_query("""SELECT * FROM BUS_LOC_DB""", conn)
                df.to_csv(bus_locs_out_path, index=False)

                # Delete All Data & Vacuum Database
                conn.execute("""DELETE FROM BUS_LOC_DB""")
                conn.execute("COMMIT")
                conn.execute("VACUUM")
                shared_logger("Data Exporter", f"Exported All Bus Locations", 1, self.cfg.dblog_path)

            except Exception as e:
                conn.execute("ROLLBACK")
                shared_logger("Data Exporter", f"Bus Location Export cleanup failed: {e}", 2, self.cfg.dblog_path)




            # # If Something Happens Rollback To Begin, Inform User, And Wait
            # except sqlite3.IntegrityError as e:
            #     conn.execute("ROLLBACK")
            #     shared_logger("Data Collector", f"Duplicate Key Error: {e}", 2, self.cfg.dblog_path)
            #     time.sleep(self.cfg.timeout_time * 2)

            # except sqlite3.OperationalError as e:
            #     conn.execute("ROLLBACK")
            #     shared_logger("Data Collector", f"Database Operational Error: {e}", 2, self.cfg.dblog_path)
            #     time.sleep(self.cfg.timeout_time * 2)

            # except KeyboardInterrupt:
            #     conn.execute("ROLLBACK")
            #     shared_logger("Data Collector", f"Keyboard Interrupt", 3, self.cfg.dblog_path)
            #     sys.exit()

            # except Exception as e:
            #     conn.execute("ROLLBACK")
            #     shared_logger("Data Collector", f"Unexpected Error: {e}", 2, self.cfg.dblog_path)
            #     time.sleep(self.cfg.timeout_time * 2)




# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    exporter = Exporter()
    exporter.export_bus_locs()

