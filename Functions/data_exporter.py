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

            # Set PRAGMAs BEFORE any transaction, while no lock is held
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=120000")

            try:
                conn.execute("BEGIN IMMEDIATE")

                # Grab All Data & Export
                df = pd.read_sql_query("""SELECT * FROM BUS_LOC_DB""", conn)
                shared_logger("Data Exporter", f"Exporting {len(df)} Rows: ", 1, self.cfg.dblog_path)
                df.to_csv(bus_locs_out_path, index=False)

                # Delete All Data & Vacuum Database
                conn.execute("""DELETE FROM BUS_LOC_DB""")
                conn.execute("COMMIT")
                conn.execute("VACUUM")
                shared_logger("Data Exporter", f"Exported All Bus Locations", 1, self.cfg.dblog_path)


            # If Something Happens Rollback To Begin, Inform User, And Wait
            except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                conn.execute("ROLLBACK")
                shared_logger("Data Exporter", f"Bus Location Export cleanup failed: {e}", 2, self.cfg.dblog_path)

            except KeyboardInterrupt:
                conn.execute("ROLLBACK")
                shared_logger("Data Exporter", f"Keyboard Interrupt", 2, self.cfg.dblog_path)
                sys.exit()

            except Exception as e:
                conn.execute("ROLLBACK")
                shared_logger("Data Exporter", f"Bus Location Export cleanup failed: {e}", 2, self.cfg.dblog_path)



    # -------------------- Public Function #2 ---------------------------------
    def export_old_gtfs(self):
        """
        When Called This Function Will Export All Old GTFS Data In The Database. Including The Current FEED_VERSION Only Keep 2 Current Feed Version. 
        Export Everything Else.
        """
        
        # Make A Connecion To The Datacollection Database - It Must be Exclusive As We Are Exporting & Cleaning The Database
        with sqlite3.connect(self.cfg.db_path, timeout=120, isolation_level=None) as conn:

            # Set PRAGMAs BEFORE any transaction, while no lock is held
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=120000")

            try:
                # Read Each GTFS Dataset That Has FEED_VERSION In It.
                for table_ in self.cfg.table_dict:
                    if table_ not in self.cfg.NOT_FEED_BASED:

                        # Grab A Lock
                        conn.execute("BEGIN IMMEDIATE")

                        # We Need To Find Rows Where The Feed Version Is 2 Cycles Older Than The Current
                        df = pd.read_sql_query(f"""SELECT DISTINCT feed_version FROM {table_}""", conn)
                        dates_ = [int(x) for x in df["feed_version"].tolist()]
                        dates_.append(20260530)
                        dates_.append(20260607)
                        dates_.sort(reverse=True)

                        # Get All Data Older Than The Second Entry
                        if len(dates_) != 1:
                            df = pd.read_sql_query(f"""SELECT * FROM {table_} WHERE CAST(feed_version AS INTEGER) < CAST({str(dates_[1])} AS INTEGER)""", conn)
                            df.to_csv(fr"""C:\Users\renac\Desktop\{table_}_EXPORT.csv""", index=False)
                            shared_logger("Data Exporter", f"Exported Old {table_} Data", 1, self.cfg.dblog_path)

                            # Delete All Data & Vacuum Database
                            conn.execute(f"""DELETE FROM {table_} WHERE CAST(feed_version AS INTEGER) < CAST({str(dates_[1])} AS INTEGER)""")
                            conn.execute("COMMIT")
                            conn.execute("VACUUM")
                            shared_logger("Data Exporter", f"Cleaned Old {table_} Data", 1, self.cfg.dblog_path)


            # If Something Happens Rollback To Begin, Inform User, And Wait
            except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                conn.execute("ROLLBACK")
                shared_logger("Data Exporter", f"Failed To Clean Up {table_}: {e}", 2, self.cfg.dblog_path)

            except KeyboardInterrupt:
                conn.execute("ROLLBACK")
                shared_logger("Data Exporter", f"Keyboard Interrupt", 2, self.cfg.dblog_path)
                sys.exit()

            except Exception as e:
                conn.execute("ROLLBACK")
                shared_logger("Data Exporter", f"Failed To Clean Up {table_}: {e}", 2, self.cfg.dblog_path)






# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    exporter = Exporter()
    exporter.export_old_gtfs()

