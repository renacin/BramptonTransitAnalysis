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
from datetime import datetime, timedelta

from Functions.env_config  import Config
from Functions.data_helper import shared_logger
# ----------------------------------------------------------------------------------------------------------------------


class Exporter():
    """ This class will collect & export data bus location data & export it to appropriate folders. This is considered Bronze Layer data  """

    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):
        """ On Instantiation Pull Config Settings """
        # Grab Config Files
        self.cfg = Config()



    # -------------------- Public Function #1 ---------------------------------
    def export_all(self):
        """
        When Called This Function Will Export All Old Data & Clean Pertinent Tables.
        """

        # Run Private Functions
        # self.__export_bus_locs()
        # self.__export_old_gtfs()
        self.__transform_rawdata()



    # -------------------- Private Function #1 ---------------------------------
    def __export_bus_locs(self):
        """
        When Called This Function Will Export All Old Data From Database That Looks At Bus Locations.
        This Function Should Run Daily.
        """

        # Define Needed Variables
        dt_nw = datetime.now().strftime(self.cfg.td_xl_dt_dsply_frmt)
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
                df.to_csv(bus_locs_out_path, index=False)
                shared_logger("Data Exporter", f"Exporting {len(df)} Rows", 1, self.cfg.dblog_path)

                # Delete All Data & Vacuum Database
                conn.execute("""DELETE FROM BUS_LOC_DB""")
                conn.execute("COMMIT")
                conn.execute("VACUUM")
                shared_logger("Data Exporter", f"Exported All Bus Locations", 1, self.cfg.dblog_path)


            # If Something Happens Rollback To Begin, Inform User, And Wait
            except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Exporter", f"Bus Location Export cleanup failed: {e}", 2, self.cfg.dblog_path)

            except KeyboardInterrupt:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Exporter", f"Keyboard Interrupt", 2, self.cfg.dblog_path)
                sys.exit()

            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Exporter", f"Bus Location Export cleanup failed: {e}", 2, self.cfg.dblog_path)



    # -------------------- Private Function #2 ---------------------------------
    def __export_old_gtfs(self):
        """
        When Called This Function Will Export All Old GTFS Data In The Database. Including The Current FEED_VERSION Only Keep 2 Current Feed Version. 
        Export Everything Else.
        """
        
        # Make A Connecion To The Data Collection Database - It Must be Exclusive As We Are Exporting & Cleaning The Database
        with sqlite3.connect(self.cfg.db_path, timeout=120, isolation_level=None) as conn:

            # Set PRAGMAs BEFORE any transaction, while no lock is held
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=120000")

            try:
                # Read Each GTFS Dataset That Has FEED_VERSION In It.
                for table_ in self.cfg.table_dict:
                    if table_ not in self.cfg.NOT_FEED_BASED:

                        # We Need To Find Rows Where The Feed Version Is 2 Cycles Older Than The Current
                        df     = pd.read_sql_query(f"""SELECT DISTINCT feed_version FROM {table_}""", conn)
                        dates_ = [int(x) for x in df["feed_version"].tolist()]
                        dates_.sort(reverse=True)

                        # Get All Data Older Than The Second Entry
                        if len(dates_) > 1:

                            # Get Path Name
                            dt_nw = datetime.now().strftime(self.cfg.td_xl_dt_dsply_frmt)
                            out_path = os.path.join(self.cfg.csv_out_path, table_, f"{table_}_{dt_nw}.csv")

                            # Pull All Data & Write To Appropriate Folder
                            df = pd.read_sql_query(f"""SELECT * FROM {table_} WHERE CAST(feed_version AS INTEGER) < CAST({str(dates_[1])} AS INTEGER)""", conn)
                            df.to_csv(out_path, index=False)
                            shared_logger("Data Exporter", f"Exported Old {table_} Data", 1, self.cfg.dblog_path)

                            # Delete All Data & Vacuum Database
                            conn.execute("BEGIN IMMEDIATE")
                            conn.execute(f"""DELETE FROM {table_} WHERE CAST(feed_version AS INTEGER) < CAST({str(dates_[1])} AS INTEGER)""")
                            conn.execute("COMMIT")
                            conn.execute("VACUUM")
                            shared_logger("Data Exporter", f"Cleaned Old {table_} Data", 1, self.cfg.dblog_path)


            # If Something Happens Rollback To Begin, Inform User, And Wait
            except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Exporter", f"Failed To Clean Up {table_}: {e}", 2, self.cfg.dblog_path)

            except KeyboardInterrupt:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Exporter", f"Keyboard Interrupt", 2, self.cfg.dblog_path)
                sys.exit()

            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Exporter", f"Failed To Clean Up {table_}: {e}", 2, self.cfg.dblog_path)




    # -------------------- Private Function #3 ---------------------------------
    def __transform_rawdata(self):
        """
        When Called This Function Will Transform Raw Observations Of Bus Locations (Raw CSVs Created From Database - Bronze Layer) 
        Into Cleaned Data, Free Of Duplicates, Errors, Etc.. (Silver Layer) Ready To Be Used For Analytics
        """

        #TODO: ADD SAFETY CHECKS, RIGHT FILE TYPE, WHAT HAPPENS IF NO FILES, TOO FEW FILES? START CLEAN UP OF BRONZE LAYER DATA, WHAT DO WE NEED TO DO TO GET TO SILVER?

        # Read CSV Storage Folder, Add Sanity Check For Right CSVs
        csv_path      = os.path.join(self.cfg.csv_out_path, "BUS_LOC_DB")
        all_raw       = [file_ for file_ in list(os.listdir(csv_path)) if file_[:10] == "BUS_LOC_DB"]

        # Get Current Date & Day Before
        dt_ystrd         = (datetime.now() - timedelta(days = 1)).strftime("%d-%m-%Y")
        dt_ystrd_m1_f1   = (datetime.now() - timedelta(days = 2)).strftime("%d-%m-%Y")
        dt_ystrd_m1_f2   = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")

        print(dt_ystrd, dt_ystrd_m1_f1, dt_ystrd_m1_f2)

        # # Focus On Files Needed For Silver Layer Data Product
        focus_raw_csv = [file_ for file_ in all_raw if file_[11:21] in [str(dt_ystrd), str(dt_ystrd_m1_f1)]]

        # Read All Data As A Pandas Dataframe, Only Focus On 1 Date For Data Cleaning (That's Why We Ingested More Data That We Needed)
        focus_raw_csv           = [os.path.join(csv_path, file_) for file_ in focus_raw_csv]
        all_raw                 = pd.concat([pd.read_csv(file_) for file_ in focus_raw_csv])
        all_raw['dt_colc']      = pd.to_datetime(all_raw['dt_colc'])
        all_raw['dt_colc_date'] = all_raw["dt_colc"].dt.strftime("%Y-%m-%d")
        all_raw                 = all_raw[all_raw["dt_colc_date"] == dt_ystrd_m1_f2]











# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    
    #pass

    # For Testing Remove Once Finished
    exprt = Exporter()
    exprt.export_all()