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
from datetime import datetime, timedelta

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

        # Make Sure MatplotLib Is Installed
        try:
            import matplotlib.pyplot as plt
            MAPLOT_IMPORT = True
        except ImportError:
            MAPLOT_IMPORT = False


        # Run Private Functions
        if MAPLOT_IMPORT:
            self.__visualize_logs()

        else:
            print("No MatplotLib")



    # -------------------- Private Function #1 ---------------------------------
    def __visualize_logs(self):
        """
        When Called This Function Will Visualize The Log Data From The Day Before
        """
        import matplotlib.pyplot as plt

        # Get Current Date & Day Before
        dt_ystrd = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")
        dt_today =  datetime.now().strftime("%Y-%m-%d")


        # Make Connection TO Log Database
        with sqlite3.connect(self.cfg.dblog_path, timeout=30, isolation_level=None) as conn:

            # Set PRAGMAs BEFORE Any Transactions, This Is Not An Urgent Connection
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")

            try:
                # Read In Database Logs From The Day Before
                df = pd.read_sql_query(f"""SELECT DISTINCT 
                                                A.* 
                                           FROM DB_LOGS AS A 
                                           WHERE 1=1
                                                AND A.time_stamp >= '{dt_ystrd}' 
                                                AND A.time_stamp <  '{dt_today}' 
                                       """, conn)
  
                # Create An Index To Split Data
                df['row_count'] = range(len(df))

                # Seperate Out: Data Collection Logs
                dc_df = df[(df["reporter"] == "Data Collector") & (df["warning_level"] == 1)]

                # Seperate Out: All Other Data
                db_logs_df = df[~df["row_count"].isin(dc_df["row_count"])]


                dc_df["time_stamp"] = pd.to_datetime(dc_df["time_stamp"])
                dc_df["new_rows"] = dc_df["info"].str.extract(r"->\s*(\d+)").astype(int)
                per_bucket = dc_df.set_index("time_stamp")["new_rows"].resample("5min").sum()
                
                # --- Load operation logs as discrete events ---
                db_logs_df["time_stamp"] = pd.to_datetime(db_logs_df["time_stamp"])
                
                # --- Totals ---
                total_rows = per_bucket.sum()
                warnings = (db_logs_df["warning_level"] >= 2).sum()
                print(f"Rows collected: {total_rows}")
                print(f"Database warnings: {warnings}")
                
                # --- Plot ---
                fig, ax = plt.subplots(figsize=(12, 6))
                
                ax.plot(per_bucket.index, per_bucket.values, label="Rows / 5 min")
                
                for ev in db_logs_df.itertuples():
                    ax.axvline(ev.time_stamp, linestyle="--", color="gray", alpha=0.7)
                
                ax.set_title(f"Transit Pipeline Logs\n{total_rows:,} rows collected, {warnings} warnings")
                ax.set_ylabel("Rows collected / 5 min")
                ax.set_ylim(bottom=0)
                ax.legend()
                
                plt.tight_layout()
                plt.savefig(r"C:\Users\renac\Desktop\transit_logs.png", dpi=150)
                plt.show()  









            # If Something Happens Rollback To Begin, Inform User, And Wait
            except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Visualiser", f"Failed To Render Log Data: {e}", 2, self.cfg.dblog_path)

            except KeyboardInterrupt:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Visualiser", f"Keyboard Interrupt", 2, self.cfg.dblog_path)
                sys.exit()

            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                shared_logger("Data Visualiser", f"Failed To Render Log Data {e}", 2, self.cfg.dblog_path)






# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":

    # Create An Instance For Testing
    DataVisualizer = Visualizer()
    DataVisualizer.visualize_all()