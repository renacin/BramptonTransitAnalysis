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

        # Import Needed Libaries
        import matplotlib.pyplot as plt
        from matplotlib.lines import Line2D

        # Get Current Date & Day Before
        dt_ystrd = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")
        dt_today =  datetime.now().strftime("%Y-%m-%d")

        # Make Connection TO Log Database
        with sqlite3.connect(self.cfg.dblog_path, timeout=30, isolation_level=None) as conn:

            try:
                # Set PRAGMAs BEFORE Any Transactions, This Is Not An Urgent Connection
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=30000")

                # try:
                # Read In Database Logs From The Day Before
                df = pd.read_sql_query(f"""SELECT DISTINCT 
                                                A.* 
                                            FROM DB_LOGS AS A 
                                            WHERE 1=1
                                                AND A.time_stamp >= '{dt_ystrd}' 
                                                AND A.time_stamp <  '{dt_today}' 
                                        """, conn)

                # Split Data Between Data Collection Logs & Database Operation Logs
                df['row_count']   = range(len(df))
                df["time_stamp"]  = pd.to_datetime(df["time_stamp"])
                dc_df             = df[(df["reporter"] == "Data Collector") & (df["warning_level"] == 1)].copy()
                db_logs_df        = df[~df["row_count"].isin(dc_df["row_count"])].copy()
                del df


                # Use Regex To Get Data Collection Points & Resample To 5 Minute Intervals
                dc_df["new_rows"] = dc_df["info"].str.extract(r"->\s*(\d+)").astype(int)
                per_bucket        = dc_df.set_index("time_stamp")["new_rows"].resample("5min").sum()


                # Find Totals For Each Category
                total_rows = per_bucket.sum()
                warnings   = (db_logs_df["warning_level"] >= 2).sum()
                n_events   = (db_logs_df["warning_level"] < 2).sum()
                hours      = per_bucket.index.hour + per_bucket.index.minute / 60
                values     = per_bucket.values

                # Create Rolling Average
                rolling = per_bucket.rolling(window=6, center=True).mean()

                # Plot Everything
                fig, ax = plt.subplots(figsize=(12, 6))
                ax.scatter(hours, values, marker="x", alpha=0.5, color="gray", label=f"{total_rows:,} Rows Collected")
                ax.plot(hours, rolling.values, color="red", label="30-Min Rolling AVG")

                # Loop Through Each Event In The DB Event Database
                for ev in db_logs_df.itertuples():

                    # Find The Hour (In Decimal Format), Find The Colour, Make The Line
                    ev_hour = ev.time_stamp.hour + ev.time_stamp.minute / 60
                    if ev.warning_level >= 2: color = "orange" 
                    else:                     color ="gray"
                    ax.axvline(ev_hour, linestyle="--", color=color, alpha=0.2)

                # axis styling: time-of-day x-axis, data fills the plot
                ax.set_title(f"Database Logs — {dt_ystrd}")
                ax.set_ylabel("Data Collected")
                ax.set_xlabel("Time")
                ax.set_xticks(range(0, 25, 3))
                ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 3)])
                ax.set_xlim(0, 24)
                ax.set_ylim(0, per_bucket.max() + 50)

                # Modify The Legend
                handles, _ = ax.get_legend_handles_labels()
                handles += [Line2D([0], [0], color="gray",   ls="--", alpha=0.7, label=f"{n_events} Event(s)"),
                            Line2D([0], [0], color="orange", ls="--", alpha=0.7, label=f"{warnings} Warning(s)"),
                            ]
                ax.legend(handles=handles)

                plt.tight_layout()
                plt.savefig(r"C:\Users\renac\Desktop\Testing.png", dpi=150)
                plt.show()



            # If Something Happens Rollback To Begin, Inform User, And Wait
            except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                shared_logger("Data Visualiser", f"Failed To Render Log Data: {e}", 2, self.cfg.dblog_path)

            except KeyboardInterrupt:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                shared_logger("Data Visualiser", "Keyboard Interrupt", 2, self.cfg.dblog_path)
                return

            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                shared_logger("Data Visualiser", f"Failed To Render Log Data: {e}", 2, self.cfg.dblog_path)






# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":

    # Create An Instance For Testing
    DataVisualizer = Visualizer()
    DataVisualizer.visualize_all()