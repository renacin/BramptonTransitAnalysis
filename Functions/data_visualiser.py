# Name:                                            Renacin Matadeen
# Date:                                               05/30/2026
# Title                                      Main Logic Of Data Visualizer
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import re
import sys
import shutil
import sqlite3
import pandas as pd
import time as time
from datetime import datetime, timedelta

from Functions.env_config  import Config
from Functions.data_helper import *
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
            import matplotlib
            MAPLOT_IMPORT = True
        except ImportError:
            MAPLOT_IMPORT = False


        # Run Private Functions
        if MAPLOT_IMPORT:
            self.__visualize_logs()
            self.__delete_old_graphics()

        else:
            shared_logger("Data Visualiser", "MatplotLib Not Installed", 2, self.cfg.dblog_path)




    # -------------------- Private Function #0 ---------------------------------
    def __delete_old_graphics(self):
        """
        When Called This Function Delete Old Graphics In The Graphics Folder, Keep Only The 5 Most Recent
        """

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
            shared_logger("Data Visualiser", "Deleted Graphics Folders That Didn't Match Convention", 1, self.cfg.dblog_path)


        # Only Keep 5 Most Recent Files
        all_folders = sorted(all_folders, reverse=True)
        if len(all_folders) > 5:
            for foldr_ in all_folders[5:]:
                shutil.rmtree(os.path.join(self.cfg.out_graphics_path, foldr_))

            # Deleted Old Folders
            shared_logger("Data Visualiser", "Deleted Old Graphics Folders", 1, self.cfg.dblog_path)




    # -------------------- Private Function #1 ---------------------------------
    def __visualize_logs(self):
        """
        When Called This Function Will Visualize The Log Data From The Day Before
        """

        # Import Needed Libaries
        import matplotlib
        matplotlib.use("Agg")
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

                # Read In Database Logs From The Day Before
                df = pd.read_sql_query(f"""SELECT DISTINCT 
                                                A.* 
                                            FROM DB_LOGS AS A 
                                            WHERE 1=1
                                                AND A.time_stamp >= '{dt_ystrd}' 
                                                AND A.time_stamp <  '{dt_today}' 
                                        """, conn)

                # If No Data Report No Need To Render Anything
                if len(df) > 0:

                    # Split Data Between Data Collection Logs & Database Operation Logs
                    df['row_count']   = range(len(df))
                    df["time_stamp"]  = pd.to_datetime(df["time_stamp"])
                    df                = df.sort_values(by='time_stamp').reset_index(drop=True)
                    dc_df             = df[(df["reporter"] == "Data Collector") & (df["warning_level"] == 1)].copy()
                    db_logs_df        = df[~df["row_count"].isin(dc_df["row_count"])].copy()

                    # Use Regex To Get Data Collection Points & Resample To 5 Minute Intervals
                    dc_df["new_rows"] = dc_df["info"].str.extract(r"->\s*(\d+)").astype(int)
                    per_bucket        = dc_df.set_index("time_stamp")["new_rows"].resample("5min").sum()

                    # Find Totals For Each Category
                    total_rows = per_bucket.sum()
                    warnings   = (db_logs_df["warning_level"] >= 2).sum()
                    n_events   = (db_logs_df["warning_level"] < 2).sum()
                    hours      = per_bucket.index.hour + per_bucket.index.minute / 60
                    values     = per_bucket.values

                    # We Need To Find Clusters Of When Outages Happened
                    df['event_status'] = 0
                    df["hours"] = df["time_stamp"].dt.hour + (df["time_stamp"].dt.minute / 60)
                    df.loc[(df['reporter'] == "Data Collector") & (df['warning_level'] == 2), "event_status"] = 1

                    # Find All Starts & Ends (0 --> 1) and (1 --> 0)
                    starts = (df['event_status'].diff()   ==  1)
                    ends   = (df['event_status'].diff(-1) == -1)

                    # Get The Indices From Each List
                    start_idx = df.index[starts].tolist()
                    end_idx   = df.index[ends].tolist()

                    # Convert cluster boundaries from row indices to decimal hours
                    start_hours = df.loc[start_idx, "hours"].tolist()
                    end_hours   = df.loc[end_idx, "hours"].tolist()

                    # Create Rolling Average
                    rolling = per_bucket.rolling(window=6, center=True).mean()

                    # Plot Everything (12 x 6 Inches)
                    fig, ax = plt.subplots(figsize=(12, 6), layout='tight')
                    ax.scatter(hours, values, marker="x", alpha=0.5, color="gray", label=f"{total_rows:,} Rows Collected")
                    ax.plot(hours, rolling.values, color="red", label="30-Min Rolling AVG")

                    # Draw Database Collection Errors (Outages) As Orange Spans
                    for x, y in zip(start_hours, end_hours):
                        ax.axvspan(x, y, color='orange', alpha=0.3)

                    # Draw Non-Warning Database Events As Gray Dashed Lines
                    for ev in db_logs_df.itertuples():
                        if ev.warning_level >= 2:
                            continue  # warnings are drawn as spans above, skip here
                        ev_hour = ev.time_stamp.hour + ev.time_stamp.minute / 60
                        ax.axvline(ev_hour, linestyle="--", color="gray", alpha=0.5)

                    # Axis Styling: Time-Of-Day X-Axis, Data Fills The Plot
                    ax.set_title(f"Data Collected & Database Events \n Date: {dt_ystrd}")
                    ax.set_ylabel("# Bus Locations Collected")
                    ax.set_xlabel("Time")
                    ax.set_xticks(range(0, 25, 3))
                    ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 3)])
                    ax.set_xlim(0, 24)
                    ax.set_ylim(0, 2000)

                    # Modify The Legend
                    handles, _ = ax.get_legend_handles_labels()
                    handles += [
                        Line2D([0], [0], color="gray",   ls="--", alpha=0.7, label=f"{n_events} Event(s)"),
                        Line2D([0], [0], color="orange",          alpha=0.7, label=f"{warnings} Warning(s)"),
                    ]
                    ax.legend(handles=handles)


                    # Make Sure Folder Exists
                    viz_export_path   = os.path.join(self.cfg.out_graphics_path, f"{dt_ystrd}")
                    if not os.path.isdir(viz_export_path):
                        os.makedirs(viz_export_path)

                    # Export Data
                    final_path   = os.path.join(viz_export_path, f"DataCollected_DatabaseEvents_{dt_ystrd}.png")
                    plt.savefig(final_path, dpi=150)
                    plt.close(fig)

                    # Make Note To Data Logger
                    shared_logger("Data Visualiser", "Created Logs Charts", 1, self.cfg.dblog_path)



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