# Name:                                            Renacin Matadeen
# Date:                                               05/18/2026
# Title                                      Main Logic Of Data Collector
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import sys
import sqlite3
import requests
import pandas as pd
import time as time
from datetime import datetime

from env_config import Config
from helper import shared_logger
# ----------------------------------------------------------------------------------------------------------------------


class Collector():
    """ This class will collect data from GTFS Live Feed  """

    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):
        """ On Instantiation Pull Config Settings """
        # Grab Config Files
        self.cfg = Config()


    # -------------------- Private Function #2 ---------------------------------
    def get_bus_loc(self):
        """
        When called, this function will navigate to Brampton Transit JSON GTFS URL and look for new bus locations.
        It will then upload those new bus locations to the appropriate database table.
        """

        # Try To Pull GTFS Data From Transit URL, Be Careful Of Request Errors
        try:
            r = requests.get(self.cfg.BUS_LOC_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=self.cfg.timeout_time)

            # Handle Rate Limiting
            if r.status_code == 429:
                shared_logger("Data Collector", f"Rate Limit Exceeded (HTTP 429)", 2, self.cfg.LOG_PATH)
                df = pd.DataFrame()

            else:
                r.raise_for_status()
                data = r.json()
                df = pd.json_normalize(data['entity'])


        # Catch All Errors
        except requests.exceptions.Timeout:                 
            shared_logger("Data Collector", f"Connection Timed Out After {self.cfg.timeout_time}s", 2, self.cfg.LOG_PATH)
            df = pd.DataFrame()
            
        except requests.exceptions.ConnectionError:
            shared_logger("Data Collector", f"Endpoint Connection Failed", 2, self.cfg.LOG_PATH)
            df = pd.DataFrame()
            
        except requests.exceptions.HTTPError as e:
            shared_logger("Data Collector", f"HTTP Error {e}", 2, self.cfg.LOG_PATH)
            df = pd.DataFrame()

        except KeyboardInterrupt:
            shared_logger("Data Collector", f"Keyboard Interrupt", 3, self.cfg.LOG_PATH)
            sys.exit()


        # ----------------------------------------------------------------------------------------
        # Check To See If GTFS Data Is Empty. If It Is Rate Limit Code Here So We Don't Get Banned
        if len(df) == 0:
            shared_logger("Data Collector", f" ^^^ Skipping Data Collection For {self.cfg.timeout_time}s", 2, self.cfg.LOG_PATH)
            time.sleep(self.cfg.timeout_time)


        # If The Dataframe Isn't Empty Format It And Get It Reeady For Injesting Into Database Table
        else:

            # Rename Columns, Every Column Has "vehicle." In It's Name - Easiest Way Is To Remove The First 8 Characters
            df.columns      = [x.replace(".", "_") for x in [x[8:] if "vehicle." in x else x for x in df.columns]]

            # Create A Datetime So We Know The Exact Time In Human Readable Rather Than Timestamp From EPOCH, Create A Unique ID To Track Entry
            df["timestamp"] = pd.to_numeric(df["timestamp"],  errors='coerce')
            df["dt_colc"]   = pd.to_datetime(df["timestamp"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')
            df["u_id"]      = df["trip_route_id"] + "_" + df["vehicle_id"] + "_" + df["timestamp"].astype(str)


            # Upload New Data To An Intermediary Temp Table, Check If The U_IDs Are In A Cache From 10 Min Ago, If Not Add To Database
            with sqlite3.connect(self.cfg.db_path) as conn:

                # Wrap With Error Handling Just In Case
                try: 

                    # Create A Temporary Space To Store Data Pulled
                    df.to_sql('LOC_TEMP', conn, if_exists='replace', index=False)
                
                    # Compare Data U_IDs From New Data Pulled To U_ID Cache Of X Minutes Ago, Only Look For New Data
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO BUS_LOC_DB(id, u_id, trip_trip_id, 
                                            trip_schedule_relationship, 
                                            trip_route_id, position_latitude, position_longitude, 
                                            position_bearing, position_speed, current_status, 
                                            timestamp, stop_id, vehicle_id, 
                                            vehicle_label, dt_colc)
                        
                        SELECT id, u_id, trip_trip_id, 
                               trip_schedule_relationship, trip_route_id, position_latitude, 
                               position_longitude, position_bearing, position_speed, 
                               current_status, timestamp, stop_id, 
                               vehicle_id, vehicle_label, dt_colc
                                
                        FROM LOC_TEMP
                        WHERE NOT EXISTS (SELECT 1 FROM U_ID_TEMP WHERE U_ID_TEMP.u_id = LOC_TEMP.u_id)
                    """)

                    # Drop The Temporary Table Where We Stored The Newly Collected Data & Capture Rows Added
                    new_rows_inserted = cursor.rowcount
                    conn.execute('DROP TABLE IF EXISTS LOC_TEMP')

                    # Update The U_ID Cache - Combine U_IDs From New Data & U_IDs In Most Recent Cache
                    all_uids = pd.concat([pd.read_sql_query("SELECT * FROM U_ID_TEMP", conn), df[["u_id", "timestamp"]]])

                    # Sort, Where The Most Recent U_IDs Are At The Top, Remove Duplicates
                    all_uids["timestamp"] = all_uids["timestamp"].astype('int')
                    all_uids = all_uids.sort_values(by="timestamp", ascending=False)
                    all_uids = all_uids.drop_duplicates()

                    # Find The Max Time Stamp, And Only Keep Rows A Couple Of Min Back From That Value
                    max_timestamp = all_uids["timestamp"].max() - (self.cfg.cache_time_limit * 60)
                    all_uids = all_uids[all_uids["timestamp"] >= max_timestamp]

                    # Update the U_ID cache with filtered data
                    all_uids.to_sql('U_ID_TEMP', conn, if_exists='replace', index=False)

                    # Save All Changes To The Database
                    conn.commit()

                    # Update User
                    shared_logger("Data Collector", f"New Bus Locations Processed --> {new_rows_inserted:04}", 1, self.cfg.LOG_PATH)
                    time.sleep(self.cfg.timeout_time)


                # If Something Happens Rollback To Begin, Inform User, And Wait
                except sqlite3.IntegrityError as e:
                    conn.rollback()
                    shared_logger("Data Collector", f"Duplicate Key Error: {e}", 2, self.cfg.LOG_PATH)
                    time.sleep(self.cfg.timeout_time * 2)

                except sqlite3.OperationalError as e:
                    conn.rollback()
                    shared_logger("Data Collector", f"Database Operational Error: {e}", 2, self.cfg.LOG_PATH)
                    time.sleep(self.cfg.timeout_time * 2)

                except KeyboardInterrupt:
                    conn.rollback()
                    shared_logger("Data Collector", f"Keyboard Interrupt", 3, self.cfg.LOG_PATH)
                    sys.exit()

                except Exception as e:
                    conn.rollback()
                    shared_logger("Data Collector", f"Unexpected Error: {e}", 2, self.cfg.LOG_PATH)
                    time.sleep(self.cfg.timeout_time * 2)



# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    collector = Collector()

    while True:
        try:
            collector.get_bus_loc()

        except KeyboardInterrupt:
            sys.exit()
