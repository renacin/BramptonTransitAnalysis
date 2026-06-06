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
from data_helper import shared_logger
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
                shared_logger("Data Collector", f"Rate Limit Exceeded (HTTP 429)", 2, self.cfg.dblog_path)
                df = pd.DataFrame()

            else:
                r.raise_for_status()
                data = r.json()
                
                if 'entity' not in data:
                    shared_logger("Data Collector", f"Unexpected Response Format: {list(data.keys())}", 2, self.cfg.dblog_path)
                    df = pd.DataFrame()
                else:
                    df = pd.json_normalize(data['entity'])


        # Catch All Errors
        except requests.exceptions.JSONDecodeError:
            shared_logger("Data Collector", f"Invalid JSON Response From Feed", 2, self.cfg.dblog_path)
            df = pd.DataFrame()

        except requests.exceptions.Timeout:                 
            shared_logger("Data Collector", f"Connection Timed Out After {self.cfg.timeout_time}s", 2, self.cfg.dblog_path)
            df = pd.DataFrame()
            
        except requests.exceptions.ConnectionError:
            shared_logger("Data Collector", f"Endpoint Connection Failed", 2, self.cfg.dblog_path)
            df = pd.DataFrame()
            
        except requests.exceptions.HTTPError as e:
            shared_logger("Data Collector", f"HTTP Error {e}", 2, self.cfg.dblog_path)
            df = pd.DataFrame()

        except KeyboardInterrupt:
            shared_logger("Data Collector", f"Keyboard Interrupt", 3, self.cfg.dblog_path)
            sys.exit()



        # ----------------------------------------------------------------------------------------
        # Check To See If GTFS Data Is Empty. If It Is Rate Limit Code Here So We Don't Get Banned
        if len(df) == 0:
            shared_logger("Data Collector", f" ^^^ Skipping Data Collection For {self.cfg.timeout_time}s", 2, self.cfg.dblog_path)
            time.sleep(self.cfg.timeout_time)


        # If The Dataframe Isn't Empty Format It And Get It Reeady For Injesting Into Database Table
        else:

            # Rename Columns, Every Column Has "vehicle." In It's Name - Easiest Way Is To Remove The First 8 Characters
            df.columns      = [x.replace(".", "_") for x in [x[8:] if "vehicle." in x else x for x in df.columns]]

            # Create A Datetime So We Know The Exact Time In Human Readable Rather Than Timestamp From EPOCH, Create A Unique ID To Track Entry
            df["timestamp"] = pd.to_numeric(df["timestamp"],  errors='coerce')
            df["dt_colc"]   = pd.to_datetime(df["timestamp"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')
            df["u_id"]      = df["trip_route_id"] + "_" + df["vehicle_id"] + "_" + df["timestamp"].astype(str)


        with sqlite3.connect(self.cfg.db_path, timeout=30, isolation_level=None) as conn:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=30000")

                # --- Phase 1: Let pandas write LOC_TEMP freely (no active transaction) ---
                df.to_sql('LOC_TEMP', conn, if_exists='replace', index=False)

                # --- Phase 2: Grab lock for all critical writes ---
                conn.execute("BEGIN IMMEDIATE")

                conn.execute("""
                    INSERT INTO BUS_LOC_DB(id, u_id, trip_trip_id, 
                                        trip_schedule_relationship, trip_route_id, 
                                        position_latitude, position_longitude, position_bearing, 
                                        position_speed, current_status, timestamp, 
                                        stop_id, vehicle_id, vehicle_label, dt_colc)
                    SELECT id, u_id, trip_trip_id, 
                        trip_schedule_relationship, trip_route_id, position_latitude, 
                        position_longitude, position_bearing, position_speed, 
                        current_status, timestamp, stop_id, 
                        vehicle_id, vehicle_label, dt_colc
                    FROM LOC_TEMP
                    WHERE NOT EXISTS (SELECT 1 FROM U_ID_TEMP WHERE U_ID_TEMP.u_id = LOC_TEMP.u_id)
                """)

                new_rows_inserted = conn.execute("SELECT changes()").fetchone()[0]
                conn.execute("DROP TABLE IF EXISTS LOC_TEMP")

                # Build updated U_ID cache in Python, then write with explicit SQL (NOT to_sql)
                all_uids = pd.read_sql_query("SELECT * FROM U_ID_TEMP", conn)
                all_uids = pd.concat([all_uids, df[["u_id", "timestamp"]]])
                all_uids["timestamp"] = all_uids["timestamp"].astype('int')
                all_uids = all_uids.sort_values(by="timestamp", ascending=False).drop_duplicates()
                max_timestamp = all_uids["timestamp"].max() - (self.cfg.cache_time_limit * 60)
                all_uids = all_uids[all_uids["timestamp"] >= max_timestamp]

                # Write U_ID_TEMP with explicit SQL instead of to_sql — keeps pandas out of the transaction
                conn.execute("DELETE FROM U_ID_TEMP")
                conn.executemany(
                    "INSERT INTO U_ID_TEMP(u_id, timestamp) VALUES (?, ?)",
                    all_uids[["u_id", "timestamp"]].values.tolist()
                )

                conn.execute("COMMIT")
                shared_logger("Data Collector", f"New Bus Locations Processed --> {new_rows_inserted:04}", 1, self.cfg.dblog_path)
                time.sleep(self.cfg.timeout_time)

            except sqlite3.OperationalError as e:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass  # Already committed, nothing to roll back
                shared_logger("Data Collector", f"Database Operational Error: {e}", 2, self.cfg.dblog_path)
                time.sleep(self.cfg.timeout_time * 2)

            except sqlite3.IntegrityError as e:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass  # Already committed, nothing to roll back
                shared_logger("Data Collector", f"Duplicate Key Error: {e}", 2, self.cfg.dblog_path)
                time.sleep(self.cfg.timeout_time * 2)

            except KeyboardInterrupt:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass  # Already committed, nothing to roll back
                shared_logger("Data Collector", f"Keyboard Interrupt", 3, self.cfg.dblog_path)
                sys.exit()

            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass  # Already committed, nothing to roll back
                shared_logger("Data Collector", f"Unexpected Error: {e}", 2, self.cfg.dblog_path)
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
