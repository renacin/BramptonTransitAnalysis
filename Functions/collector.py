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

# ----------------------------------------------------------------------------------------------------------------------



class Collector():
    """ This class will collect data from GTFS Live Feed  """


    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self, log_level = 0):
        """ This function will run when the DataCollector Class is instantiated """

        # Try To Create A Table For Each Item In The Following Database
        self.BUS_LOC_URL     = r'https://gtfs-rt-merge.prod.bt-cadavl.com/BramptonTransit/GTFS/merged_VehiclePosition.json'
        self.GATHER_TABLE    = {"BUS_LOC_DB", "U_ID_TEMP", "ERROR_DB", "ROUTE_SPEED"}
        self.table_dict      = {
            "BUS_LOC_DB":     ["u_id", "id", "is_deleted", "trip_update", "alert", "trip_id", "start_time", "start_date", "schedule_relationship", "route_id", "latitude", "longitude", "bearing", "odometer", "speed", "current_stop_sequence", "current_status", "timestamp", "congestion_level", "stop_id", "vehicle_id", "label", "license_plate", "dt_colc"],
            "U_ID_TEMP":      ["u_id", "timestamp"],
            "ERROR_DB":       ["timestamp", "e_type", "delay"],
            "FEED_INFO":      ["feed_publisher_name", "feed_lang", "feed_start_date", "feed_end_date", "feed_version"],
            "ROUTES":         ["route_id", "route_short_name", "route_long_name", "feed_version"],
            "TRIPS":          ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "block_id", "shape_id", "feed_version"],
            "STOPS":          ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "zone_id", "stop_url", "parent_station", "feed_version"],
            "STOP_TIMES":     ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "timepoint", "feed_version"],
            "ROUTE_SPEED":    ['trip_id', 'tot_dist', 'tot_idle_time', 'tot_trvl_time', 'tot_trip_time', 'avg_trip_speed', 'avg_trvl_speed', "feed_version"]
        }

        # Define All Needed Paths
        self.cache_time_limit    = 5
        self.timeout_time        = 10
        self.user_path           = os.path.expanduser('~')
        self.dwnld_path          = os.path.join(os.path.expanduser('~'), "Downloads")
        db_out_path              = os.path.join(self.dwnld_path,         "BramptonTransitAnalysis", "3_Data")
        self.db_folder           = db_out_path
        self.db_path             = os.path.join(db_out_path,             "DataStorage.db")
        self.csv_out_path        = os.path.join(self.dwnld_path,         "BramptonTransitAnalysis", "4_Storage")
        self.rfresh_tkn_path     = os.path.join(self.dwnld_path,         "DropboxInfo", "GrabToken.sh")
        self.zip_path            = os.path.join(self.csv_out_path,       "GTFS", "GTFS.zip")
        self.foldr_path          = os.path.join(self.csv_out_path,       "GTFS")
        self.out_dict            = {}

        # Datetime Variables
        self.td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
        self.td_s_dt_dsply_frmt = "%d-%m-%Y"
        self.log_level          = log_level




    # -------------------- Private Function #1 ---------------------------------
    def __logger(self, message = ""):
        """ Find The Location Of The Downloads Folder """

        # Verify That The Path Exists Raise Error!
        if self.log_level == 1:
            print(f"{datetime.now().strftime(self.td_l_dt_dsply_frmt)}: {message}")




    # -------------------- Private Function #2 ---------------------------------
    def get_bus_loc(self):
        """
        When called, this function will navigate to Brampton Transit JSON GTFS URL and look for new bus locations.
        It will then upload those new bus locations to the appropriate database table.
        """

        # Try To Pull GTFS Data From Transit URL, Be Careful Of Request Errors
        try:
            r = requests.get(self.BUS_LOC_URL, headers = {'User-Agent': 'Mozilla/5.0'}, timeout = self.timeout_time)
            r.raise_for_status()
            data = r.json()
            df = pd.json_normalize(data['entity'])
            
        except requests.exceptions.Timeout:                 
            self.__logger(f"Data Collector | Connection Timed Out After {self.timeout_time}s")
            df = pd.DataFrame()
            
        except requests.exceptions.ConnectionError:
            self.__logger(f"Data Collector | Endpoint Connection Failed")
            df = pd.DataFrame()
            
        except requests.exceptions.HTTPError as e:
            self.__logger(f"Data Collector | HTTP Error {e}")
            df = pd.DataFrame()

        except KeyboardInterrupt:
            conn.rollback()
            self.__logger(f"Data Collector | Keyboard Interrupt")
            sys.exit()


        # ----------------------------------------------------------------------------------------
        # Check To See If GTFS Data Is Empty. If It Is Rate Limit Code Here So We Don't Get Banned
        if len(df) == 0:
            self.__logger(f"Data Collector | ^^^ Skipping Data Collection For {self.timeout_time}s")
            time.sleep(self.timeout_time)


        # If The Dataframe Isn't Empty Format It And Get It Reeady For Injesting Into Database Table
        else:

            # Rename Columns, Every Column Has "vehicle." In It's Name - Easiest Way Is To Remove The First 8 Characters
            df.columns      = [x.replace(".", "_") for x in [x[8:] if "vehicle." in x else x for x in df.columns]]

            # Create A Datetime So We Know The Exact Time In Human Readable Rather Than Timestamp From EPOCH, Create A Unique ID To Track Entry
            df["timestamp"] = pd.to_numeric(df["timestamp"],  errors='coerce')
            df["dt_colc"]   = pd.to_datetime(df["timestamp"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')
            df["u_id"]      = df["trip_route_id"] + "_" + df["vehicle_id"] + "_" + df["timestamp"].astype(str)


            # Upload New Data To An Intermediary Temp Table, Check If The U_IDs Are In A Cache From 10 Min Ago, If Not Add To Database
            with sqlite3.connect(self.db_path) as conn:

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
                    max_timestamp = all_uids["timestamp"].max() - (self.cache_time_limit * 60)
                    all_uids = all_uids[all_uids["timestamp"] >= max_timestamp]

                    # Update the U_ID cache with filtered data
                    all_uids.to_sql('U_ID_TEMP', conn, if_exists='replace', index=False)

                    # Save All Changes To The Database
                    conn.commit()

                    # Update User
                    self.__logger(f"Data Collector | New Bus Locations Processed --> {new_rows_inserted:04}")
                    time.sleep(self.timeout_time)


                # If Something Happens Rollback To Begin, Inform User, And Wait
                except sqlite3.IntegrityError as e:
                    conn.rollback()
                    self.__logger(f"Data Collector | Duplicate Key Error: {e}")
                    time.sleep(self.timeout_time * 2)

                except sqlite3.OperationalError as e:
                    conn.rollback()
                    self.__logger(f"Data Collector | Database Operational Error: {e}")
                    time.sleep(self.timeout_time * 2)

                except Exception as e:
                    conn.rollback()
                    self.__logger(f"Data Collector | Unexpected Error: {e}")
                    time.sleep(self.timeout_time * 2)

                except KeyboardInterrupt:
                    conn.rollback()
                    self.__logger(f"Data Collector | Keyboard Interrupt")
                    sys.exit()





# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    collector = Collector(log_level = 1)

    for x in range(1000):
        collector.get_bus_loc()
        time.sleep(10)
