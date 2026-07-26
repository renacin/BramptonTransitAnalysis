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
from Functions.data_helper import *

from contextlib import closing
from pathlib import Path
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
        self.__export_bus_locs()
        self.__export_old_gtfs()
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
        # Wrap Cntext Manager With Closing Context Manager. With Doesn't Natively Close SQLite Connections, Only SQLite Transactions
        with closing(sqlite3.connect(self.cfg.db_path, timeout=120, isolation_level=None)) as conn:
            with conn:   

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
        # Wrap Cntext Manager With Closing Context Manager. With Doesn't Natively Close SQLite Connections, Only SQLite Transactions
        with closing(sqlite3.connect(self.cfg.db_path, timeout=120, isolation_level=None)) as conn:
            with conn:

                # Set PRAGMAs BEFORE any transaction, while no lock is held
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=120000")

                # Wrap In Try & Accept
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
                                df = pd.read_sql_query(f"""SELECT * FROM {table_} WHERE CAST(feed_version AS INTEGER) <= CAST({str(dates_[1])} AS INTEGER)""", conn)
                                df.to_csv(out_path, index=False)

                                # Export To Logger
                                shared_logger("Data Exporter", f"Exported Old {table_} Data", 1, self.cfg.dblog_path)

                                # Delete All Data & Vacuum Database
                                conn.execute("BEGIN IMMEDIATE")
                                conn.execute(f"""DELETE FROM {table_} WHERE CAST(feed_version AS INTEGER) <= CAST({str(dates_[1])} AS INTEGER)""")
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

        # Read CSV Storage Folder, Grab The 3 Most Recent Exports. Sort By Write Time, NOT Filename 
        csv_path      = os.path.join(self.cfg.csv_out_path, "BUS_LOC_DB")
        all_files     = sorted(Path(csv_path).glob("BUS_LOC_DB_*.csv"), key=os.path.getmtime)[-3:]

        # Get Current Date & Day Before
        days_            = 1
        dt_ystrd____f2   = (datetime.now() - timedelta(days = days_)).strftime("%Y-%m-%d")
        dt_ystrd____f3   = (datetime.now() - timedelta(days = days_)).strftime("%Y%m%d")


        # Proceed Only If There Is At Least One Export To Read
        if len(all_files) >= 1:

            # Catch Errors Or Exceptions
            try:
                #============================================================================
                # [Bronze 2 Silver] --> PHASE 1: Focus On Pertinent Data
                #============================================================================
                all_raw                 = pd.concat([pd.read_csv(file_, dtype={"trip_route_id": str}) for file_ in all_files], ignore_index=True)
                all_raw['dt_colc']      = pd.to_datetime(all_raw['dt_colc'])
                all_raw['dt_colc_date'] = all_raw["dt_colc"].dt.strftime("%Y-%m-%d")
                all_raw                 = all_raw[all_raw["dt_colc_date"] == dt_ystrd____f2]

                # Nothing Dated Yesterday? Log It Loudly Instead Of Writing An Empty File
                if len(all_raw) == 0:
                    shared_logger("Data Exporter", f"No Rows Found For {dt_ystrd____f2} In {len(all_files)} Export(s)", 2, self.cfg.dblog_path)
                    return



                #============================================================================
                # [Bronze 2 Silver] --> PHASE 2: Drop Bad Columns, Rename, Round
                #============================================================================
                all_raw = all_raw.sort_values(by=["vehicle_id", "timestamp"])
                all_raw = all_raw.rename(columns={'dt_colc': 'batch_timestamp', 'position_speed': 'speed_kmph'})
                for col in ["dt_colc_date", "timestamp", "vehicle_label", "u_id", "id"]:
                    del all_raw[col]

                all_raw["position_latitude"]  = all_raw["position_latitude"].round(5)
                all_raw["position_longitude"] = all_raw["position_longitude"].round(5)



                #============================================================================
                # [Bronze 2 Silver] --> PHASE 3: Validate Each Column, Speed, Heading Etc...
                #============================================================================
                # Create A Filter So We Can Find Good & Bad Rows
                valid_mask = (
                        all_raw["speed_kmph"].between(0, 120)            &
                        all_raw["position_bearing"].between(0, 360)      &
                        all_raw["position_latitude"].between(43.5, 44.0) &
                        all_raw["position_longitude"].between(-80.0, -79.4)
                    )
                
                # Separate Rows Into Good & Bad
                bad_reading_data  = all_raw[~valid_mask]
                good_reading_data = all_raw[valid_mask]


                # Store Lens For Logging
                all_raw_len           = len(all_raw)
                bad_reading_data_len  = len(bad_reading_data)
                good_reading_data_len = len(good_reading_data)

                # Delete Unneeded Data
                del all_raw



                #============================================================================
                # [Bronze 2 Silver] --> PHASE 4: Identify Multiple Updates For 1 Timestamp
                #============================================================================

                # Read In Most Recent Route MasterKey
                rt_mk_path              = os.path.join(self.cfg.csv_out_path, "ROUTES_MASTERKEY")
                max_rt_mk_ver           = max([int(file_[:8]) for file_ in list(os.listdir(rt_mk_path)) if file_[9:] == "ROUTEMASTERKEY.csv"])
                max_rt_mk_path          = os.path.join(self.cfg.csv_out_path, "ROUTES_MASTERKEY", f"{max_rt_mk_ver}_ROUTEMASTERKEY.csv")
                stops_df                = pd.read_csv(max_rt_mk_path)


                # Try To Remove Duplicates Based On As Many Columns As Possible, Store Len For Logging
                good_reading_data = good_reading_data.drop_duplicates(subset=["vehicle_id", "trip_trip_id", "position_latitude", "position_longitude", "current_status", "stop_id"])
                nodup_p1_data_len = len(good_reading_data)


                # Left Join Bus Stop Data Onto Bus Location Observations
                good_reading_data['trip_trip_id']  =  norm_stop_id(good_reading_data['trip_trip_id'])
                good_reading_data['stop_id' ] =  norm_stop_id(good_reading_data['stop_id'])
                stops_df['stop_id']           =  norm_stop_id(stops_df['stop_id'])
                stops_df["trip_id"]           =  norm_stop_id(stops_df["trip_id"])
                data_with_stops               = pd.merge(good_reading_data, stops_df, left_on=['stop_id', 'trip_trip_id'], right_on=['stop_id', 'trip_id'], how='left')
                del stops_df

                # Determine Distance Between Points
                data_with_stops['km2nxtstp']  = hvrsn_dist((data_with_stops['position_latitude'].values, data_with_stops['position_longitude'].values), (data_with_stops['stop_lat'].values, data_with_stops['stop_lon'].values))

                # Sort Data By Multiple Columns
                data_with_stops               = data_with_stops.sort_values(by=['vehicle_id', 'batch_timestamp', 'stop_sequence', 'km2nxtstp'], ascending=[True, True, True, False])

                # Round KM To Next Stop, We Only Need Two Digits
                data_with_stops['km2nxtstp'] = data_with_stops['km2nxtstp'].astype(str).str.extract(r'(-?\d+\.\d{2})')[0].astype(float)


                #============================================================================
                # [Bronze 2 Silver] --> PHASE 5: Enrich Data
                #============================================================================

                # Delete Unneeded Cols
                del data_with_stops["trip_id"]

                # Account For Multiple Updates For 1 Timestamp | Find Which Have Multiple
                mltpl_obs         = data_with_stops.groupby(['vehicle_id', 'batch_timestamp'], as_index=False).agg(obs_per_btch_tmstmp = ("vehicle_id", "count"))
                mltpl_obs         = mltpl_obs[mltpl_obs["obs_per_btch_tmstmp"] >= 2]
                data_with_stops   = pd.merge(data_with_stops, mltpl_obs, on=['vehicle_id', 'batch_timestamp'], how='left')
                data_with_stops["obs_per_btch_tmstmp"] = data_with_stops["obs_per_btch_tmstmp"].fillna(1)
                del data_with_stops["route_id"]

                # Add Columns In Regards To The Datetime
                data_with_stops["hour_of_day"] = data_with_stops["batch_timestamp"].dt.hour
                data_with_stops["day_of_week"] = data_with_stops["batch_timestamp"].dt.day_name()
                data_with_stops["is_weekend"]  = data_with_stops["batch_timestamp"].dt.dayofweek >= 5

                # For Logging
                data_with_stops = data_with_stops.drop_duplicates()
                data_with_stops_len = len(data_with_stops)


                #============================================================================
                # [Bronze 2 Silver] --> PHASE 6: Export Data To Appropriate Location
                #============================================================================

                # Reorganize Data For Better Readability
                data_with_stops = data_with_stops.loc[:,["trip_trip_id","trip_schedule_relationship","trip_route_id",
                                                         "service_id","trip_headsign","direction_id","batch_timestamp",
                                                         "hour_of_day","day_of_week","is_weekend","obs_per_btch_tmstmp",
                                                         "vehicle_id","position_latitude","position_longitude","position_bearing",
                                                         "speed_kmph","current_status","stop_sequence","stop_id","stop_name",
                                                         "stop_lat","stop_lon","km2nxtstp"]]


                # Export Data
                out_path = os.path.join(self.cfg.csv_out_path, f"CLEANED_LOC_DATA")
                out_file = os.path.join(out_path, f"{dt_ystrd____f3}_CLND_BUSLOC.csv")
                data_with_stops.to_csv(out_file, index=False)

                # Inform Logger Of Status
                shared_logger("Data Exporter", f"Cleaned Bus Loc Data (Raw Len: {all_raw_len}, Bad Data: {bad_reading_data_len}, Operable Rows: {good_reading_data_len}, Final Rows: {data_with_stops_len})", 1, self.cfg.dblog_path)


            except KeyboardInterrupt:
                shared_logger("Data Exporter", f"Keyboard Interrupt", 2, self.cfg.dblog_path)

            except Exception as e:
                shared_logger("Data Exporter", f"Failed To Clean Up Bus Loc Data: {e}", 2, self.cfg.dblog_path)





# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    
    pass

