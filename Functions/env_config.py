# Name:                                            Renacin Matadeen
# Date:                                               05/19/2026
# Title                                       Main Configuration Of Script
#
# ----------------------------------------------------------------------------------------------------------------------
import os
# ----------------------------------------------------------------------------------------------------------------------

class Config():
    """ This class stores all the neded config settings  """

    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):

        # Try To Create A Table For Each Item In The Following Database
        self.BUS_LOC_URL         = r'https://gtfs-rt-merge.prod.bt-cadavl.com/BramptonTransit/GTFS/merged_VehiclePosition.json'
        self.GTFS_URL            = r'https://www.arcgis.com/sharing/rest/content/items/a355aabd5a8c490186bdce559c9c75fb/data'
        self.FOLDERs             = ["BUS_LOC_DB", "FEED_INFO", "ROUTES", "TRIPS", "STOPS", "STOP_TIMES", "ROUTE_SPEED", "GTFS", "GRAPHICS"]
        self.NOT_FEED_BASED      = {"BUS_LOC_DB", "U_ID_TEMP"}
        self.GATHER_TABLE        = {"BUS_LOC_DB", "U_ID_TEMP", "ROUTE_SPEED"}
        self.log_dict            = {"DB_LOGS":        ['time_stamp', 'reporter', 'warning_level', 'info']}
        self.table_dict          = {"BUS_LOC_DB":     ['u_id', 'id', 'trip_trip_id', 'trip_schedule_relationship', 'trip_route_id','position_latitude', 'position_longitude', 'position_bearing','position_speed', 'current_status', 'timestamp', 'stop_id','vehicle_id', 'vehicle_label', 'dt_colc'],
                                    "U_ID_TEMP":      ["u_id", "timestamp"],
                                    "FEED_INFO":      ["feed_publisher_name", "feed_lang", "feed_start_date", "feed_end_date", "feed_version"],
                                    "ROUTES":         ["route_id", "route_short_name", "route_long_name", "feed_version"],
                                    "TRIPS":          ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "block_id", "shape_id", "feed_version"],
                                    "STOPS":          ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "zone_id", "stop_url", "parent_station", "feed_version"],
                                    "STOP_TIMES":     ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "timepoint", "feed_version"],
                                    "ROUTE_SPEED":    ['trip_id', 'tot_dist', 'tot_idle_time', 'tot_trvl_time', 'tot_trip_time', 'avg_trip_speed', 'avg_trvl_speed', "feed_version"]}
        
        # Define All Needed Paths
        self.cache_time_limit    = 5
        self.timeout_time        = 10
        self.user_path           = os.path.expanduser('~')
        self.dwnld_path          = os.path.join(os.path.expanduser('~'), "Downloads")
        db_out_path              = os.path.join(self.dwnld_path,         "BramptonTransitAnalysis", "3_Data")
        self.db_folder           = db_out_path
        self.dblog_path          = os.path.join(db_out_path,             "LogStorage.db")
        self.db_path             = os.path.join(db_out_path,             "DataStorage.db")
        self.csv_out_path        = os.path.join(self.dwnld_path,         "BramptonTransitAnalysis", "4_Storage")
        self.rfresh_tkn_path     = os.path.join(self.dwnld_path,         "DropboxInfo", "GrabToken.sh")
        self.zip_path            = os.path.join(self.csv_out_path,       "GTFS", "GTFS.zip")
        self.foldr_path          = os.path.join(self.csv_out_path,       "GTFS")
        self.out_dict            = {}

        # Data Export Paths
        self.out_bus_loc_path    = os.path.join(self.csv_out_path, "BUS_LOC_DB")
        self.out_graphics_path   = os.path.join(self.csv_out_path, "GRAPHICS")

        # Datetime Variables
        self.td_xl_dt_dsply_frmt = "%d-%m-%Y_%H_%M_%S"
        self.td_l_dt_dsply_frmt  = "%d-%m-%Y %H:%M:%S"
        self.td_s_dt_dsply_frmt  = "%d-%m-%Y"



# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    pass