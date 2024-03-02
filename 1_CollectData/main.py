# Name:                                            Renacin Matadeen
# Date:                                               03/03/2024
# Title                            Main Logic Of Data Collector: Version 2 Memory Optimized?
#
# ----------------------------------------------------------------------------------------------------------------------
from Functions.collect_data import DataCollector
from Functions.visualize_data import *
import socket
import datetime
import time
import sys
# ----------------------------------------------------------------------------------------------------------------------

# Main Logic Of Python Code
def main():

    # --------------------------------------------------------------------------
    # Define [Internal Storage], & [External Storage] Storage
    if socket.gethostname() == "Renacins-MacBook-Pro.local":
        print("Running On Macbook Pro")
        db_out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
        csv_out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
        db_path = db_out_path + "/DataStorage.db"

    elif socket.gethostname() == "raspberrypi":
        print("Running On RPI3")
        db_out_path = r"/home/pi/Documents/Python/BramptonTransitAnalysis/3_Data"
        csv_out_path = r"/media/pi/STORAGE"
        db_path = db_out_path + "/DataStorage.db"

    else:
        print(f"Invalid Host Name")
        raise Exception


    # --------------------------------------------------------------------------
    # Create An Instance Of The Data Collector
    try:
        Collector = DataCollector(db_path, csv_out_path, skp_dwnld=True)

    except KeyboardInterrupt as e:
        now = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        print(f"Interrupt Error: {now}")
        sys.exit(1)






	#
    # # --------------------------------------------------------------------------
    # # Scheduled Maintenance Will Be The Next Day (+1) At 0300 AM
    # tm_delay = 18
    # alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
    # alrm_hr = 3
	#
    # # Keep Data Collector Running
    # while True:
	#
    #     # Get The Current Time
    #     cur_dt = str(datetime.datetime.now().strftime('%Y-%m-%d'))
    #     cur_hr = int(datetime.datetime.now().strftime('%H'))
	#
    #     try:
    #         # If It's 0300AM, Export Data To CSV, Clean DB Tables, Generate Graphics, Etc...
    #         if (cur_hr == alrm_hr and cur_dt == alrm_dt) | True:
	#
	#
    #             # What Do We Know About The Memory Issue? We Know The Export File Was Written, But Then No Graphic Was Created
    #             # Was The Issue With data_viz_1?
	#
    #             # Perform Data Maintenance, Export Data & Clean Database
    #             # Collector.xprt_data("BUS_LOC", "BUS_LOC_DB", "u_id", True)
	#
    #             # Define Needed Connections
    #             bus_loc_path, b_af = Collector.return_files_dates("BUS_LOC")
    #             graphics_path, g_af = Collector.return_files_dates("GRAPHICS")
	#
    #             # Run Data Visualizations
    #             # data_viz_1(graphics_path, bus_loc_path, b_af, str((datetime.datetime.now() + datetime.timedelta(days=-3)).strftime('%Y-%m-%d')))
    #             # data_viz_2(graphics_path, bus_loc_path, b_af, str((datetime.datetime.now() + datetime.timedelta(days=-2)).strftime('%Y-%m-%d')))
	#
    #             raise KeyboardInterrupt
	#
    #             # Set New Alarm Date
    #             alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
	#
    #             # Wait To Gather Data Again
    #             time.sleep(tm_delay)
	#
    #         # If It's Not Scheduled Maintenance Just Collect Data
    #         else:
    #             Collector.get_bus_loc()
    #             time.sleep(tm_delay)
	#
	#
    #     except KeyboardInterrupt:
    #         now = datetime.datetime.now()
    #         dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
    #         print(f"Interrupt Error: {dt_string}")
    #         break
	#
	#
    #     except Exception as e:
    #         now = datetime.datetime.now()
    #         dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
    #         print(f"Operation Error: Type {dt_string}, {e}")
    #         time.sleep(tm_delay)



# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code
if __name__ == "__main__":

    main()
