# Name:                                            Renacin Matadeen
# Date:                                               01/27/2024
# Title                                Main Logic Of Data Collector & Output Formatter
#
# ----------------------------------------------------------------------------------------------------------------------
from Functions.collect_data import DataCollector
import socket
import datetime
import time
# ----------------------------------------------------------------------------------------------------------------------

# Main Logic Of Python Code
def main():

    # Define [Internal Storage], & [External Storage] Storage
    if socket.gethostname() == "Renacins-MacBook-Pro.local":
        db_out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
        csv_out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
        print("Running On Macbook Pro")

    else:
        db_out_path = r"/home/pi/Documents/Python/BramptonTransitAnalysis/3_Data"
        csv_out_path = r"/media/pi/STORAGE"
        print("Running On RPI3")

    db_path = db_out_path + "/DataStorage.db"

    # Create An Instance Of The Data Collector
    Collector = DataCollector(db_path, skp_rte_dwn=True, skp_stp_dwn=True)

    # Scheduled Maintenance Will Be The Next Day (+1) At 0300 AM, Export Data From DB To CSV, And Clear The Database
    # Note: Compare Hour Only. Incase Processing Causes It To Miss The Exact Time With Regards To Minutes
    alrm_dt = str(datetime.datetime.now().strftime('%Y-%m-%d'))
    alrm_hr = "03"

    # Keep Data Collector Running
    while True:

        # Get The Current Time
        nw_dt = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        nw_tm = str(datetime.datetime.now().strftime('%H'))

        try:
            # If It's 0300AM, Export Data To CSV, Clean DB Tables, Generate Graphics, Etc...
            if ((nw_tm == alrm_hr) & (nw_dt == alrm_dt)):

                # Perform Data Maintenance
                Collector.xprt_data(csv_out_path, "BUS_LOC_DB", "u_id", True)
                Collector.xprt_data(csv_out_path, "DB_META_DT", "time", True)

                # Do Other Data Processing Once Methodology Found

                # Set New Alarm Date
                alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
                time.sleep(15)

            # If It's Not Scheduled Maintenance Just Collect Data
            else:
                Collector.get_bus_loc()
                time.sleep(15)

        except KeyboardInterrupt:
            now = datetime.datetime.now()
            dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
            print(f" Stoped By User: {dt_string}")
            break

        except Exception as e:
            print(f"Operation Error: {e}")
            break


# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code
if __name__ == "__main__":
    main()
