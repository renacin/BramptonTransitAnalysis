# Name:                                            Renacin Matadeen
# Date:                                               10/30/2024
# Title                                      Main Logic Of Data Collector
#
# ----------------------------------------------------------------------------------------------------------------------
from concurrent.futures import ProcessPoolExecutor
from Functions.collect_data import *
import warnings
import datetime
import time
import sys
import gc

warnings.filterwarnings("ignore")

# ----------------------------------------------------------------------------------------------------------------------


def Collector_Setup():
    """ This function will download all necessary files, but won't pass on an object """
    DataCollector(skp_dwnld=False)


def Collector_Collect():
    "This function will create an instance of the Collector Class, collect data, and execute main logic"
    Collector = DataCollector(skp_dwnld=False)
    Collector.get_bus_loc()


def Collector_Export():
    "This function will create an instance of the Collector Class, and export data"
    Collector = DataCollector(skp_dwnld=False)
    Collector.xprt_data("BUS_LOC", "BUS_LOC_DB", "u_id", True)
    Collector.xprt_data("ERROR", "ERROR_DB", "timestamp", True)


# # --------------------------------------------------------------------------------------------------------------------

# Define The Main Logic Of Data Collection Tool
def main():

    # Define Constants & Next Alarm Date Scheduled Maintenance Will Be The Next Day (+1) At 0300 AM
    alrm_hr = 3
    tm_delay = 18
    td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
    td_s_dt_dsply_frmt = "%d-%m-%Y"
    alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime(td_s_dt_dsply_frmt))
    td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))


    # Spin Up A Child Process, Reduce Memory Considerations - Download Data
    with ProcessPoolExecutor() as exe:
        exe.submit(Collector_Setup)


    # Main Loop Of Code
    while True:

        # Get The Current Time
        cur_dt =   str(datetime.datetime.now().strftime(td_s_dt_dsply_frmt))
        cur_hr =   int(datetime.datetime.now().strftime('%H'))
        td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))

        try:
            # If It's 0300AM, Do Certain Things
            if (cur_hr == alrm_hr and cur_dt == alrm_dt):

                # Get Today's Date As A Variable
                td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))

                # Spin Up A Child Process, Reduce Memory Considerations - Export Data
                with ProcessPoolExecutor() as exe:
                    exe.submit(Collector_Export)

                # Once Complete Set New Alarm
                alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=+1)).strftime(td_s_dt_dsply_frmt))


            # If It's Not Scheduled Maintenance Just Collect Data
            else:

                # Spin Up A Child Process, Reduce Memory Considerations - Collect Data
                with ProcessPoolExecutor() as exe:
                    exe.submit(Collector_Collect)

            #When Done Iteration Implement Delay
            time.sleep(tm_delay)


        except KeyboardInterrupt:
            now = datetime.datetime.now().strftime(td_l_dt_dsply_frmt)
            print(f"{now}: Keyboard Interrupt Error")
            break


        except Exception as e:
            now = datetime.datetime.now().strftime(td_l_dt_dsply_frmt)
            print(f"{now}: Logic Operation Error (Type - {e})")
            time.sleep(tm_delay)

        # Run Garbage For Each Scope - Hopefully This Solves Our Memory Leak Issue
        gc.collect()



# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code
if __name__ == "__main__":
    main()
