# Name:                                            Renacin Matadeen
# Date:                                               04/06/2024
# Title                                      Main Logic Of Data Collector
#
# ----------------------------------------------------------------------------------------------------------------------
from Functions.collect_data import *
from Functions.visualize_data import *
import datetime
import time
import sys
import gc

import warnings
warnings.filterwarnings("ignore")
# ----------------------------------------------------------------------------------------------------------------------



# Needed Standards
td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
td_s_dt_dsply_frmt = "%d-%m-%Y"

# Scheduled Maintenance Will Be The Next Day (+1) At 0300 AM
tm_delay = 18
alrm_hr = 3
alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime(td_s_dt_dsply_frmt))

# Instantiate Data Collector
Collector = DataCollector(skp_dwnld=True)

# Get Today's Date As A Variable
td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))


# Main Loop Of Code
while True:

    # Run Garbage For Each Scope - Hopefully This Solves Our Memory Leak Issue
    gc.collect()
    Collector.clean_class()


    # Get The Current Time
    cur_dt =   str(datetime.datetime.now().strftime(td_s_dt_dsply_frmt))
    cur_hr =   int(datetime.datetime.now().strftime('%H'))
    td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))


    try:
        # If It's 0300AM, Do Certain Things
        if (cur_hr == alrm_hr and cur_dt == alrm_dt):

            # Get Today's Date As A Variable
            td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))

            # If It's Time, Export Data & Render Data Visualizations
            Collector.xprt_data("BUS_LOC", "BUS_LOC_DB", "u_id", True)
            Collector.xprt_data("ERROR", "ERROR_DB", "timestamp", True)

            # Once Complete Set New Alarm
            alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=+1)).strftime(td_s_dt_dsply_frmt))


        # If It's Not Scheduled Maintenance Just Collect Data
        else:
            Collector.get_bus_loc()


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



# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code
if __name__ == "__main__":
    main()

    # Notes: Looks Like It Starts At 2024-10-30 1:49PM
    #                                1.1 MB -  1.1 MB
    #                                7.5 MB -  7.7 MB
    #                               80.4 MB - 80.6 MB
