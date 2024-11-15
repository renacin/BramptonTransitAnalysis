# Name:                                            Renacin Matadeen
# Date:                                               11/02/2024
# Title                                      Main Logic Of Data Collector
#
# ----------------------------------------------------------------------------------------------------------------------
from concurrent.futures        import ProcessPoolExecutor
import datetime
import time
import sys
import gc
# ----------------------------------------------------------------------------------------------------------------------


def setup_folders():
    from Functions.collect_data import DataCollector
    Collector = DataCollector(skp_dwnld=True)
    del Collector, DataCollector
    gc.collect()


def collect_data():
    from Functions.collect_data import DataCollector
    Collector = DataCollector(skp_dwnld=True)
    Collector.get_bus_loc()
    del Collector, DataCollector
    gc.collect()


def export_data():
    from Functions.collect_data import DataCollector
    Collector = DataCollector(skp_dwnld=True)
    Collector.xprt_data("BUS_LOC", "BUS_LOC_DB", "u_id", True)
    Collector.xprt_data("ERROR", "ERROR_DB", "timestamp", True)
    Collector.frmt_speed_data()
    del Collector, DataCollector
    gc.collect()

# ----------------------------------------------------------------------------------------------------------------------

# Define The Main Logic Of Data Collection Tool
def main():

    # Define Constants & Next Alarm Date Scheduled Maintenance Will Be The Next Day (+1) At 0300 AM
    alrm_hr            = 3
    td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
    td_s_dt_dsply_frmt = "%d-%m-%Y"
    alrm_dt            = str((datetime.datetime.now() + datetime.timedelta(days = 1)).strftime(td_s_dt_dsply_frmt))

    # Setup Folders With Worker
    with ProcessPoolExecutor(max_workers = 1) as exe:
        exe.submit(setup_folders)

    while True:
        # Create A New Object To Reduce Possibility Of Memory Leak & Define Needed Variables
        cur_dt         =   str(datetime.datetime.now().strftime(td_s_dt_dsply_frmt))
        cur_hr         =   int(datetime.datetime.now().strftime('%H'))
        now            =   datetime.datetime.now().strftime(td_l_dt_dsply_frmt)

        # Enter Main Logic, Is It Time To Collect Or Export Data?
        try:

            # Is It Time To Export Data?
            if (cur_hr == alrm_hr and cur_dt == alrm_dt):
                with ProcessPoolExecutor(max_workers = 1) as exe:
                    exe.submit(export_data)
                time.sleep(10)
                alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days = 1)).strftime(td_s_dt_dsply_frmt))

            # If Not Just Collect Data
            else:
                with ProcessPoolExecutor(max_workers = 1) as exe:
                    exe.submit(collect_data)
                time.sleep(20)


        # Catch Keyboard Interupt
        except KeyboardInterrupt:
            print(f"{now}: Keyboard Interrupt Error")
            sys.exit(1)


        # Catch All Other Errors
        except Exception as e:
            print(f"{now}: Logic Operation Error (Type - {e})")
            time.sleep(10)


        # Try To Conserve Memory
        del cur_dt, cur_hr, now
        gc.collect()


# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code
if __name__ == "__main__":
    main()
