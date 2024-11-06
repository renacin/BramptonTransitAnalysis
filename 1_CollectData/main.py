# Name:                                            Renacin Matadeen
# Date:                                               11/02/2024
# Title                                      Main Logic Of Data Collector
#
# ----------------------------------------------------------------------------------------------------------------------
from concurrent.futures     import ProcessPoolExecutor
import datetime
import time
import sys
import gc
# # ----------------------------------------------------------------------------------------------------------------------


def Collector_Setup():
    """ This function will download all necessary files, but won't pass on an object """
    from Functions.collect_data import DataCollector
    Collector = DataCollector(skp_dwnld=False)
    del Collector, DataCollector
    gc.collect()


def Collector_Collect():
    "This function will create an instance of the Collector Class, collect data, and execute main logic"
    from Functions.collect_data import DataCollector
    Collector = DataCollector(skp_dwnld=True)
    Collector.get_bus_loc()
    del Collector, DataCollector
    gc.collect()


def Collector_Export():
    "This function will create an instance of the Collector Class, and export data"
    from Functions.collect_data import DataCollector
    Collector = DataCollector(skp_dwnld=True)
    Collector.xprt_data("BUS_LOC", "BUS_LOC_DB", "u_id", True)
    Collector.xprt_data("ERROR", "ERROR_DB", "timestamp", True)
    del Collector, DataCollector
    gc.collect()







# FOR TESTING REMOVE!
def Collector_Format_Data():
    from Functions.collect_data import DataCollector
    Collector = DataCollector(skp_dwnld=True)
    Collector.frmt_data()
    del Collector, DataCollector
    gc.collect()





# # --------------------------------------------------------------------------------------------------------------------

# Define The Main Logic Of Data Collection Tool
def main():

    # Define Constants & Next Alarm Date Scheduled Maintenance Will Be The Next Day (+1) At 0300 AM
    alrm_hr = 3
    tm_delay = 20
    td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
    td_s_dt_dsply_frmt = "%d-%m-%Y"
    alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime(td_s_dt_dsply_frmt))
    td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))


    # # Spin Up A Child Process, Reduce Memory Considerations - Download Data
    # with ProcessPoolExecutor(max_workers=1) as exe:
    #     exe.submit(Collector_Setup)







    # For Testing Please Remove!
    with ProcessPoolExecutor(max_workers=1) as exe:
        exe.submit(Collector_Format_Data)






    #
    # # Main Loop Of Code
    # while True:
    #
    #     # Get The Current Time
    #     cur_dt   =   str(datetime.datetime.now().strftime(td_s_dt_dsply_frmt))
    #     cur_hr   =   int(datetime.datetime.now().strftime('%H'))
    #     td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))
    #     now      = datetime.datetime.now().strftime(td_l_dt_dsply_frmt)
    #
    #     # Enter Main Logic, Is It Time To Collect Or Export Data?
    #     try:
    #         if (cur_hr == alrm_hr and cur_dt == alrm_dt):
    #
    #             # Spin Up A Child Process, Reduce Memory Considerations - Export Data
    #             with ProcessPoolExecutor(max_workers=1) as exe:
    #                 exe.submit(Collector_Export)
    #
    #             td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))
    #             alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=+1)).strftime(td_s_dt_dsply_frmt))
    #
    #         else:
    #             # Spin Up A Child Process, Reduce Memory Considerations - Collect Data
    #             with ProcessPoolExecutor(max_workers=1) as exe:
    #                 exe.submit(Collector_Collect)
    #
    #         time.sleep(tm_delay)
    #
    #     # Catch Keyboard Interupt
    #     except KeyboardInterrupt:
    #         print(f"{now}: Keyboard Interrupt Error")
    #         sys.exit(1)
    #
    #     # Catch All Other Errors
    #     except Exception as e:
    #         print(f"{now}: Logic Operation Error (Type - {e})")
    #         time.sleep(tm_delay)
    #
    #     # Try To Conserve Memory
    #     del cur_dt, cur_hr, td_dt_mx, now
    #     gc.collect()



# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code
if __name__ == "__main__":
    main()
