# Name:                                            Renacin Matadeen
# Date:                                               03/03/2024
# Title                            Main Logic Of Data Collector: Version 2 Memory Optimized?
#
# ----------------------------------------------------------------------------------------------------------------------
from Functions.collect_data import *
import datetime
import time
# ----------------------------------------------------------------------------------------------------------------------

# Main Logic Of Python Code
def main():

    # Needed Standards
    td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
    td_s_dt_dsply_frmt = "%d-%m-%Y"

    # Scheduled Maintenance Will Be The Next Day (+1) At 0300 AM
    tm_delay = 18
    alrm_hr = 3
    alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime(td_s_dt_dsply_frmt))


    # Instantiate Data Collector
    Collector = DataCollector(skp_dwnld=False)

    while True:

        # Get The Current Time
        cur_dt = str(datetime.datetime.now().strftime(td_s_dt_dsply_frmt))
        cur_hr = int(datetime.datetime.now().strftime('%H'))

        try:

            # If It's 0300AM, Do Certain Things
            if (cur_hr == alrm_hr and cur_dt == alrm_dt):

                alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime(td_s_dt_dsply_frmt))
                time.sleep(tm_delay)

            # If It's Not Scheduled Maintenance Just Collect Data
            else:
                Collector.get_bus_loc()
                time.sleep(tm_delay)


        except KeyboardInterrupt:
            now = datetime.datetime.now().strftime(td_l_dt_dsply_frmt)
            print(f"{now}: Interrupt Error")
            break


        except Exception as e:
            now = datetime.datetime.now().strftime(td_l_dt_dsply_frmt)
            print(f"{now}: Operation Error (Type - {e})")
            time.sleep(tm_delay)



# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code
if __name__ == "__main__":
    main()
