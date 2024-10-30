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

import warnings
warnings.filterwarnings("ignore")
# ----------------------------------------------------------------------------------------------------------------------


# Main Logic Of Python Code
def main():

    # Needed Standards
    td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
    td_s_dt_dsply_frmt = "%d-%m-%Y"

    # Scheduled Maintenance Will Be The Next Day (+1) At 0300 AM
    tm_delay = 18

    # Instantiate Data Collector
    Collector = DataCollector(skp_dwnld=True)

    # Main Loop Of Code
    num_iterations = 100
    counter = num_iterations
    while True:

        # Implement Simpler Version; Every 100 Iterations Export A Log File With The Current Local Scape Variables & Size
        try:

            # If We Are At The Counter Min Export Data & Reset Counter
            if counter == 0:

                # A Local Scope Won't Help, We Need Granular Data From Within The Class
                counter = num_iterations

                # Pass Local Data Within Main To See If We Can See Anything On A Granular Picture
                local_vars = list(locals().items())
                loc_vars = []
                loc_size = []
                for var, obj in local_vars:
                    loc_vars.append(var)
                    loc_size.append(sys.getsizeof(obj))

                Collector.disp_mem_consum(loc_vars, loc_size)


            # If We Aren't At The Counter Min Minus One From The Counter & Keep Going
            else:
                Collector.get_bus_loc()
                time.sleep(tm_delay)
                counter -= 1


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

    # Notes: Looks Like It Starts At 86MB - 2024-10-29 10:08PM
