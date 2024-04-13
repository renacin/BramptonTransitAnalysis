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
    alrm_hr = 3
    alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime(td_s_dt_dsply_frmt))

    # Instantiate Data Collector
    Collector = DataCollector(skp_dwnld=True)


    td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))

    # Run Main Data Formatter
    Collector.frmt_rwbslc_data(td_dt_mx)

    # # Define Needed Connections
    # bus_loc_path, b_af =  Collector.return_files_dates("BUS_LOC")
    # bus_stp_path, bstp_af =  Collector.return_files_dates("BUS_STP")
    # fmted_path, f_af =    Collector.return_files_dates("FRMTD_DATA")
    # error_path, e_af =    Collector.return_files_dates("ERROR")
    # graphics_path, g_af = Collector.return_files_dates("GRAPHICS")
    #
    # # Run Data Visualizations #1
    # data_viz_3(graphics_path,
    #             fmted_path,
    #             f_af,
    #             bus_stp_path,
    #             bstp_af,
    #             error_path,
    #             e_af,
    #             td_dt_mx)











    #
    # # Main Loop Of Code
    # while True:
    #
    #     # Get The Current Time
    #     cur_dt =   str(datetime.datetime.now().strftime(td_s_dt_dsply_frmt))
    #     cur_hr =   int(datetime.datetime.now().strftime('%H'))
    #     td_dt_mx = str((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(td_s_dt_dsply_frmt))
    #
    #     try:
    #         # If It's 0300AM, Do Certain Things
    #         if (cur_hr == alrm_hr and cur_dt == alrm_dt):
    #
    #             # If It's Time, Export Data & Render Data Visualizations
    #             Collector.xprt_data("BUS_LOC", "BUS_LOC_DB", "u_id", True)
    #             Collector.xprt_data("ERROR", "ERROR_DB", "timestamp", True)
    #
    #             # Define Needed Connections
    #             bus_loc_path, b_af =  Collector.return_files_dates("BUS_LOC")
    #             error_path, e_af =    Collector.return_files_dates("ERROR")
    #             graphics_path, g_af = Collector.return_files_dates("GRAPHICS")
    #
    #             # Run Main Data Formatter
    #             Collector.frmt_rwbslc_data(td_dt_mx)
    #
    #             # Run Data Visualizations #1
    #             data_viz_1(graphics_path,
    #                         bus_loc_path,
    #                         b_af,
    #                         error_path,
    #                         e_af,
    #                         td_dt_mx)
    #
    #             # Run Data Visualizations #2
    #             data_viz_2(graphics_path,
    #                         bus_loc_path,
    #                         b_af,
    #                         error_path,
    #                         e_af,
    #                         td_dt_mx)
    #
    #             # Upload Gaphics To Dropbox Folder
    #             Collector.upld_2_dbx()
    #
    #             # Once Complete Set New Alarm
    #             alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=+1)).strftime(td_s_dt_dsply_frmt))
    #             time.sleep(tm_delay)
    #
    #
    #         # If It's Not Scheduled Maintenance Just Collect Data
    #         else:
    #             Collector.get_bus_loc()
    #             time.sleep(tm_delay)
    #
    #
    #     except KeyboardInterrupt:
    #         now = datetime.datetime.now().strftime(td_l_dt_dsply_frmt)
    #         print(f"{now}: Keyboard Interrupt Error")
    #         break
    #
    #
    #     except Exception as e:
    #         now = datetime.datetime.now().strftime(td_l_dt_dsply_frmt)
    #         print(f"{now}: Logic Operation Error (Type - {e})")
    #         time.sleep(tm_delay)



# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code
if __name__ == "__main__":
    main()
