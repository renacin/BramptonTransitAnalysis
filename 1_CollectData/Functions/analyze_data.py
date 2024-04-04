# Name:                                            Renacin Matadeen
# Date:                                               04/03/2024
# Title                          Having collected data, we must now begin to analyze bus traffic
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import gc
import time
import sqlite3
import numpy as np
import pandas as pd
import datetime
# ----------------------------------------------------------------------------------------------------------------------


# Needed Variables For All Functions
td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
td_s_dt_dsply_frmt = "%d-%m-%Y"


def main_attempt(graphics_path, out_path, fl_data, bus_stp_path, bstp_af, e_out_path, e_fl_data, td_dt_mx):
    """ Is It Possible To Infer Trip Times From GTFS Data? """

    # Make Sure We Have At Least 2 Days Worth Of Data
    if len(fl_data["FILE_NAME"].tolist()) >= 2:

        # Find Days Between Today And Minus 1 Days | Add Fix For Date Formatting
        fl_data["DATE"] = fl_data["DATE"].astype(str)
        # fl_data["DATE"] = fl_data["DATE"].str.split("-").str[-1] + "-" + fl_data["DATE"].str.split("-").str[1] + "-" + fl_data["DATE"].str.split("-").str[0]
        fl_data["DATE"] = pd.to_datetime(fl_data["DATE"], format="%Y-%m-%d")
        fl_data["YEAR"] = fl_data["DATE"].dt.year
        fl_data["MONTH"] = fl_data["DATE"].dt.month
        fl_data["DAY"] = fl_data["DATE"].dt.day

        td_dt_mx = "02-04-2024"
        td_dt_mx = datetime.datetime.strptime(td_dt_mx, td_s_dt_dsply_frmt)
        fl_data = fl_data[fl_data["DATE"] == td_dt_mx]

        print(fl_data)
        # df = pd.concat([pd.read_csv(path_, usecols=['u_id', 'dt_colc', 'vehicle_id']) for path_ in [f"{out_path}/{x}" for x in fl_data["FILE_NAME"].tolist()]])
        # del fl_data
