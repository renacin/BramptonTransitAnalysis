# Name:                                            Renacin Matadeen
# Date:                                               04/12/2026
# Title                                   Helper Functions Needed For Main Logic
#
# ----------------------------------------------------------------------------------------------------------------------
from datetime import datetime
import numpy as np
import sqlite3
import logging
import time
import os
# ----------------------------------------------------------------------------------------------------------------------



# ---------------------- Function #1 ---------------------------------
# Define Function That Will Determine The Distance Between Two Points
def hvrsn_dist(coord1, coord2):
    """
    coord1 = first location reported
    coord2 = current location reported

    This function will calculate the distance between bus location and bus stop; returns distance in km
    Taken from: https://datascience.blog.wzb.eu/2018/02/02/vectorization-and-parallelization-in-python-with-numpy-and-pandas/
    """

    # TODO: Change This Soon, We Can't Use As-The Crows Flies Distance, We Need Manhattan Distance For Accurate Distance Measure, & Time Estimation

    # Grab Coordinates For Both Points
    b_lat, b_lng = coord1[0], coord1[1]
    a_lat, a_lng = coord2[0], coord2[1]

    R = 6371  # earth radius in km

    a_lat = np.radians(a_lat)
    a_lng = np.radians(a_lng)
    b_lat = np.radians(b_lat)
    b_lng = np.radians(b_lng)

    d_lat = b_lat - a_lat
    d_lng = b_lng - a_lng

    d_lat_sq = np.sin(d_lat / 2) ** 2
    d_lng_sq = np.sin(d_lng / 2) ** 2

    a = d_lat_sq + np.cos(a_lat) * np.cos(b_lat) * d_lng_sq
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))

    return R * c  # Returns Distance In KM



# ---------------------- Function #2 ---------------------------------
def shared_logger(logger_name="", message_txt="", func_level=1, log_location=""):
    """ 
    When called this function will write the information passed to a table within the main dataase that will store logs. 
    This allows for multiple concurrent users to read/write to the database.

    This function will require:
        logger_name  --> "Data Collector", "Data Exporter", "Data Janitor" (Where Is This Log Coming From)
        message_txt  --> "Hello this is a log message"
        func_level   --> 1: INFO, 2:WARNING, 3:ERROR, 4:CRITICAL,
        log_location --> Database & Table Location
    """

    # Get Current Time For Logging
    now = datetime.now()

    # Write To Database
    with sqlite3.connect(log_location, timeout=30, isolation_level=None) as conn:

        # Connect TO Database Table & Write Data
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("INSERT INTO DB_LOGS (time_stamp, reporter, warning_level, info) values (?, ?, ?, ?)", (now, logger_name, func_level, message_txt))

        # Save All Changes To The Database
        conn.commit()



# ---------------------- Function #3 ---------------------------------
def time_it(func):
    """ 
    This is a decorator function, it will mainly be used to time how long a function takes to run.
    This function will be used in testing, not production.
    """

    def wrapper(*args, **kwargs):

        # Wrap Function In Timers & Determine The Time It Took To Run
        start   = time.perf_counter()
        result  = func(*args, **kwargs)
        elapsed = time.perf_counter() - start

        # Print To The User How Long It Took
        print(f"Completion Time: {func.__name__} - {elapsed:.4f}s")
        return result
    
    return wrapper



# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    pass