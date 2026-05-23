# Name:                                            Renacin Matadeen
# Date:                                               04/12/2026
# Title                                   Helper Functions Needed For Main Logic
#
# ----------------------------------------------------------------------------------------------------------------------
import numpy as np
import logging
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
def shared_logger(logger_name="", message_txt="", func_level=1):

    # Where Are We Writting To?


    # Define Logger Name & Basic Level Of Logging Tool
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # Remove Any Other Loggers, Stream To The Console, And Define Format
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '|-- %(asctime)s | %(name)-12s | %(levelname)-8s : %(message)s --|',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        logger.addHandler(handler)

    # Depending On The Output Choose Level
    if   func_level == 1: logger.info(message_txt)
    elif func_level == 2: logger.warning(message_txt)
    elif func_level == 3: logger.error(message_txt)
    elif func_level == 4: logger.critical(message_txt)



# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    shared_logger("Data Collector", "Shit is fucked?", 1)
    shared_logger("Data Collector", "Shit is fucked?", 2)
    shared_logger("Data Collector", "Shit is fucked?", 3)
    shared_logger("Data Collector", "Shit is fucked?", 4)
    shared_logger("Data Collector", "Shit is fucked?", 1)
    shared_logger("Data Collector", "Shit is fucked?", 1)
    
    
    
    