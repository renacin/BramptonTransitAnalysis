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
def logger(self, message = ""):
    """ Find The Location Of The Downloads Folder """






# ----------------------------------------------------------------------------------------------------------------------

# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    logging.basicConfig(
        level   = logging.INFO, # minimum level to show
        format  = '%(asctime)s | Data Collector | %(levelname)-8s --> %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S'
        )

    logging.info('Hello, this is a log message')
    logging.warning('Something looks weird')
    logging.error('Something broke')
    logging.critical('Shit is fucked')