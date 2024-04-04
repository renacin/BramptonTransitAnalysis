# Name:                                            Renacin Matadeen
# Date:                                               04/03/2024
# Title                          Having collected data, we must now begin to analyze bus traffic
#
# ----------------------------------------------------------------------------------------------------------------------
import numpy as np
import math
# ----------------------------------------------------------------------------------------------------------------------

def vec_haversine(coord1, coord2):
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

    return R * c  # returns distance between a and b in km



def get_bearing(coord1, coord2):
    """
	coord1 = first location reported
	coord2 = current location reported

    This function will calculate the bearing between two coordinates
    Taken from: https://stackoverflow.com/questions/54873868/python-calculate-bearing-between-two-lat-long
    """
    lat1, long1 = coord1[0], coord1[1]
    lat2, long2 = coord2[0], coord2[1]

    dLon = (long2 - long1)
    x = math.cos(math.radians(lat2)) * math.sin(math.radians(dLon))
    y = math.cos(math.radians(lat1)) * math.sin(math.radians(lat2)) - math.sin(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.cos(math.radians(dLon))
    brng = np.arctan2(x,y)
    brng = np.degrees(brng)

    return brng
