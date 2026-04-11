# -*- coding: utf-8 -*-

from numpy import *


def convert_360_lon_to_180(lons):
    """ Converts any-dimension array of longitudes from 0 to 360 to longitudes from -180 to 180.
    """
    lons = array(lons)
    outside_range = lons > 180
    lons[outside_range] = lons[outside_range] - 360
    return lons