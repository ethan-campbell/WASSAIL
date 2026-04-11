# -*- coding: utf-8 -*-

from datetime import datetime, timedelta


def convert_tuple_to_datetime(tuple_datetime):
    """ Converts a tuple of arbitrary length [e.g. (Y,M,D) or (Y,M,D,H) or (Y,M,D,H,M,S)] to a Datetime object.
    """
    return datetime(*tuple_datetime)


def convert_date_to_365(date_tuple):
    date = convert_tuple_to_datetime(date_tuple)
    start_date = datetime(date.year-1,12,31)
    return (date - start_date).days


def is_time_in_range(start_date, end_date, time_in_question):
    """ Is datetime tuple within range of date tuples? Responds 'True' or 'False'.
    """
    return convert_tuple_to_datetime(start_date) <= convert_tuple_to_datetime(time_in_question) <= convert_tuple_to_datetime(end_date)


def dates_in_range(start_date, end_date):
    """ Returns dates within two boundary dates (inclusive).
    
    Args:
        start_date and end_date: (Y,M,D), with start/end inclusive
    Returns:
        list of date tuples (y,m,d)
        
    """
    
    start_date = convert_tuple_to_datetime(start_date)
    end_date = convert_tuple_to_datetime(end_date)
    all_dates = []
        
    date_iter = start_date
    while date_iter <= end_date:
        all_dates.append((date_iter.year,date_iter.month,date_iter.day))
        date_iter += timedelta(days=1)
    
    return all_dates


def now(include_time=False):
    """ Returns tuple of current date (YYYY,MM,DD) or datetime (YYYY,MM,DD,HH,MM,SS) if 'include_time' is True.
    """
    if include_time:
        return (datetime.now().year,datetime.now().month,datetime.now().day,datetime.now().hour,datetime.now().minute,datetime.now().second)
    else:
        return (datetime.now().year, datetime.now().month, datetime.now().day)
