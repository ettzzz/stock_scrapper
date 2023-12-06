# -*- coding: utf-8 -*-

import time
import datetime

from utils.gibber import logger

DATE_FORMAT = "%Y-%m-%d"

def struct_timestr(timestr, _format = DATE_FORMAT):
    structed_time = time.strptime(timestr, _format)
    return structed_time


def struct_datestr(datestr, _format = DATE_FORMAT):
    structed_date = datetime.datetime.strptime(datestr, _format)
    return structed_date


def get_now():
    return time.time()


def get_today_date():
    today = datetime.datetime.now()
    today_str = datetime.datetime.strftime(today, DATE_FORMAT)
    return today_str


def get_delta_date(date, days):
    # type(date) is str
    # type(target_datestr) is str
    strd = struct_datestr(date)
    target_strd = strd + datetime.timedelta(days)
    target_datestr = datetime.datetime.strftime(target_strd, DATE_FORMAT)
    return target_datestr
    

def timestamper(time_str, _format=DATE_FORMAT):
    '''
    time_in_str: '2016-05-27 07:07:26'
    _format: '%Y-%m-%d %H:%M:%S'
    return 1464304046
    '''
    if _format is int:
        return int(time_str)
    else:
        structed_time = struct_timestr(time_str, _format)
        return int(time.mktime(structed_time))


def date_range(start, end, step=1, format_=DATE_FORMAT, category = 'all'):
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    end = strftime(strptime(end, format_) + datetime.timedelta(1), format_)
    days = (strptime(end, format_) - strptime(start, format_)).days

    if category == 'all':
        return [strftime(strptime(start, format_) + datetime.timedelta(i), format_) for i in range(0, days, step)]
    elif category == 'weekend':
        weekends = []
        for i in range(0, days, step):
            new_day = strptime(start, format_) + datetime.timedelta(i)
            if new_day.weekday() > 4:
                weekends.append(strftime(new_day, format_))
        return weekends

    elif category == 'workday':
        workdays = []
        for i in range(0, days, step):
            new_day = strptime(start, format_) + datetime.timedelta(i)
            if new_day.weekday() <= 4:
                workdays.append(strftime(new_day, format_))
        return workdays
    else:
        logger.error('Wrong day category!, only "all", "weekday" and "workday" are acceptable.')
        return []
    #strptime(start, format_).weekday() # 0 monday 1 Tuesday, 6 Sunday
