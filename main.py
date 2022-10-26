#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:38:01 2022

@author: eee
"""

from apscheduler.schedulers.background import BackgroundScheduler

from schedules import call_for_update


def main():
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=call_for_update, trigger="cron", day_of_week="tue-sat", hour=4
    )
    scheduler.start()


if __name__ == "__main__":
    # main()
    call_for_update()
    """
    if main() is not on the run, add
        1 4 * * 2-6 /path/to/python3 /path/to/project/main.py
    to crontab -e
    which means run this script on 04:01 from Tuesday to Saturday
    """
