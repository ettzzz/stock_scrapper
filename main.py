#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:38:01 2022

@author: eee
"""
from apscheduler.schedulers.background import BackgroundScheduler

from schedules import call_for_update


if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=call_for_update, trigger="cron", day_of_week="tue-sat", hour=4
    )
    scheduler.start()

    import time
    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Shut down the scheduler gracefully
        scheduler.shutdown()
