#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 09:28:58 2021

@author: ert
"""

# from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.background import BackgroundScheduler

# exe_boy = ThreadPoolExecutor(1)  # TODO: how this boy is played? toxic boy!
scheduler = BackgroundScheduler()
scheduler.start()
