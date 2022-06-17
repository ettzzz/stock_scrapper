#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 12 19:59:18 2021

@author: eee
"""

import os
import logging
import logging.config

from config.static_vars import (
    ROOT,
    IS_DEBUG,
    LOGGING_FMT,
    LOGGING_DATE_FMT,
    LOGGING_NAME,
    STREAMING_FMT,
)


def get_logger(debug=IS_DEBUG):
    """
    # # print log info
    # logger.debug('debug message')
    # logger.info('info message')
    # logger.warning('warn message')
    # logger.error('error message')
    # logger.critical('critical message')
    """
    # create logger
    logger_name = "{}_logger".format(LOGGING_NAME)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    if not debug:
        # create formatter
        formatter = logging.Formatter(LOGGING_FMT, LOGGING_DATE_FMT)
        # create file handler
        log_path = os.path.join(ROOT, "{}.log".format(LOGGING_NAME))
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.INFO)
        # add handler and formatter to logger
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    else:
        formatter = logging.Formatter(STREAMING_FMT, LOGGING_DATE_FMT)
        sh = logging.StreamHandler()
        sh.setLevel(logging.DEBUG)
        sh.setFormatter(formatter)
        logger.addHandler(sh)
    return logger


gabber = get_logger()
