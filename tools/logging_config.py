"""
@File  : log_config.py
@Author: lee
@Date  : 2022/7/13/0013 11:08:55
@Desc  :
"""
import logging
import sys

LOGGING_CONFIG = dict(
    version=1,
    disable_existing_loggers=False,
    loggers={
        # 新曾自定义日志，用于数据采集程序
        "general": {
            "level": "INFO",
            "handlers": ["console", "general"],
            "propagate": True,
            "qualname": "general.debug",
        },
        "upload_data": {
            "level": "DEBUG",
            "handlers": ["console", "upload_data"],
            "propagate": True,
            "qualname": "upload_data.debug",
        },
        "upload_data2": {
            "level": "DEBUG",
            "handlers": ["console", "upload_data2"],
            "propagate": True,
            "qualname": "upload_data2.debug",
        },
    },
    handlers={
        # 数据采集程序控制台输出handler
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "generic",
            "stream": sys.stdout,
        },
        "general": {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'log/general/general.log',
            'maxBytes': 10 * 1024 * 1024,
            'delay': True,
            "formatter": "generic",
            "backupCount": 20,
            "encoding": "utf-8"
        },
        "upload_data": {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'log/upload_data/upload_data.log',
            'maxBytes': 10 * 1024 * 1024,
            'delay': True,
            "formatter": "generic",
            "backupCount": 20,
            "encoding": "utf-8"
        },
        "upload_data2": {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'log/upload_data2/upload_data2.log',
            'maxBytes': 10 * 1024 * 1024,
            'delay': True,
            "formatter": "generic",
            "backupCount": 20,
            "encoding": "utf-8"
        },
    },
    formatters={
        # 自定义文件格式化器
        "generic": {
            "format": "%(asctime)s {%(process)d(%(thread)d)} [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
            "datefmt": "[%Y-%m-%d %H:%M:%S]",
            "class": "logging.Formatter",
        },
    },
)
general = logging.getLogger("general")
upload_data = logging.getLogger("upload_data")
upload_data2 = logging.getLogger("upload_data2")
