"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import logging
import os

from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import create_engine
import urllib.parse
import multiprocessing

import time
from itertools import cycle
import pandas as pd


class MessageValidator:


    _update_fields_list = ['Symbol', '7 Day Yield', 'Ask', 'Ask Change',
                           'Ask Market Center', 'Ask Size', 'Ask Time',
                           'Available Regions', 'Average Maturity', 'Bid',
                           'Bid Change', 'Bid Market Center', 'Bid Size',
                           'Bid Time', 'Change', 'Close', 'Close Range 1',
                           'Close Range 2', 'Days to Expiration', 'Decimal Precision',
                           'Delay', 'Exchange ID', 'Extended Trade', 'Extended Trade Date',
                           'Extended Trade Market Center', 'Extended Trade Size',
                           'Extended Trade Time', 'Extended Trading Change',
                           'Extended Trading Difference', 'Financial Status Indicator', 'High', 'Last', 'Last Date',
                           'Last Market Center', 'Last Size', 'Last Time',
                           'Low', 'Market Capitalization', 'Market Open',
                           'Message Contents', 'Most Recent Trade', 'Most Recent Trade Conditions', 'Most Recent Trade Date',
                           'Most Recent Trade Market Center', 'Most Recent Trade Size',
                           'Most Recent Trade Time', 'Net Asset Value',
                           'Number of Trades Today', 'Open', 'Open Interest',
                           'Open Range 1', 'Open Range 2', 'Percent Change',
                           'Percent Off Average Volume', 'Previous Day Volume',
                           'Price-Earnings Ratio', 'Range', 'Restricted Code',
                           'Settle', 'Settlement Date', 'Spread', 'Tick', 'TickID',
                           'Total Volume', 'Volatility', 'VWAP']

    def __init__(self):
        pass

    def process_message(self, message: str, retry=False):
        """
        Pretent to do some processing/transformation
        :param self:
        :param message:
        :return:
        """
        time.sleep(0.001)
        return message


class FakeFeedCSV:
    """Generates a Repeating Feed of Data for Benchmarking/Testing
    176,    Q,RISJ,nan,207.95,nan,11,100,03:01.3,
    """

    def __init__(self, sleep_time: (int or float) = 1.0, filepath=f'zmq_high_speed_subs/test_data.csv'):
        self.filepath = filepath
        self.sleep_time = sleep_time
        try:
            df = pd.read_csv(self.filepath)
        except:
            df = pd.read_csv(os.path.basename(self.filepath))

        df.reset_index(drop=True, inplace=True)
        self.messages = []
        counter = 0

        for idx, df_ in enumerate(df.index):
            msg = f"{counter}," + ",".join(list(df.iloc[idx, :].astype(str).to_dict().values())).replace("nan", "")
            self.messages.append(msg)

    def __iter__(self):
        counter = 0
        test_messages = []
        time_start = time.time()
        for msg in cycle(self.messages):
            counter += 1
            test_messages.append(msg)
            yield msg
            # time.sleep(self.sleep_time)


class FakeFeedSQL:
    """Generates a Repeating Feed of Data for Benchmarking/Testing
    123,MSFT,None,141.46,0.01,18,800,48463271015,
    """

    def __init__(self, sleep_time: (int or float) = 1.0, filepath=f'zeromq_high_speed_subscribers/test_data.csv'):
        self.sql = """SELECT TOP (500) * FROM [20190726_LVL1_Q2]"""
        self.sleep_time = sleep_time
        print(sleep_time)
        sql_db_connection = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f'SERVER={os.getenv("DB_HOST")};'
            f'DATABASE={os.getenv("DB_DATABASE")};'
            f'UID={os.getenv("DB_USERNAME")};'
            f'PWD={os.getenv("DB_PASSWORD")}'
        )

        params = urllib.parse.quote_plus(sql_db_connection)

        self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", echo=True, fast_executemany=True)

    def __iter__(self):
        counter = 0

        for chunk in pd.read_sql_query(self.sql, self.engine, chunksize=50):
            counter += 1
            msg = f"{counter}," + ",".join(list(chunk.iloc[0, :].astype(str).to_dict().values()))
            print(msg)
            yield msg
            time.sleep(self.sleep_time)



import logging

class CustomFormatter(logging.Formatter):
    """Logging Formatter to add colors and count warning / errors"""

    grey = "\x1b[38;21m"
    yellow = "\x1b[33;21m"
    red = "\x1b[31;21m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    # format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
    format = '%(asctime)s %(levelname)-8s [%(processName)-10s(%(process)d)] %(message)s'

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def setup_logging():
    # import coloredlogs, logging
    # coloredlogs.install()

    # logging.info("It works!")
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.INFO)
    # Create formatters and add it to handlers
    # c_format = logging.Formatter('%(asctime)s %(levelname)-8s [%(processName)-10s(%(process)d)] %(message)s')
    # c_handler.setFormatter(c_format)
    c_handler.setFormatter(CustomFormatter())

    return c_handler


def initializer(level):
    global logger
    logger = multiprocessing.log_to_stderr(level)


def setup_db_connection(driver='sqlalchemy', echo=False):

    if driver == "sqlalchemy":

        sql_db_connection = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f'SERVER={os.getenv("DB_HOST")};'
            f'DATABASE={os.getenv("DB_DATABASE")};'
            f'UID={os.getenv("DB_USERNAME")};'
            f'PWD={os.getenv("DB_PASSWORD")}'
        )

        params = urllib.parse.quote_plus(sql_db_connection)

        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", echo=echo, fast_executemany=True)

        try:
            engine.connect()
            return engine
        except Exception as E:
            print("\n\tProblem Connecting to Database")
            print(E)
            return False

    else:
        return False
