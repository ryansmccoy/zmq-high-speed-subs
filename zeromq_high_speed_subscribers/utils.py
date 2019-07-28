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
import datetime
import multiprocessing

import time
from itertools import cycle
import pandas as pd

class MessageTransformer:
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

    def process_message(self, message):
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
    def __init__(self, sleep_time: (int or float) = 1.0, filepath=f'zeromq_high_speed_subscribers/test_data.csv'):
        self.filepath = filepath
        self.sleep_time = sleep_time

    def __iter__(self):
        try:
            df = pd.read_csv(self.filepath)
        except:
            df = pd.read_csv(os.path.basename(self.filepath))

        df.reset_index(drop=True, inplace=True)
        counter = 0
        for idx, df_ in cycle(enumerate(df.index)):
            counter += 1
            msg = f"{counter}," + ",".join(list(df.iloc[idx,:].astype(str).to_dict().values()))
            # print(msg)
            yield msg
            time.sleep(self.sleep_time)

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
            msg = f"{counter}," + ",".join(list(chunk.iloc[0,:].astype(str).to_dict().values()))
            print(msg)
            yield msg
            time.sleep(self.sleep_time)

class MessageHandler:

    def initialize(self, show_messages=False, check_messages=True, status_update=True):
        self.logger = multiprocessing.get_logger()
        self.counter_messages_cum = 0
        self.counter_messages = 0
        self.status_update_counter = 0
        self.sleep_counter = 0
        self.queue_update_counter = 0
        self.show_messages = show_messages
        self.check_messages = check_messages
        self.status_update = status_update

    def update_counters(self):

        self.status_update_counter += 1
        self.counter_messages += 1
        self.sleep_counter += 1
        self.queue_update_counter += 1

    def check_message(self, message):

        self.update_counters()

        if not self.check_messages:
            return
        ##############################
        if self.show_messages:
            self.show_message(message)

        if self.status_update and self.status_update_counter >= 1000:
            self.status_update_display(message)
            self.status_update_counter = 0

        if self.sleep_counter >= 50000:
            self.sleep_counter = 0

        # shutdown if receive stop message
        if message[0:4] == "stop":
            self.stop_message(message)

    def show_message(self, message):
        # self.logger.info(f"Show Message:\t{message[0:45]}")
        self.time_start = time.time()
        self.show_messages = False

    def status_update_display(self, message):
        # self.logger.info(f"Status Count:\t{self.counter_messages}\t{message[0:5]}")
        self.status_update_counter = 0

    def stop_message(self, message):
        end_time = datetime.datetime.now()
        # the producer emits None to indicate that it is done
        self.logger.info(f'Shutdown Received\t{end_time}')
        self.logger.info(f'Start Time:\t{self.time_start}')
        time_end = time.time()
        total_time = time_end - self.time_start
        self.logger.info(f'Time Elapsed:\t{round(total_time, 2)} seconds')
        self.logger.info(f'Total Messages:\t{self.counter_messages}')
        self.logger.info(f'Messages Per Second:\t{round(self.counter_messages / total_time, 3)}')

def setup_logging():
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)
    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(asctime)s %(levelname)-8s [%(processName)-10s(%(process)d)] %(message)s')
    c_handler.setFormatter(c_format)
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

        print(sql_db_connection)

        params = urllib.parse.quote_plus(sql_db_connection)

        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", echo=echo, fast_executemany=True)

        try:
            engine.connect()
            return engine
        except Exception as E:
            print("\n\tProblem Connecting to Database")
            print(E)
            return False
