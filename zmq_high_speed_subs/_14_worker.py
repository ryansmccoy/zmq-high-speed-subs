"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import multiprocessing
import os
import time
from datetime import datetime
from queue import Empty

import numpy as np
import pandas as pd

pd.set_option("display.float_format", lambda x: "%.5f" % x)  # pandas
pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 100)
pd.set_option("display.width", 600)

from settings import BaseConfig

from zmq_high_speed_subs.message_handler import MessageHandler, array_columns

from zmq_high_speed_subs.utils import setup_db_connection, setup_logging

class Worker(MessageHandler, multiprocessing.Process):
    """
    The Worker's Sole Purpose in Life is to take Data from it's Queue, Format it, and then Insert into Database
    """
    name = "Worker"

    def __init__(self, queue, status_queue, kill_switch, interval_time=30):
        super().__init__()
        multiprocessing.Process.__init__(self)
        self.status_queue = status_queue
        self.interval_time = interval_time
        self.queue = queue
        self.kill_switch = kill_switch
        self.show_first_message = True
        self.table_name = f"{datetime.now().strftime('%Y%m%d')}_LVL1"
        self.initialize_counters()

    def run(self):
        """
        Starts worker that connects to the Pusher
        """
        self.initialize()

        self.engine = setup_db_connection(driver="Fake")
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()

        self.logger.debug("\n\n")
        self.logger.debug(f'Spawning Worker')
        self.logger.debug("\n\n")

        self.time_start_process = time.time()
        self.time_start_cycle = time.time()

        # -------------------------------
        #  Start Processing Data


        data_unprocessed = self.get_data_from_queue()

        df = pd.DataFrame()

        df = self.process_data(data_unprocessed)

        if not df.empty:
            self.insert_data_into_database(df)

        # -------------------------------

        self.check_status("COMPLETED")
        return

    def get_data_from_queue(self, init_array_size=5000):

        self.current_loop_iteration = 0
        self.current_loop_pause = 0
        count = 0

        # allocate array prior to appending
        temp_array = [()] * init_array_size

        while not self.kill_switch.is_set():

            try:
                packed = self.queue.get()

                if packed == "--END--":
                    break

                message = tuple(packed.decode().split(','))

                temp_array[count] = message
                count += 1

                self.check_status(message)

            except Empty as empty:
                self.logger.debug(f'')
                self.logger.debug(f'Current Loop Iteration:\ {self.current_loop_iteration}')
                self.logger.debug(f'Current Numpy Array:\t {count}')
                self.logger.debug(f'Waiting for more messages...')
                self.logger.debug(f'')
                time.sleep(0.01)

            except Exception as E:
                self.logger.debug(f"Empty Queue\t{E}")

        self.counter_messages_total += count
        self.counter_messages_period = count

        return temp_array

    def process_data(self, temp_array):

        df = pd.DataFrame(temp_array, columns=array_columns)

        return df

    def insert_data_into_database(self, df):

        try:
            self.logger.debug(f'Starting to Insert into Database')

            if self.engine:
                df.to_sql(name=self.table_name, con=self.engine, index=False, if_exists="append")
                self.logger.debug(f'Completed to Insert into Database')
            else:
                self.logger.debug(f'Couldnt find DB Connection. Not Saving Data')
                del df
            return

        except Exception as e:
            self.logger.error(e)


    def initialize_counters(self):
        # counters for benchmarking
        self.counter = 0
        self.counter_messages_current = 0
        self.counter_total_database = 0
        self.counter_total_manager = 0
        self.current_loop_iteration = 0
        self.counter_messages_total = 0
        self.counter_messages_period = 0
        self.counter_messages_period_sleep = 0
        self.counter_messages_current = 0
        self.counter_total_consumer = 0
        self.counter_messages_period_sleep = 0
        self.counter_messages_total = 0
        self.time_total = 0

    def check_status(self, message=None):

        self.current_loop_iteration += 1
        self.current_loop_pause += 1
        self.counter_total_database += 1
        self.counter_total_manager += 1
        self.counter_messages_period += 1
        self.counter_messages_total += 1

        if time.time() - self.time_start_cycle > self.interval_time or message == "COMPLETED":
            time_now = time.time()
            time_cycle = time_now - self.time_start_cycle
            self.time_total = self.time_total + time_cycle
            self.logger.info(f'Worker Messages Time Elapsed:\t{round(self.time_total, 2)} seconds')
            self.logger.debug(f'Worker Messages:\t{self.counter_total_manager}')
            self.logger.info(f'Worker Messages Per Second:\t{round((self.counter_total_manager / self.time_total), 2)}\n\n')
            self.logger.debug(f"{message}")
            self.time_start_cycle = time.time()
            self.logger.debug(f'\n\n')
            self.counter_messages_period = 0
            self.time_count = 0
            self.current_loop_pause = 0
            self.counter_messages_period_sleep = 0

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name
