"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import datetime
import multiprocessing

import time
from itertools import cycle
import pandas as pd

class FakeFeed:
    """Generates a Repeating Feed of Data for Benchmarking/Testing"""
    def __init__(self, sleep_time: (int or float) = 1, filepath=f'zeromq_high_speed_subscribers/test_data.csv'):
        self.filepath = filepath
        self.sleep_time = sleep_time

    def __iter__(self):
        df = pd.read_csv(self.filepath)
        df.reset_index(drop=True, inplace=True)
        counter = 0
        for idx, num in cycle(df.iterrows()):
            counter += 1
            msg = f"{counter},    " + ",".join(list(num.astype(str).to_dict().values()))
            yield msg
            time.sleep(self.sleep_time)


class MessageChecker:

    def initialize(self, stop_event):
        self.stop_event = stop_event
        self.logger = multiprocessing.get_logger()
        self.counter_messages_cum = 0
        self.counter_messages = 0
        self.status_update_counter = 0
        self.sleep_counter = 0
        self.show_messages = True

    def check_message(self, message):

        self.status_update_counter += 1
        self.counter_messages += 1
        self.sleep_counter += 1

        ##############################
        if self.show_messages:
            self.logger.info(message[0:45])
            self.time_start = time.time()
            self.show_messages = False

        if self.status_update_counter >= 10000:
            self.status_update_counter = 0

        if self.sleep_counter >= 50000:
            self.sleep_counter = 0

        if self.sleep_counter >= 50000:
            self.sleep_counter = 0

        # shutdown if receive stop message
        if message[0:4] == "stop":
            end_time = datetime.datetime.now()
            # the producer emits None to indicate that it is done
            self.logger.info(f'Shutdown Received\t{end_time}')
            self.logger.info(f'Start Time:\t{self.time_start}')
            time_end = time.time()
            total_time = time_end - self.time_start
            self.logger.info(f'Time Elapsed:\t{round(total_time, 2)} seconds')
            self.logger.info(f'Total Messages:\t{self.counter_messages}')
            self.logger.info(f'Messages Per Second:\t{round(self.counter_messages / total_time, 3)}')
            self.stop_event.set()
