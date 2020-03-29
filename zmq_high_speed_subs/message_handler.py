import datetime
import multiprocessing
import time
import logging

logger = logging.getLogger(__name__)

class MessageHandler:

    _field_dtype = [('Symbol', 'S128'), ('Ask', '<f8'),
                    ('Ask Change', '<f8'), ('Ask Market Center', 'u1'),
                    ('Ask Size', '<u8'), ('Ask Time', '<u8'), ('Available Regions', 'S128'),
                    ('Average Maturity', '<f8'), ('Bid', '<f8'), ('Bid Change', '<f8'),
                    ('Bid Market Center', 'u1'), ('Bid Size', '<u8'), ('Bid Time', '<u8'),
                    ('Change', '<f8'), ('Change From Open', '<f8'),
                    ('Close', '<f8'), ('Close Range 1', '<f8'), ('Close Range 2', '<f8'),
                    ('Days to Expiration', '<u2'), ('Decimal Precision', 'u1'), ('Delay', 'u1'),
                    ('Exchange ID', 'u1'), ('Extended Price', '<f8'),
                    ('Extended Trade Date', '<M8[D]'), ('Extended Trade Market Center', 'u1'),
                    ('Extended Trade Size', '<u8'), ('Extended Trade Time', '<u8'),
                    ('Extended Trading Change', '<f8'),
                    ('Extended Trading Difference', '<f8'), ('Financial Status Indicator', 'S1'),
                    ('Fraction Display Code', 'u1'), ('High', '<f8'), ('Last', '<f8'), ('Last Date', '<M8[D]'),
                    ('Last Market Center', 'u1'),
                    ('Last Size', '<u8'), ('Last Time', '<u8'), ('Low', '<f8'), ('Market Capitalization', '<f8'),
                    ('Market Open', '?'), ('Message Contents', 'S9'), ('Most Recent Trade', '<f8'),
                    ('Most Recent Trade Conditions', 'S16'), ('Most Recent Trade Date', '<M8[D]'),
                    ('Most Recent Trade Market Center', 'u1'), ('Most Recent Trade Size', '<u8'),
                    ('Most Recent Trade Time', '<u8'),
                    ('Net Asset Value', '<f8'), ('Number of Trades Today', '<u8'), ('Open', '<f8'),
                    ('Open Interest', '<u8'), ('Open Range 1', '<f8'), ('Open Range 2', '<f8'),
                    ('Percent Change', '<f8'),
                    ('Percent Off Average Volume', '<f8'), ('Previous Day Volume', '<u8'),
                    ('Price-Earnings Ratio', '<f8'), ('Range', '<f8'), ('Restricted Code', '?'),
                    ('Settle', '<f8'), ('Settlement Date', '<M8[D]'),
                    ('Spread', '<f8'), ('7 Day Yield', '<f8'), ('Tick', '<i8'),
                    ('TickId', '<u8'), ('Total Volume', '<u8'), ('VWAP', '<f8'), ('Volatility', '<f8')]

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
