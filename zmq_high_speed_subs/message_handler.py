import datetime
import multiprocessing
import time
import logging

logger = logging.getLogger(__name__)

array_columns = ['timestamp_received','symbol', '7_day_yield', 'ask', 'ask_change', 'ask_market_center', 'ask_size', 'ask_time', 'available_regions', 'average_maturity', 'bid', 'bid_change', 'bid_market_center', 'bid_size', 'bid_time', 'change', 'change_from_open', 'close', 'close_range_1', 'close_range_2', 'days_to_expiration', 'decimal_precision', 'delay', 'exchange_id', 'extended_trade', 'extended_trade_date', 'extended_trade_market_center', 'extended_trade_size', 'extended_trade_time', 'extended_trading_change', 'extended_trading_difference', 'financial_status_indicator', 'fraction_display_code', 'high', 'last', 'last_date', 'last_market_center', 'last_size', 'last_time', 'low', 'market_capitalization', 'market_open', 'message_contents', 'most_recent_trade', 'most_recent_trade_conditions', 'most_recent_trade_date', 'most_recent_trade_market_center', 'most_recent_trade_size', 'most_recent_trade_time', 'net_asset_value', 'number_of_trades_today', 'open', 'open_interest', 'open_range_1', 'open_range_2', 'percent_change', 'percent_off_average_volume', 'previous_day_volume', 'price-earnings_ratio', 'range', 'restricted_code', 'settle', 'settlement_date', 'spread', 'tick', 'tickid', 'total_volume', 'volatility', 'vwap', 'timestamp_inserted']

packed = b'SPY,,328.11,0.01,5,600,30207883071,,,328.09,0.01,26,800,30207883071,0,,324.12,,,0,4,0,7,,00:00.0,5,500,30205418926,3.99,3.99,N,14,,324.12,00:00.0,11,100,72000003024,,309382912,0,ba,328.11,173D,00:00.0,5,500,30205418926,,2183,,0,,,0,-0.984107,69242293,,,0,,00:00.0,0.02,0,,1063916,,327.7901,23:16.2'

class MessageHandler:

    def initialize(self, show_messages=False, check_messages=True, status_update=True):
        self._current_update_fields = []
        self._update_names = []
        self._update_reader = []
        self._num_update_fields = 0
        self.time_count = 0
        self.time_flush = False
        self.fundamental_messages = []
        self.summary_messages = []
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
        self.sleep_counter += 1

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


if __name__ == "__main__":
    from dotenv import load_dotenv
    import zmq_high_speed_subs.utils as utils

    load_dotenv()

    engine = utils.setup_db_connection()
    sql = """SELECT TOP (5) * FROM [20190722_LVL1_Q]"""
    import pandas as pd
    import numpy as np

    # daily 22,599,788
    for chunk in pd.read_sql_query(sql, engine, chunksize=5):
        df = chunk
        break
    _empty_update_msg = np.zeros(1, dtype=_field_dtype)
    df
    values = df.iloc[0, :].values
    new_message = ",".join(str(x) for x in list(values))

    process = MessageValidator()
    fields = process.process_message(new_message)

    df = pd.DataFrame(fields)
