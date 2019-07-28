"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""

import datetime
import time
from datetime import datetime

import zmq
import msgpack
from utils import FakeFeedSQL, FakeFeedCSV
from utils import setup_logging
import logging

def fake_feed_publisher(sleep_time=1.0, fake_feed="CSV", port="5558"):
    """
    Publishes a constant stream of Data to the listening Subscribers
    """
    logger = logging.getLogger(__name__)

    logger.addHandler(setup_logging())
    logger.setLevel(logging.DEBUG)

    if fake_feed == "CSV":
        fakefeed = FakeFeedCSV(sleep_time=sleep_time)
    elif fake_feed == "SQL":
        fakefeed = FakeFeedSQL(sleep_time=sleep_time)
    else:
        return

    context = zmq.Context()
    socket = context.socket(zmq.PUB)

    socket.bind(f"tcp://*:{port}")

    socket.setsockopt(zmq.SNDHWM, 100000)
    socket.setsockopt(zmq.RCVHWM, 100000)

    start_time = datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")
    logger.info(f'Starting FakeFeed{fake_feed}\t{start_time}')

    counter_messages_total = 0
    counter_messages_period = 0
    time_start = time.time()

    while True:
        for item in fakefeed:

            timestamp = datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")

            message = "run," + timestamp + item
            packed = msgpack.packb(message)
            socket.send(packed)

            counter_messages_period += 1
            counter_messages_total += 1

            if time.time() - time_start > 10:
                time_now = time.time()
                total_time = time_now - time_start
                messages_per_second = round(counter_messages_period / total_time, 2)
                logger.info(f'')
                logger.info(f"{message[0:25]}")
                logger.info(f'')
                logger.info(f'Time Elapsed:\t{round(total_time, 2)} seconds')
                logger.info(f'Messages During Period:\t{counter_messages_period}')
                logger.info(f'Messages Per Second:\t{messages_per_second}')
                logger.info(f'')
                logger.info(f'Total Messages Published :\t{counter_messages_total}')
                logger.info(f'')
                logger.info(f'\n\n')
                counter_messages_period = 0
                time_start = time.time()

if __name__ == "__main__":

    fake_feed_publisher(sleep_time=0.001, fake_feed="CSV", port="5558")
