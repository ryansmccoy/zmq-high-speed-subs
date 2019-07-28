"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import multiprocessing
import datetime
import time
from datetime import datetime

import zmq
import msgpack
from utils import FakeFeedSQL, FakeFeedCSV
from utils import setup_logging,initializer
import logging

class FeedPublisher(multiprocessing.Process):
    def __init__(self,kill_switch, sleep_time=1.0, fake_feed="CSV", host="*", port="5558"):
        self.kill_switch = kill_switch
        self.sleep_time = sleep_time
        self.fake_feed=fake_feed
        self.url = f'tcp://{host}:{port}'
        multiprocessing.Process.__init__(self)

    def send_shutdown(self):
        with self.ctx.socket(zmq.PUB) as self.zmq_socket:
            self.zmq_socket.connect(self.url)
            print("Sending Shutdown")
            for x in range(10):
                self.zmq_socket.send_string("stop")

    def run(self):
        """
        Publishes a constant stream of Data to the listening Subscribers
        """
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()
        self.logger.info("Initializing ZMQ Publisher")
        self.logger.info(f"Starting ZMQ \t{datetime.now()}")
        self.logger.info(f'\n\n')

        if self.fake_feed == "CSV":
            fakefeed = FakeFeedCSV(sleep_time=self.sleep_time)
        elif self.fake_feed == "SQL":
            fakefeed = FakeFeedSQL(sleep_time=self.sleep_time)
        else:
            return

        self.ctx = zmq.Context()
        with self.ctx.socket(zmq.PUB) as self.zmq_socket:
            self.zmq_socket.setsockopt(zmq.SNDHWM, 100000)
            self.zmq_socket.setsockopt(zmq.RCVHWM, 100000)
            self.zmq_socket.bind(self.url)

            start_time = datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")
            self.logger.info(f'Starting FakeFeed{self.fake_feed}\t{start_time}')
            counter_messages_total = 0
            counter_messages_period = 0
            time_start = time.time()

            while not self.kill_switch.is_set():

                for item in fakefeed:

                    timestamp = datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")

                    message = "run," + timestamp + item
                    packed = msgpack.packb(message)
                    self.zmq_socket.send(packed)

                    counter_messages_period += 1
                    counter_messages_total += 1

                    if time.time() - time_start > 10:
                        time_now = time.time()
                        total_time = time_now - time_start
                        messages_per_second = round(counter_messages_period / total_time, 2)
                        self.logger.info(f'')
                        self.logger.info(f"{message[0:25]}")
                        self.logger.info(f'')
                        self.logger.info(f'Time Elapsed:\t{round(total_time, 2)} seconds')
                        self.logger.info(f'Messages During Period:\t{counter_messages_period}')
                        self.logger.info(f'Messages Per Second:\t{messages_per_second}')
                        self.logger.info(f'')
                        self.logger.info(f'Total Messages Published :\t{counter_messages_total}')
                        self.logger.info(f'')
                        self. logger.info(f'\n\n')
                        counter_messages_period = 0
                        time_start = time.time()

                if self.kill_switch.is_set:
                    break


        self.send_shutdown()


if __name__ == "__main__":
    initializer(logging.DEBUG)

    kill_switch = multiprocessing.Event()
    fake_feed = FeedPublisher(kill_switch, sleep_time=0.00001, fake_feed="CSV", port="5558")
    fake_feed.start()

    try:
        fake_feed.join()
    except KeyboardInterrupt as e :
        print('\nSTOP')
        kill_switch.set()
    except Exception as e:
        kill_switch.set()
    finally:
        time.sleep(5)



