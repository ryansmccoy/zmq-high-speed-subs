"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import logging
import zmq
import datetime
import multiprocessing
from utils import MessageHandler
import time

from utils import MessageHandler, setup_logging, initializer

class ZMQSubscriber(multiprocessing.Process, MessageHandler):
    def __init__(self, queue, kill_switch, host=r'127.0.0.1', port="5558", check_messages=True, show_messages=True):
        self.queue = queue

        self.url = f'tcp://{host}:{port}'
        self.kill_switch = kill_switch
        self.check_messages = check_messages
        self.show_messages = show_messages
        multiprocessing.Process.__init__(self)

    def send_shutdown(self):
        self.queue.put("stop")

    def run(self):
        self.initialize(show_messages=False, check_messages=True)
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()

        self.logger.info(f"Initializing ZMQ Subscriber")
        self.logger.info(f"Starting ZMQ \t{datetime.datetime.now()}")

        self.logger.info(f'\n\n')

        counter_messages_period = 0
        counter_total = 0
        time_start = time.time()

        self.ctx = zmq.Context()

        with self.ctx.socket(zmq.SUB) as self.zmq_socket:
            self.zmq_socket.setsockopt(zmq.SNDHWM, 100000)
            self.zmq_socket.setsockopt(zmq.RCVHWM, 100000)
            self.zmq_socket.setsockopt_string(zmq.SUBSCRIBE, "")
            self.zmq_socket.connect(self.url)

            # receive message and pipe to other process
            while not self.kill_switch.is_set():
                message = self.zmq_socket.recv()

                # if self.check_messages:
                self.check_message(message)
                counter_messages_period += 1
                counter_total += 1
                self.queue.put(message)

                if time.time() - time_start > 10:
                    time_now = time.time()
                    total_time = time_now - time_start
                    messages_per_second = round(counter_messages_period / total_time, 2)
                    self.logger.info(f'')
                    self.logger.info(f'')
                    self.logger.info(f'Time Elapsed:\t{round(total_time, 2)} seconds')
                    self.logger.info(f'Messages During Period:\t{self.counter_messages}')
                    self.logger.info(f'Messages Per Second:\t{messages_per_second}')
                    self.logger.info(f'Total Subscriber Messages:\t{counter_total}')
                    self.logger.info(f"Current Queue Size:\t{self.queue.qsize()}")
                    self.logger.info(f"{message[0:45]}")
                    time_start = time.time()
                    self.logger.info(f'')
                    self.logger.info(f'\n\n')
                    counter_messages_period = 0
                    time.sleep(1)


class ZMQPusher(multiprocessing.Process, MessageHandler):
    def __init__(self, queue, kill_switch, host=r'127.0.0.1', port="5559", check_messages=True, show_messages=True):
        self.queue = queue

        self.url = f'tcp://{host}:{port}'
        self.kill_switch = kill_switch
        self.check_messages = check_messages
        self.show_messages = show_messages
        multiprocessing.Process.__init__(self)

    def send_shutdown(self):
        with self.ctx.socket(zmq.PUSH) as self.zmq_socket:
            self.zmq_socket.bind(self.url)
            print("Sending Shutdown")
            for x in range(10):
                self.zmq_socket.send_string("stop")

    def run(self):
        self.initialize(show_messages=False, check_messages=True)
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()
        self.logger.info("Initializing ZMQ Pusher")
        self.logger.info(f"Starting ZMQ \t{datetime.datetime.now()}")
        self.logger.info(f'\n\n')

        message = ""
        time_start = time.time()
        counter_messages_period = 0
        counter_total = 0

        self.ctx = zmq.Context()

        with self.ctx.socket(zmq.PUSH) as self.zmq_socket:
            self.zmq_socket.setsockopt(zmq.SNDHWM, 10000)
            self.zmq_socket.setsockopt(zmq.RCVHWM, 10000)
            self.zmq_socket.bind(self.url)
            while not self.kill_switch.is_set():
                if not self.queue.empty():

                    # receive message and push to worker processes
                    message = self.queue.get()
                    # flag to turn on and off check messages
                    # if self.check_messages:
                    self.check_message(message)
                    counter_messages_period += 1
                    counter_total += 1
                    self.zmq_socket.send(message)

                else:
                    time.sleep(0.1)

                if time.time() - time_start > 10:
                    time_now = time.time()
                    total_time = time_now - time_start
                    messages_per_second = round(counter_messages_period / total_time, 2)
                    self.logger.info(f'')
                    self.logger.info(f'')
                    self.logger.info(f'Time Elapsed:\t{round(total_time, 2)} seconds')
                    self.logger.info(f'Messages During Period:\t{self.counter_messages}')
                    self.logger.info(f'Messages Per Second:\t{messages_per_second}')
                    self.logger.info(f'Total Pusher Messages:\t{counter_total}')
                    self.logger.info(f"Current Queue Size:\t{self.queue.qsize()}")
                    self.logger.info(f"{message[0:45]}")
                    time_start = time.time()
                    self.logger.info(f'')
                    self.logger.info(f'\n\n')
                    counter_messages_period = 0
                    time.sleep(1)


if __name__ == "__main__":

    # multiprocessing.log_to_stderr(logging.DEBUG)
    initializer(logging.DEBUG)

    queue = multiprocessing.Queue()
    kill_switch = multiprocessing.Event()

    process_subscriber = ZMQSubscriber(queue, kill_switch, show_messages=False)
    process_pusher = ZMQPusher(queue, kill_switch, show_messages=False)

    process_subscriber.start()
    process_pusher.start()

    try:
        process_pusher.join()
        process_subscriber.join()
    except KeyboardInterrupt:
        print('parent received ctrl-c')
        process_subscriber.terminate()
        process_pusher.terminate()
        process_subscriber.join()
        process_pusher.join()
