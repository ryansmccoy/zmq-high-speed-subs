"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import logging
import zmq
import datetime
import multiprocessing
from utils import MessageChecker

class ZMQSubscriber(multiprocessing.Process, MessageChecker):
    def __init__(self, queue, stop_event, host=r'127.0.0.1', port="5558", check_messages=True):
        self.queue = queue

        self.url = f'tcp://{host}:{port}'
        self.stop_event = stop_event
        self.check_messages = check_messages
        multiprocessing.Process.__init__(self)

    def run(self):
        self.initialize(self.stop_event)
        self.logger = multiprocessing.get_logger()
        self.logger.info(f"Starting ZMQ \t{datetime.datetime.now()}")

        ctx = zmq.Context()
        with ctx.socket(zmq.SUB) as zmq_socket:
            zmq_socket.setsockopt(zmq.SNDHWM, 100000)
            zmq_socket.setsockopt(zmq.RCVHWM, 100000)
            zmq_socket.setsockopt_string(zmq.SUBSCRIBE, "")
            zmq_socket.connect(self.url)

            # receive message and pipe to other process
            while not self.stop_event.is_set():
                message = zmq_socket.recv_string()

                if self.check_messages:
                    self.check_message(message)

                self.queue.put(message)

class ZMQPusher(multiprocessing.Process, MessageChecker):
    def __init__(self, queue, stop_event, host=r'127.0.0.1', port="5559", check_messages=True):
        self.queue = queue

        self.url = f'tcp://{host}:{port}'
        self.stop_event = stop_event
        self.check_messages = check_messages
        multiprocessing.Process.__init__(self)

    def run(self):
        self.initialize(self.stop_event)
        self.logger = multiprocessing.get_logger()
        self.logger.info("Initializing ZMQSubscriber")
        self.logger.info(f"Starting ZMQ \t{datetime.datetime.now()}")

        ctx = zmq.Context()
        with ctx.socket(zmq.PUSH) as zmq_socket:
            zmq_socket.setsockopt(zmq.SNDHWM, 100000)
            zmq_socket.setsockopt(zmq.RCVHWM, 100000)
            zmq_socket.bind(self.url)
            while not self.stop_event.is_set():

                # receive message and push to worker processes
                message = self.queue.get()

                # flag to turn on and off check messages
                if self.check_messages:
                    self.check_message(message)

                zmq_socket.send_string(message)

            # send stop messages to potential workers
            for x in range(10):
                zmq_socket.send_string(message)

if __name__ == "__main__":

    multiprocessing.log_to_stderr(logging.DEBUG)

    queue = multiprocessing.Queue()
    stop_event = multiprocessing.Event()

    process_subscriber = ZMQSubscriber(queue, stop_event)
    process_pusher = ZMQPusher(queue, stop_event)

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
