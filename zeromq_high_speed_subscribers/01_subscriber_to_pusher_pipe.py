"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import datetime
import multiprocessing
import logging.config
import zmq
import msgpack
from utils import MessageHandler

class ZMQSubscriber(multiprocessing.Process, MessageHandler):
    def __init__(self, pipe_connection, stop_event, host=r'127.0.0.1', port="5558", check_messages=True):
        self.pipe = pipe_connection

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
            # zmq_socket.setsockopt(zmq.SUBSCRIBE, "")
            zmq_socket.connect(self.url)

            # receive message and pipe to other process
            while not self.stop_event.is_set():
                # message = zmq_socket.recv_string()
                message = zmq_socket.recv()

                if self.check_messages:
                    self.check_message(message)

                self.pipe.send(message)

class ZMQPusher(multiprocessing.Process, MessageHandler):
    def __init__(self, pipe_connection, stop_event, host=r'127.0.0.1', port="5559", check_messages=True):
        self.url = f'tcp://{host}:{port}'
        self.pipe = pipe_connection
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
            zmq_socket.setsockopt(zmq.SNDHWM, 10000)
            zmq_socket.setsockopt(zmq.RCVHWM, 10000)
            zmq_socket.bind(self.url)
            while not self.stop_event.is_set():

                # receive message and push to worker processes
                message = self.pipe.recv()

                # flag to turn on and off check messages
                if self.check_messages:
                    self.check_message(message)

                # zmq_socket.send_string(message)
                zmq_socket.send(message)

            # send stop messages to potential workers
            for x in range(10):
                # zmq_socket.send_string(message)
                # zmq_socket.send(message)
                zmq_socket.send(message)


if __name__ == "__main__":

    multiprocessing.log_to_stderr(logging.DEBUG)

    (pipe_pusher, pipe_subscriber) = multiprocessing.Pipe(duplex=False)

    stop_event = multiprocessing.Event()

    process_subscriber = ZMQSubscriber(pipe_subscriber, stop_event)
    process_pusher = ZMQPusher(pipe_pusher, stop_event)

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
