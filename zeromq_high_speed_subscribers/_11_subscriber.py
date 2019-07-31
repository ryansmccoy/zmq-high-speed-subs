import datetime
import multiprocessing
import time
from datetime import datetime

import zmq

from utils import MessageHandler, setup_logging

class ZMQSubscriberAsync:
    def __init__(self, port="5558"):
        self.ctx = zmq.asyncio.Context()
        self.sock = self.ctx.socket(zmq.SUB)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, "")
        self.port = port
        self.counter = 0
        self.counter_print = 0
        print(f"\n\tStarting Subscriber on {port}")

    async def handle(self, queue):
        self.sock.connect(f'tcp://127.0.0.1:{self.port}')
        self.counter += 0

        while True:
            msg = await self.sock.recv_string()
            # print(f"SUB -> {msg[0:100]}")
            await queue.put(msg)


class ZMQSubscriberPipe(multiprocessing.Process, MessageHandler):
    def __init__(self, pipe_connection, stop_event, host=r'127.0.0.1', port="5558", check_messages=True):
        self.pipe = pipe_connection

        self.url = f'tcp://{host}:{port}'
        self.stop_event = stop_event
        self.check_messages = check_messages
        multiprocessing.Process.__init__(self)

    def run(self):
        self.initialize(self.stop_event)
        self.logger = multiprocessing.get_logger()
        self.logger.info(f"Starting ZMQ \t{datetime.now()}")

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


class ZMQSubscriberQueue(multiprocessing.Process, MessageHandler):
    def __init__(self, queue, kill_switch,bind=True,host=r'127.0.0.1', port="5558", interval_time=10):
        self.queue = queue
        self.interval_time = interval_time

        self.url = f'tcp://{host}:{port}'
        self.kill_switch = kill_switch
        multiprocessing.Process.__init__(self)

    def send_shutdown(self):
        self.queue.put("stop")

    def run(self):
        self.initialize(show_messages=False, check_messages=True)
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()

        self.logger.info(f"Initializing ZMQ Subscriber")
        self.logger.info(f"Starting ZMQ \t{datetime.now()}")

        self.logger.info(f'\n\n')

        counter_messages_period = 0
        counter_total = 0
        time_start = time.time()
        time_total = 0

        self.ctx = zmq.Context()

        with self.ctx.socket(zmq.SUB) as self.zmq_socket:

            self.zmq_socket.setsockopt(zmq.SNDHWM, 100000)
            self.zmq_socket.setsockopt(zmq.RCVHWM, 100000)
            self.zmq_socket.setsockopt_string(zmq.SUBSCRIBE, "")

            # self.zmq_socket.bind(self.url)
            self.zmq_socket.connect(self.url)

            # receive message and pipe to other process
            while not self.kill_switch.is_set():
                message = self.zmq_socket.recv()

                # if self.check_messages:
                self.check_message(message)
                counter_messages_period += 1
                counter_total += 1

                self.queue.put(message)

                if time.time() - time_start > self.interval_time:
                    time_now = time.time()
                    time_period = time_now - time_start
                    time_total = time_total + time_period
                    messages_per_second = round(counter_messages_period / time_period, 2)

                    self.logger.info(f'')
                    self.logger.info(f'Time Elapsed:\t{round(self.interval_time, 2)} seconds')
                    self.logger.info(f'Messages During Period:\t{self.counter_messages}')
                    self.logger.info(f'Messages Per Second:\t{messages_per_second}')

                    self.logger.info(f'')
                    self.logger.info(f'Total Time Elapsed:\t{round(time_total, 2)} seconds')
                    self.logger.info(f'Total Messages:\t{counter_total}')
                    self.logger.info(f'Total Messages Per Second:\t{round((counter_total / time_total), 2)}')
                    self.logger.info(f'')
                    self.logger.info(f"Current Queue Size:\t{self.queue.qsize()}")
                    self.logger.info(f'')
                    self.logger.info(f"{message[0:45]}")

                    time_start = time.time()
                    self.logger.info(f'')
                    self.logger.info(f'\n\n')
                    counter_messages_period = 0
                    # time.sleep(0.1)
