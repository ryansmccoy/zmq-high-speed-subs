"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import asyncio
import multiprocessing
import time
from datetime import datetime

import zmq

from message_handler import MessageHandler
from utils import setup_logging



class ZMQPusherQueue(multiprocessing.Process, MessageHandler):
    def __init__(self, queue, kill_switch, host=r'127.0.0.1', port="5559", interval_time=10, check_messages=False, show_messages=False):
        self.queue = queue
        self.interval_time = interval_time
        self.url = f'tcp://{host}:{port}'
        self.kill_switch = kill_switch
        self.check_messages = check_messages
        self.show_messages = show_messages
        multiprocessing.Process.__init__(self)

    def send_shutdown(self):
        with self.ctx.socket(zmq.PUSH) as self.zmq_socket:
            self.zmq_socket.bind(self.url)
            self.logger.info("Sending Shutdown")
            for x in range(10):
                self.zmq_socket.send_string("stop")

    def run(self):
        # self.initialize(show_messages=False, check_messages=False)
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()
        self.logger.info("Initializing ZMQ Pusher")
        self.logger.info(f"Starting ZMQ \t{datetime.now()}")
        self.logger.info(f'\n\n')

        message = ""

        time_start = time.time()
        counter_messages_period = 0
        counter_total = 0
        time_total = 0

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
                    # self.check_message(message)
                    counter_messages_period += 1
                    counter_total += 1
                    self.zmq_socket.send(message)

                else:
                    time.sleep(0.1)

                if time.time() - time_start > self.interval_time:
                    time_now = time.time()
                    time_period = time_now - time_start
                    time_total = time_total + time_period
                    messages_per_second = round(counter_messages_period / time_period, 2)

                    self.logger.info(f'')
                    self.logger.info(f'Time Elapsed:\t{round(self.interval_time, 2)} seconds')
                    self.logger.info(f'Messages During Period:\t{counter_messages_period}')
                    self.logger.info(f'Messages Per Second:\t{messages_per_second}')

                    self.logger.info(f'')
                    self.logger.info(f'Total Time Elapsed:\t{round(time_total, 2)} seconds')
                    self.logger.info(f'Total Messages:\t{counter_total}')
                    self.logger.info(f'Total Messages Per Second:\t{round((counter_total / time_total), 2)}')
                    self.logger.info(f'')
                    self.logger.info(f"Current _Queue Size:\t{self.queue.qsize()}")
                    self.logger.info(f'')
                    self.logger.info(f"{message[0:45]}")
                    time_start = time.time()
                    self.logger.info(f'')
                    self.logger.info(f'\n\n')
                    counter_messages_period = 0
                    # time.sleep(1)

#
# class ZMQPusherAsync:
#     """
#     Not Entirely sure I'm doing Async Correctly
#     """
#
#     def __init__(self, port="5559"):
#         self.ctx = zmq.asyncio.Context()
#         self.push_socket = self.ctx.socket(zmq.PUSH)
#         self.port = port
#         self.counter = 0
#         self.counter_print = 0
#         self.shutdown_messages = ["stop"] * 5
#         self.logger.info(f"\n\tStarting Pusher on {port}")
#
#     async def handle(self, queue):
#         self.push_socket.bind(f'tcp://*:{self.port}')
#
#         while True:
#             msg = await queue.get()
#             self.counter += 1
#             self.counter_print += 1
#
#             if self.counter_print >= 1000:
#                 self.logger.info(f'{datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")}')
#                 self.logger.info(f"\n[INFO/ZMQPusher] ->\t {msg[0:100]}")
#                 self.counter_print = 0
#
#             if msg[0:4] == "stop":
#                 # the producer emits None to indicate that it is done
#                 [self.push_socket.send_string(message) for message in self.shutdown_messages]
#                 self.logger.info("\nStarting Shutdown...")
#                 await asyncio.sleep(10)
#                 self.push_socket.close()
#                 self.ctx.term()
#                 return
#             else:
#                 # self.logger.info(f"PUSH -> {msg[0:100]}")
#                 self.push_socket.send_string(msg)
#                 queue.task_done()
#
#
# class ZMQPusherPipe(multiprocessing.Process, MessageHandler):
#     def __init__(self, pipe_connection, stop_event, host=r'127.0.0.1', port="5559", check_messages=False):
#         self.url = f'tcp://{host}:{port}'
#         self.pipe = pipe_connection
#         self.stop_event = stop_event
#         self.check_messages = check_messages
#         multiprocessing.Process.__init__(self)
#
#     def run(self):
#         self.initialize(self.stop_event)
#         self.logger = multiprocessing.get_logger()
#         self.logger.info("Initializing ZMQSubscriber")
#         self.logger.info(f"Starting ZMQ \t{datetime.now()}")
#
#         ctx = zmq.Context()
#         with ctx.socket(zmq.PUSH) as zmq_socket:
#             zmq_socket.setsockopt(zmq.SNDHWM, 10000)
#             zmq_socket.setsockopt(zmq.RCVHWM, 10000)
#             zmq_socket.bind(self.url)
#             while not self.stop_event.is_set():
#
#                 # receive message and push to worker processes
#                 message = self.pipe.recv()
#
#                 # flag to turn on and off check messages
#                 if self.check_messages:
#                     self.check_message(message)
#
#                 # zmq_socket.send_string(message)
#                 zmq_socket.send(message)
#
#             # send stop messages to potential workers
#             for x in range(10):
#                 # zmq_socket.send_string(message)
#                 # zmq_socket.send(message)
#                 zmq_socket.send(message)
