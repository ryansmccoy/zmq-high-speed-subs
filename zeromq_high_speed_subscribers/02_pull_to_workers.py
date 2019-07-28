"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import sys
import os
from datetime import datetime
import time
import logging
import multiprocessing
from multiprocessing import JoinableQueue
from multiprocessing import Queue
from queue import Empty

import msgpack
import numpy as np
import pandas as pd
import zmq
from dotenv import load_dotenv

load_dotenv()
try:
    from arrayqueues.shared_arrays import ArrayQueue
except Exception as e:
    print(e)

    sys.exit(0)

try:
    from message_transformer import MessageTransformer
except:
    from utils import MessageTransformer

from utils import MessageHandler, setup_logging, initializer, setup_db_connection

# set the date and time format
date_format = "%m-%d-%Y %H:%M:%S"

class MessageConsumer(multiprocessing.Process):
    def __init__(self, messageQueue, databaseQueue, stopEventQueue):
        multiprocessing.Process.__init__(self)
        self.messageQueue = messageQueue
        self.databaseQueue = databaseQueue
        self.stopEventQueue = stopEventQueue

        self.show_first_message = True
        self.counter_messages_current = 0
        self.counter_total_consumer = 0
        self.time_start = 0

    def run(self):
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()

        self.logger.info("")
        self.logger.info("")
        self.logger.info(f'\tSpawning MessageConsumer')
        self.logger.info("")
        self.logger.info("")

        process = MessageTransformer()

        current_loop_iteration = 0
        counter_messages_total = 0
        counter_period_begin = 0
        counter = 0

        time_start = time.time()
        while True:
            try:
                stopEvent = self.stopEventQueue.get(block=False)
                if stopEvent is None:
                    self.logger.info(f"Stop Event Triggered. Terminating...")
                    self.stopEventQueue.task_done()
                    break
            except Empty:
                message = self.messageQueue.get()
                self.counter_total_consumer += 1

                # If consumer has met poison pill, stop working.
                if message is None:
                    self.messageQueue.task_done()
                    self.logger.info(f"Stop Event Triggered. Terminating...")
                    break

                """
                Starts worker that connects to the Pusher
                """

                current_loop_iteration += 1

                if message:
                    np_array = process.process_message(message)
                else:
                    np_array = None

                if np_array:
                    self.databaseQueue.put(np_array)

                counter += 1
                counter_messages_total += 1

                if time.time() - time_start > 10:
                    time_now = time.time()
                    total_counts_in_period = counter - counter_period_begin
                    total_time = time_now - time_start
                    messages_per_second = round((total_counts_in_period * 6) / (total_time * 6), 2)
                    self.logger.info(f'')
                    self.logger.info(f'')
                    self.logger.info(f'Time Elapsed:\t{round(total_time, 2)} seconds')
                    self.logger.info(f'Messages During Period:\t{total_counts_in_period}')
                    self.logger.info(f'Messages Per Second:\t{messages_per_second}')
                    self.logger.info(f'')
                    self.logger.info(f'Total Consumer Messages:\t{self.counter_total_consumer}')
                    self.logger.info(f'')
                    self.logger.info(f"Current Queue Size:\t{self.messageQueue.qsize()}")
                    time_start = time.time()
                    counter_period_begin = int(counter)
                    self.logger.info(f'')
                    self.logger.info(f'\n\n')


class DatabaseConsumer(multiprocessing.Process):
    """
    Sole Purpose is to take Data From Queue and Insert into Database
    """
    def __init__(self, databaseQueue, resultQueue, stopEventQueue):
        multiprocessing.Process.__init__(self)
        self.databaseQueue = databaseQueue
        self.resultQueue = resultQueue
        self.stopEventQueue = stopEventQueue

        self.show_first_message = True
        self.counter_messages_current = 0
        self.counter_total_database = 0
        self.time_start = 0

    def run(self):
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()

        process = MessageTransformer()
        header = [head.lower().replace(" ", "_") for head in process._update_fields_list]

        engine = setup_db_connection()

        counter_messages_total = 0
        time_start = time.time()

        self.logger.info("")
        self.logger.info("")
        self.logger.info(f'Spawning DatabaseConsumer')
        self.logger.info("")
        self.logger.info("")

        while True:
            counter_messages_period = 0

            try:
                stopEvent = self.stopEventQueue.get(block=False)
                if stopEvent is None:
                    self.logger.info(f"Stop Event Triggered. Terminating...")
                    self.stopEventQueue.task_done()
                    break
            except Empty:

                np_array = np.zeros([], dtype=process._update_dtype)
                current_loop_iteration = 0
                while current_loop_iteration < 10000:
                    try:
                        array = self.databaseQueue.get(block=False)
                        np_array = np.append(np_array, array[0])
                        current_loop_iteration += 1
                        self.counter_total_database += 1
                    except Empty as empty:
                        time.sleep(1)
                        continue
                    except Exception as E:
                        self.logger.info(f"Empty Queue\t{E}")

                """
                Starts worker that connects to the Pusher
                """

                # check if array is greater than 1 in size (created with 1 row)
                if np_array.size > 1:
                    try:
                        df = pd.DataFrame(np_array[1:], columns=process._update_fields_list)
                        df.columns = header
                        df['symbol'] = df['symbol'].str.decode("utf-8")
                        df['available_regions'] = df['available_regions'].str.decode("utf-8")
                        df['message_contents'] = df['message_contents'].str.decode("utf-8")
                        df['financial_status_indicator'] = df['financial_status_indicator'].str.decode("utf-8")
                        df['timestamp_inserted'] = datetime.now()

                        # df = df.drop_duplicates()
                        # df = df.sort_values(['ask_time', 'symbol'])

                        df.to_sql(name=f"00000_LVL1_Q2", con=engine, index=False, if_exists="append")
                        counter_messages_total += np_array.size
                        counter_messages_period = np_array.size

                    except Exception as e:
                        self.logger.error(e)
                        df.to_csv(os.path.join('b:\\', f"{datetime.now()}.csv".replace(":", "-")))
                        with open(os.path.join('b:\\', f"{datetime.now()}.txt".replace(":", "-")), "w") as f:
                            f.write(e)
                            f.write("\n".join(",".join(map(str, x)) for x in (np_array)))

                if time.time() - time_start > 10:
                    time_now = time.time()
                    total_time = time_now - time_start
                    messages_per_second = round(counter_messages_period / total_time, 2)
                    self.logger.info(f'')
                    self.logger.info(f'')
                    self.logger.info(f'Time Elapsed:\t{round(total_time, 2)} seconds')
                    self.logger.info(f'Messages During Period:\t{counter_messages_period}')
                    self.logger.info(f'Messages Per Second:\t{messages_per_second}')
                    self.logger.info(f'')
                    self.logger.info(f'Total Database Messages:\t{self.counter_total_database}')
                    # self.logger.info(f"Current Queue Size:\t{self.databaseQueue.qsize()}")
                    time_start = time.time()
                    self.logger.info(f'')
                    self.logger.info(f'\n\n')
                df = pd.DataFrame()

class MessageBroker(MessageHandler):
    def __init__(self, kill_switch, max_consumers=2, host=r'127.0.0.1', port="5559", check_messages=True, show_messages=True):
        self.kill_switch = kill_switch
        self.url = f'tcp://{host}:{port}'
        self.check_messages = check_messages
        self.show_messages = show_messages

        self.messageQueue = JoinableQueue()
        self.resultQueue = JoinableQueue()
        self.databaseQueue = JoinableQueue()
        self.stopEventQueue = JoinableQueue()
        self.databaseQueue = ArrayQueue(max_mbytes=300)

        self.consumers = []
        self.max_consumers = max_consumers
        self.max_tasks = 1000

    def start(self):
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()
        self.logger.info("Initializing ZMQSubscriber")
        self.logger.info(f"Starting ZMQ \t{datetime.now()}")
        self.initialize(show_messages=True, check_messages=True)
        self.monitor_zmq_pusher()

    def monitor_zmq_pusher(self):
        time.sleep(5)
        # start message consumer
        messageConsumer = MessageConsumer(self.messageQueue, self.databaseQueue, self.stopEventQueue)
        messageConsumer.start()
        self.consumers.append(messageConsumer)
        message = ""

        time.sleep(5)
        databaseConsumer = DatabaseConsumer(self.databaseQueue, self.resultQueue, self.stopEventQueue)
        databaseConsumer.start()
        self.consumers.append(databaseConsumer)

        counter_messages_period = 0
        self.counter_total_manager = 0
        time_start = time.time()

        self.ctx = zmq.Context()

        with self.ctx.socket(zmq.PULL) as self.zmq_socket:
            self.zmq_socket.setsockopt(zmq.SNDHWM, 10000)
            self.zmq_socket.setsockopt(zmq.RCVHWM, 10000)
            self.zmq_socket.connect(self.url)

            while not self.kill_switch.is_set():
                # packed = socket_zmq_pull.recv_string()
                packed = self.zmq_socket.recv()
                message = msgpack.unpackb(packed).decode().split(',')
                self.counter_total_manager += 1
                counter_messages_period += 1

                message[2] = "Q"
                self.messageQueue.put(message[2:-1])

                if time.time() - time_start > 10:
                    time_now = time.time()
                    total_time = time_now - time_start
                    messages_per_second = round(counter_messages_period / total_time, 2)
                    self.logger.info(f'')
                    self.logger.info(f"{message[0:6]}")
                    self.logger.info(f'')
                    self.logger.info(f'Time Elapsed:\t{round(total_time, 2)} seconds')
                    self.logger.info(f'Messages During Period:\t{counter_messages_period}')
                    self.logger.info(f'Messages Per Second:\t{messages_per_second}')
                    self.logger.info(f'')
                    self.logger.info(f'Total Message Broker Messages:\t{self.counter_total_manager}')
                    self.logger.info(f'')
                    self.logger.info(f"Message Broker Queue Size:\t{self.messageQueue.qsize()}")
                    self.logger.info(f'')

                    self.logger.info(f'\n\n')
                    counter_messages_period = 0
                    time.sleep(1)
                    time_start = time.time()

                self.update_counters()
                self.check_message(message)

    # def monitor_consumers(self):
    #
    #     while True:
    #         time.sleep(1)
    #         aliveConsumerCount = sum(consumer.is_alive() for consumer in self.consumers)
    #         self.logger.info(f'Total [{aliveConsumerCount} alive / {len(self.consumers)}] consumers and {self.messageQueue.qsize()} tasks are there.')
    #
    #         if (self.messageQueue.qsize() > self.max_tasks and len(self.consumers) < self.max_consumers):
    #
    #             newConsumer = MessageConsumer(self.messageQueue, self.resultQueue, self.stopEventQueue)
    #
    #             self.consumers.append(newConsumer)
    #
    #             newConsumer.start()
    #
    #             self.logger.info("Adding new consumer")
    #
    #         elif self.messageQueue.qsize() == 0:
    #             for _ in range(aliveConsumerCount):
    #                 self.messageQueue.put(None)
    #             break
    #
    #         self.messageQueue.put(None)
    #         self.logger.info("Killing a consumer.")


if __name__ == "__main__":
    results_queue = Queue()
    kill_switch = multiprocessing.Event()

    initializer(logging.DEBUG)

    workers = {}
    num_workers = 2

    manager = MessageBroker(kill_switch)

    try:
        manager.start()
    except KeyboardInterrupt:
        print('parent received ctrl-c')
        kill_switch.set()
        for consumer in manager.consumers:
            consumer.join()
