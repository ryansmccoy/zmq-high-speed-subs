import multiprocessing
import os
import sys
import time
from datetime import datetime
from queue import Empty

import msgpack
import numpy as np
import pandas as pd
import zmq
from dotenv import load_dotenv
import binascii
import threading
from threading import Thread

load_dotenv()
try:
    from arrayqueues.shared_arrays import ArrayQueue
except Exception as e:
    print(e)

    sys.exit(0)

try:
    from message_transformer import MessageValidator
except:
    from utils import MessageTransformer

from utils import setup_logging, setup_db_connection

class ZMQPuller(multiprocessing.Process):
    name = "ZMQPuller"

    def __init__(self,statusQueue, kill_switch, pull_host=r'127.0.0.1', pull_port="5559", interval_time=10):
        multiprocessing.Process.__init__(self)

        self.zmq_pull_url = f'tcp://{pull_host}:{pull_port}'
        self.statusQueue = statusQueue
        self.kill_switch = kill_switch
        self.worker_processes = 16
        self.work_size = 10000
        self.interval_time = interval_time
        self.counter_initialize()
        self.message_queue = multiprocessing.Queue()
        self._receive_thread = threading.Thread(target=self.collector_thread, name="Collector")

    def collector_thread(self):

        self.logger.debug(f"worker thread {threading.currentThread().ident} starting")

        socket = self.context.socket(zmq.REP)
        socket.connect(self.url_worker)

        while True:
            try:
                request_packet = socket.recv()
            except zmq.core.error.ZMQError as e:
                if e.errno == zmq.ETERM:
                    break
            else:
                self.logger.debug(f"thread {threading.currentThread().ident} got request packet: {request_packet}")
                self.logger.debug(f"thread {threading.currentThread().ident} sending response packet: {response_packet}")
                socket.send(response_packet)

        self.logger.debug(f"worker thread {threading.currentThread().ident} exiting")

    def run(self):

        from uuid import uuid4
        from collections import defaultdict
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()
        self.logger.info("")
        self.logger.info("")
        self.logger.info(f'\tSpawning ZMQPuller')
        self.logger.info("")
        self.logger.info("\n\n")
        self.started_workers_and_queues = defaultdict(list)
        self.finished_workers_and_queues = defaultdict(list)

        self.ctx = zmq.Context()

        with self.ctx.socket(zmq.PULL) as self.zmq_socket:
            self.zmq_socket.setsockopt(zmq.SNDHWM, 10000)
            self.zmq_socket.setsockopt(zmq.RCVHWM, 10000)
            self.zmq_socket.connect(self.zmq_pull_url)

            while not self.kill_switch.is_set():

                if len(self.started_workers_and_queues) < self.worker_processes:
                    """
                    Starts worker that connects to the Pusher
                    """
                    # create dedicated queue for worker process
                    status_queue = multiprocessing.Queue()
                    queue = multiprocessing.JoinableQueue()
                    # create Worker, add to list, and start
                    worker = Worker(queue, status_queue, self.kill_switch)
                    # recv data from socket and add to queue
                    for slot in range(self.work_size):
                        packed = self.zmq_socket.recv()

                        queue.put(packed)
                        self.counter_messages_period += 1

                    queue.put("END")

                    worker.start()

                    self.started_workers_and_queues[str(uuid4())] = worker, queue
                    self.check_status(packed)
                    time.sleep(5)

                self.logger.info("")
                self.logger.info(f"\tCurrently Running Workers:\t {len(self.started_workers_and_queues)}")
                self.logger.info("\n\n")

                if len(self.started_workers_and_queues) == self.worker_processes:
                    time.sleep(5)

                if len(self.started_workers_and_queues) > 0:
                    workers_to_delete = []
                    # check if alive and queue empty
                    for idx, (uuid, (_worker, _queue)) in enumerate(self.started_workers_and_queues.items()):
                        if not _worker.is_alive() and _queue.empty():
                            self.counter_messages_total += self.work_size
                            self.logger.info(f'Worker[{idx}]:\tis not alive')
                            self.logger.info(f'Worker[{idx}]:\tqueue is empty')
                            self.logger.info(f'Moving Worker[{idx}] to Finished Worker List')
                            self.finished_workers_and_queues[uuid] = _worker, _queue
                            self.logger.info(f'Adding Worker[{idx}] to Finished Worker List')
                            workers_to_delete.append(uuid)

                    if len(workers_to_delete) > 0:
                        for uuid in workers_to_delete:
                            self.logger.info(f'Removing Worker[{uuid}] from Running Workers List')
                            del self.started_workers_and_queues[uuid]
                            self.logger.info(f'\n\n')

                # time.sleep(1)

    def counter_initialize(self):

        self.counter_total_manager = 0
        self.current_loop_iteration = 0
        self.counter_messages_total = 0
        self.counter = 0
        self.counter_messages_period = 0
        self.counter_messages_period_sleep = 0
        self.counter_messages_current = 0
        self.counter_total_consumer = 0
        self.show_first_message = True
        self.time_total = 0
        self.time_period_start = time.time()

    def check_status(self,message):

        self.counter_total_consumer += 1
        self.counter_total_manager += 1
        # self.counter_messages_period += 1
        self.counter_messages_period_sleep += 1
        self.current_loop_iteration += 1
        self.counter += 1
        # self.counter_messages_total += 1
        #
        # if self.counter_messages_period_sleep > 3000:
        #     # pause because CPU spikes to 100 at ~5000 iterations
        #     self.logger.info(f'Taking a Pause During Iteration')
        #     time.sleep(0.5)
        #     self.counter_messages_period_sleep = 0

        if time.time() - self.time_period_start > self.interval_time:
            time_now = time.time()

            time_period = time_now - self.time_period_start

            self.time_total = self.time_total + time_period
            messages_per_second = round(self.counter_messages_period / time_period, 2)

            self.logger.info(f'')
            self.logger.info(f'Time Elapsed:\t{round(self.interval_time, 2)} seconds')
            self.logger.info(f'Messages During Period:\t{self.counter_messages_period}')
            self.logger.info(f'Messages Per Second:\t{messages_per_second}')

            self.logger.info(f'')
            self.logger.info(f'Total Time Elapsed:\t{round(self.time_total, 2)} seconds')
            self.logger.info(f'Total Messages Distributed to finished Workers:\t{self.counter_messages_total}')
            self.logger.info(f'Total Messages Per Second:\t{round((self.counter_messages_total / self.time_total), 2)}')
            self.logger.info(f'')
            # self.logger.info(f"Current Queue Size:\t{self.databaseQueue.qsize()}")
            self.logger.info(f'')
            self.logger.info(f"{message[0:45]}")
            self.logger.info(f'\n\n')

            self.counter_messages_period = 0
            self.counter_messages_period_sleep = 0

            self.time_period_start = time.time()

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

class Worker(MessageValidator, multiprocessing.Process):
    """
    Sole Purpose is to take Data From Queue and Insert into Database
    """
    name = "Worker"

    def __init__(self, queue,status_queue, kill_switch, interval_time=30):
        super().__init__()
        multiprocessing.Process.__init__(self)
        self.status_queue = status_queue
        self.interval_time = interval_time
        self.queue = queue
        self.kill_switch = kill_switch
        self.show_first_message = True

        self.counter = 0
        self.counter_messages_current = 0
        self.counter_total_database = 0
        self.counter_total_manager = 0
        self.current_loop_iteration = 0
        self.counter_messages_total = 0
        self.counter_messages_period = 0
        self.counter_messages_period_sleep = 0
        self.counter_messages_current = 0
        self.counter_total_consumer = 0
        self.counter_messages_period_sleep = 0
        self.counter_messages_total = 0

        self.time_total = 0

    def get_data_from_queue(self):

        self.current_loop_iteration = 0
        self.current_loop_pause = 0

        np_array = self._empty_update_msg

        while not self.kill_switch.is_set():
            try:
                packed = self.queue.get()

                if packed == "END":
                    break

                message = packed.decode().split(',')
                array = self.process_message(message[1:])
                np_array = np.append(np_array, array[0])
                self.check_status(message)

            except Empty as empty:
                self.logger.info(f'')
                self.logger.info(f'Current Loop Iteration:\ {self.current_loop_iteration}')
                self.logger.info(f'Current Numpy Array:\t {np_array.size}')
                self.logger.info(f'Waiting for more messages...')
                self.logger.info(f'')
                time.sleep(0.01)

            except Exception as E:
                self.logger.info(f"Empty Queue\t{E}")

        self.counter_messages_total += np_array.size
        self.counter_messages_period = np_array.size
        return np_array

    def run(self):
        self.inititialize()
        self.engine = setup_db_connection()
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()
        print("\n\n")
        self.logger.info("")
        self.logger.info(f'Spawning Worker')
        self.logger.info("")

        self.time_start_process = time.time()
        self.time_start_cycle = time.time()

        """
        Starts worker that connects to the Pusher
        """
        np_array = self.get_data_from_queue()
        self.insert_data_into_database(np_array)
        self.check_status("COMPLETED")
        return

    def check_status(self, message=None):

        self.current_loop_iteration += 1
        self.current_loop_pause += 1
        self.counter_total_database += 1
        self.counter_total_manager += 1
        self.counter_messages_period += 1
        # self.counter_messages_period_sleep += 1
        self.counter_messages_total += 1

        # if self.current_loop_pause > 5000:
        #     # pause because CPU spikes to 100 at ~5000 iterations
        #     # self.logger.info(f'Taking a Pause During Iteration')
        #     time.sleep(0.001)
        #     self.current_loop_pause = 0

        # if self.counter_messages_period_sleep > 3000:
        #     # pause because CPU spikes to 100 at ~5000 iterations
        #     self.logger.info(f'Taking a Pause During Iteration')
        #     time.sleep(0.5)
        #     self.counter_messages_period_sleep = 0

        if time.time() - self.time_start_cycle > self.interval_time or message == "COMPLETED":
            time_now = time.time()
            time_cycle = time_now - self.time_start_cycle
            self.time_total = self.time_total + time_cycle
            messages_per_second = round(self.counter_messages_period / time_cycle, 2)

            # self.logger.info(f'')
            print("\n\n")
            # self.logger.info(f'')
            # self.logger.info(f'Reached the End of the Queue')
            # self.logger.info(f'Current Loop Iteration:\t {self.current_loop_iteration}')
            # self.logger.info(f'Current Numpy Array:\t {np_array.size}')
            # self.logger.info(f'Starting Database Insert')
            self.logger.info(f'')
            # self.logger.info(f'Time Elapsed:\t{round(self.interval_time, 2)} seconds')
            # self.logger.info(f'Messages During Period:\t{self.counter_messages_period}')
            # self.logger.info(f'Messages Per Second:\t{messages_per_second}')

            # self.logger.info(f'')
            self.logger.info(f'Worker Messages Time Elapsed:\t{round(self.time_total, 2)} seconds')
            self.logger.info(f'Worker Messages:\t{self.counter_total_manager}')
            self.logger.info(f'Worker Messages Per Second:\t{round((self.counter_total_manager / self.time_total), 2)}')
            self.logger.info(f'')
            # self.logger.info(f"Current Queue Size:\t{self.databaseQueue.qsize()}")
            self.logger.info(f"{message}")
            self.time_start_cycle = time.time()
            self.logger.info(f'\n\n')
            self.counter_messages_period = 0
            self.time_count = 0
            # time.sleep(5)
            self.current_loop_pause = 0
            self.counter_messages_period_sleep = 0

    def insert_data_into_database(self, np_array):
        if np_array.size > 1:
            try:
                self.logger.info(f'Starting to Insert into Database')
                df = pd.DataFrame(np_array[1:], columns=self.update_fields_list)
                df.columns = self.header
                df['symbol'] = df['symbol'].str.decode("utf-8")
                df['available_regions'] = df['available_regions'].str.decode("utf-8")
                df['message_contents'] = df['message_contents'].str.decode("utf-8")
                df['financial_status_indicator'] = df['financial_status_indicator'].str.decode("utf-8")
                df['timestamp_inserted'] = datetime.now()
                df = df.drop_duplicates()
                df = df.sort_values(['ask_time', 'symbol'])
                # for symbol, df_g in df.groupby(['symbol']):
                #     df_g.to_sql(name=f"{symbol}_LVL1_Q_V2", con=engine, index=False, if_exists="append")
                df.to_sql(name=f"{datetime.now().strftime('%Y%m%d')}_LVL1_TEST", con=self.engine, index=False, if_exists="append")
                self.logger.info(f'Completed to Insert into Database')
            except Exception as e:
                self.logger.error(e)
                try:
                    df.to_csv(os.path.join('d:\\', f"{datetime.now()}.csv".replace(":", "-")))
                except:
                    with open(os.path.join('d:\\', f"{datetime.now()}.csv".replace(":", "-")), "w") as f:
                        f.write("\n".join(",".join(map(str, x)) for x in (np_array)))

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name
