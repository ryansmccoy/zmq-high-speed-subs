"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import multiprocessing
import time
from queue import Queue

import zmq
from dotenv import load_dotenv
import threading
from uuid import uuid4
from collections import defaultdict

from _14_worker import Worker

load_dotenv()

try:
    from message_transformer import MessageValidator
except:
    from utils import MessageTransformer

from utils import setup_logging

def _zmq_puller(message_queue, zmq_pull_url):

    ctx = zmq.Context()

    with ctx.socket(zmq.PULL) as zmq_socket:
        zmq_socket.setsockopt(zmq.SNDHWM, 10000)
        zmq_socket.setsockopt(zmq.RCVHWM, 10000)
        zmq_socket.connect(zmq_pull_url)

        while True:
            try:
                packed = zmq_socket.recv()
                message_queue.put(packed)
            except zmq.core.error.ZMQError as e:
                if e.errno == zmq.ETERM:
                    break

class ZMQPuller(multiprocessing.Process):
    name = "ZMQPuller"

    def __init__(self,kill_switch, pull_host=r'127.0.0.1', pull_port="5559", interval_time=10):
        multiprocessing.Process.__init__(self)
        self.zmq_pull_url = f'tcp://{pull_host}:{pull_port}'
        self.kill_switch = kill_switch
        self.worker_processes = 12
        self.work_size = 2500
        self.message_queue_size = 2500
        self.show_first_message = True
        self.interval_time = interval_time
        self.counter_initialize()

    def run(self):
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()

        self.message_queue = Queue()

        self._receive_thread = threading.Thread(target=_zmq_puller,args=(self.message_queue, self.zmq_pull_url,), name="ZMQPullerThread")
        self._receive_thread.start()

        self.logger.info("")
        self.logger.info("")
        self.logger.info(f'\tSpawning ZMQPuller')
        self.logger.info("")
        self.logger.info("\n\n")
        self.started_workers_and_queues = defaultdict(list)
        self.finished_workers_and_queues = defaultdict(list)
        self.counter_initialize()

        while not self.kill_switch.is_set():
            self.check_for_completed_workers()
            self.check_status()

            # if queue greater x AND worker processes currently launched less than threshhold
            if self.message_queue.qsize() > self.message_queue_size \
                and len(self.started_workers_and_queues) <= self.worker_processes:
                self.launch_worker()
            else:
                time.sleep(0.1)

    def launch_worker(self):
        """
        Starts worker that connects to the Pusher
        """
        packed = ""
        # create dedicated queue for worker process
        status_queue = multiprocessing.Queue()
        queue = multiprocessing.JoinableQueue()
        # create Worker, add to list, and start
        worker = Worker(queue, status_queue, self.kill_switch)
        # recv data from socket and add to queue
        for slot in range(self.work_size):
            packed = self.message_queue.get()
            queue.put(packed)
            self.counter_messages_period += 1

        worker.start()
        queue.put("END")

        self.started_workers_and_queues[str(uuid4())] = worker, queue

    def check_for_completed_workers(self):

        if len(self.started_workers_and_queues) > 0:
            workers_to_delete = []
            # check if alive and queue empty
            for idx, (uuid, (_worker, _queue)) in enumerate(self.started_workers_and_queues.items()):
                if not _worker.is_alive() and _queue.empty():
                    self.counter_messages_total += self.work_size
                    self.logger.debug(f'Worker[{idx}]:\tis not alive')
                    self.logger.debug(f'Worker[{idx}]:\tqueue is empty')
                    self.logger.debug(f'Moving Worker[{idx}] to Finished Worker List')
                    self.finished_workers_and_queues[uuid] = _worker, _queue
                    self.logger.debug(f'Adding Worker[{idx}] to Finished Worker List')
                    workers_to_delete.append(uuid)

            if len(workers_to_delete) > 0:
                for uuid in workers_to_delete:
                    self.logger.debug(f'Removing Worker[{uuid}] from Running Workers List')
                    del self.started_workers_and_queues[uuid]
                    self.logger.debug(f'\n\n')

    def counter_initialize(self):

        self.counter = 0
        self.counter_total_manager = 0
        self.counter_total_consumer = 0

        self.current_loop_iteration = 0

        self.counter_messages_total = 0
        self.counter_messages_period = 0
        self.counter_messages_period_sleep = 0
        self.counter_messages_current = 0

        self.time_total = 0
        self.time_period_start = time.time()

    def check_status(self):

        self.counter_total_consumer += 1
        self.counter_total_manager += 1
        # self.counter_messages_period += 1
        self.counter_messages_period_sleep += 1
        self.current_loop_iteration += 1
        self.counter += 1

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
            self.logger.info(f'')
            self.logger.info(f"\tTotal Messages in _Queue:\t {self.message_queue.qsize()}")
            self.logger.info(f"\tCurrently Running Workers:\t {len(self.started_workers_and_queues)}")
            self.logger.info("\n\n")

            self.counter_messages_period = 0
            self.counter_messages_period_sleep = 0

            self.time_period_start = time.time()

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

