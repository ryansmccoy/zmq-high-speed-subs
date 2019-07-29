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

from utils import setup_logging, setup_db_connection


class MessageConsumer(multiprocessing.Process):
    name = "MessageConsumer"

    def __init__(self, databaseQueue, statusQueue, kill_switch, host=r'127.0.0.1', port="5559", interval_time=10):
        multiprocessing.Process.__init__(self)

        self.zmq_url = f'tcp://{host}:{port}'
        self.interval_time = interval_time

        self.databaseQueue = databaseQueue
        self.statusQueue = statusQueue
        self.kill_switch = kill_switch

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

        counter_messages_period = 0
        self.counter_total_manager = 0
        time_start = time.time()

        time_start = time.time()
        counter_messages_period = 0
        counter_total = 0
        time_total = 0


        self.ctx = zmq.Context()

        with self.ctx.socket(zmq.PULL) as self.zmq_socket:
            self.zmq_socket.setsockopt(zmq.SNDHWM, 10000)
            self.zmq_socket.setsockopt(zmq.RCVHWM, 10000)
            self.zmq_socket.connect(self.zmq_url)

            while not self.kill_switch.is_set():

                # packed = socket_zmq_pull.recv_string()
                packed = self.zmq_socket.recv()
                message = msgpack.unpackb(packed).decode().split(',')

                self.counter_total_manager += 1
                counter_messages_period += 1

                message[2] = "Q"
                # self.messageQueue.put(message[2:-1])

                # message = self.messageQueue.get()
                self.counter_total_consumer += 1

                """
                Starts worker that connects to the Pusher
                """

                current_loop_iteration += 1

                if message:
                    np_array = process.process_message(message[2:-1])
                else:
                    np_array = None

                if np_array:
                    self.databaseQueue.put(np_array)

                counter += 1
                counter_messages_total += 1

                if time.time() - time_start > self.interval_time:
                    time_now = time.time()
                    time_period = time_now - time_start
                    time_total = time_total + time_period
                    messages_per_second = round(counter_messages_period / time_period, 2)

                    self.logger.info(f'')
                    self.logger.info(f'')
                    self.logger.info(f'Time Elapsed:\t{round(self.interval_time, 2)} seconds')
                    self.logger.info(f'Messages During Period:\t{counter_messages_period}')
                    self.logger.info(f'Messages Per Second:\t{messages_per_second}')

                    self.logger.info(f'')
                    self.logger.info(f'Total Time Elapsed:\t{round(time_total, 2)} seconds')
                    self.logger.info(f'Total Messages:\t{self.counter_total_manager}')
                    self.logger.info(f'Total Messages Per Second:\t{round((self.counter_total_manager / time_total), 2)}')
                    self.logger.info(f'')
                    # self.logger.info(f"Current Queue Size:\t{self.databaseQueue.qsize()}")
                    self.logger.info(f'')
                    self.logger.info(f"{message[0:45]}")

                    time_start = time.time()
                    self.logger.info(f'')
                    self.logger.info(f'\n\n')
                    counter_messages_period = 0
                    time.sleep(1)


    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


class DatabaseConsumer(multiprocessing.Process):
    """
    Sole Purpose is to take Data From Queue and Insert into Database
    """
    name = "DatabaseConsumer"

    def __init__(self, databaseQueue, statusQueue, kill_switch, interval_time=10):
        multiprocessing.Process.__init__(self)
        self.interval_time = interval_time
        self.databaseQueue = databaseQueue
        self.resultQueue = statusQueue
        self.kill_switch = kill_switch

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
        time_total = 0
        self.logger.info("")
        self.logger.info("")
        self.logger.info(f'Spawning DatabaseConsumer')
        self.logger.info("")
        self.logger.info("")

        counter_messages_period = 0

        while not self.kill_switch.is_set():

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

            if time.time() - time_start > self.interval_time:
                time_now = time.time()
                time_period = time_now - time_start
                time_total = time_total + time_period
                messages_per_second = round(counter_messages_period / time_period, 2)

                self.logger.info(f'')
                self.logger.info(f'')
                self.logger.info(f'Time Elapsed:\t{round(self.interval_time, 2)} seconds')
                self.logger.info(f'Messages During Period:\t{counter_messages_period}')
                self.logger.info(f'Messages Per Second:\t{messages_per_second}')

                self.logger.info(f'')
                self.logger.info(f'Total Time Elapsed:\t{round(time_total, 2)} seconds')
                self.logger.info(f'Total Messages:\t{self.counter_total_database}')
                self.logger.info(f'Total Messages Per Second:\t{round((self.counter_total_database / time_total), 2)}')
                self.logger.info(f'')
                # self.logger.info(f"Current Queue Size:\t{self.databaseQueue.qsize()}")
                self.logger.info(f'')
                # self.logger.info(f"{message[0:45]}")

                time_start = time.time()
                self.logger.info(f'')
                self.logger.info(f'\n\n')
                counter_messages_period = 0

            df = pd.DataFrame()

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name
