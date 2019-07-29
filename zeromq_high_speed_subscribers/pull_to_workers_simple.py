"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import os
import time
from datetime import datetime
import logging
import multiprocessing
from multiprocessing.queues import Empty
from multiprocessing import Process, Queue, current_process
import msgpack

import zmq

def worker(results_queue, stop_event, host="127.0.0.1", port="5559", insert_limit = 15000):
    """
    Starts worker that connects to the Pusher
    """
    logger = multiprocessing.get_logger()

    pid = os.getpid()
    pname = current_process().name

    ctx = zmq.Context()

    show_first_message = True
    counter_messages_current = 0
    counter_messages_total = 0
    time_start = 0

    logger.info(f'{pname}-pid:{pid}]\tSpawning Worker')

    with ctx.socket(zmq.PULL) as zmq_socket:
        zmq_socket.setsockopt(zmq.SNDHWM, 10000)
        zmq_socket.setsockopt(zmq.RCVHWM, 10000)
        zmq_socket.connect(f'tcp://{host}:{port}')
        while not stop_event.is_set():

            # packed = socket_zmq_pull.recv_string()
            packed = zmq_socket.recv()
            message = msgpack.unpackb(packed)

            ################################################
            # Everything below is for benchmarking purposes
            counter_messages_current += 1

            if show_first_message:
                time_start = time.time()
                start_time = datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")
                logger.info(f'Starting \t{start_time}')
                show_first_message = False

            if counter_messages_current >= 10000:
                counter_messages_total = counter_messages_total + counter_messages_current
                counter_messages_current = 0
                logger.info(f"{counter_messages_total}->{message[0:50]}")

            if message[0:4] == "stop":
                last_message = f"Received the message: {message}"
                logger.info(f'{last_message} {datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")}')
                stop_event.set()
                break

    time_end = time.time()
    total_time = time_end - time_start

    results_queue.put(f"{pname}-pid:{pid}\tTotal Messages Current:\t\t{counter_messages_current}")
    results_queue.put(f"{pname}-pid:{pid}]\tTotal Time:\t\t{total_time}")
    results_queue.put(f"{pname}-pid:{pid}]\tTotal Msg Per Sec:\t\t{insert_limit / total_time}")
    results_queue.put(f"{pname}-pid:{pid}]\tTotal Messages:\t\t{counter_messages_total}")
    return

if __name__ == "__main__":

    results_queue = Queue()
    stop_event = multiprocessing.Event()

    multiprocessing.log_to_stderr(logging.DEBUG)

    workers = {}
    num_workers = 2

    for procnum in range(num_workers):
        workers[procnum] = Process(name=f"Puller({procnum + 1})",target=worker, args=(results_queue,stop_event))
        workers[procnum].start()

    try:
        for id, worker in workers.items():
            worker.join()
    except KeyboardInterrupt:
        print('parent received ctrl-c')
        for id, worker in workers.items():
            worker.terminate()
            worker.join()

    results = []
    while True:
        try:
            results.append(results_queue.get(timeout=1))
        except Empty:
            break  # empty queue, we're done!

    for result in results:
        print(result)
