from datetime import datetime
import zmq
import os
from multiprocessing import current_process
from multiprocessing import Process, Queue
import time

inputqueue = Queue()
job_queue = Queue()
result_queue = Queue()


def worker():
    """
    Starts worker that connects to the Pusher
    :return:
    """
    pid = os.getpid()

    counter = 0
    counter_print = 0

    pname = current_process().name
    print(f"\n\t[INFO/{pname}-pid:{pid}]\tWaiting for First Message")

    context = zmq.Context()
    work_receiver = context.socket(zmq.PULL)
    work_receiver.connect("tcp://127.0.0.1:5559")
    first_message = True
    time_start = time.time()

    while True:

        message = work_receiver.recv_string()

        counter_print += 1
        counter += 1

        if first_message:
            time_start = time.time()
            start_time =datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")
            print(f'\nStarting \t{start_time}')
            first_message = False

        if counter_print >= 5000:
            print(f"\n\n[INFO/{pname}-pid:{pid}] ->\t {message[0:100]}")
            counter_print = 0

        if message[0:4] == "stop":
            last_message = f"\nReceived the message: {message}"
            print(last_message + f'\n\n{datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")}')
            break

    time_end = time.time()
    total_time = time_end - time_start
    print(f'\n\nTime Elapsed:\t{round(total_time, 2)} seconds \tMessages Per Second:\t{counter / total_time}\n')

if __name__ == "__main__":

    num_workers = 4

    workers = {}
    count = 100000

    for x in range(num_workers):
        workers[x] = Process(name=f"Process {x + 1}",target=worker, args=())
        workers[x].start()

    try:
        for id, worker in workers.items():
            worker.join()
    except KeyboardInterrupt:
        print('parent received ctrl-c')
        for id, worker in workers.items():
            worker.terminate()
            worker.join()
