import zmq
import os
from multiprocessing import Process, current_process

def worker():
    pname = current_process().name

    pid = os.getpid()

    context = zmq.Context()
    work_receiver = context.socket(zmq.PULL)
    work_receiver.connect("tcp://127.0.0.1:5559")

    while True:
        message = work_receiver.recv_string()
        print(f"[INFO/{pname}-pid:{pid}] ->\t {message[0:100]}")

if __name__ == "__main__":

    num_workers = 4

    workers = {}

    for x in range(num_workers):
        workers[x] = Process(name=f"Process {x + 1}",target=worker, args=())
        workers[x].start()


