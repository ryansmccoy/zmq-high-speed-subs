"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""

import datetime
import multiprocessing
import sys
import time

import zmq

class ZMQSubscriber(multiprocessing.Process):
    def __init__(self, queue, port="5558"):
        self.queue = queue
        self.port = port
        multiprocessing.Process.__init__(self)

    def run(self):
        print(f"\nStarting SUBSCRIBE\t{datetime.datetime.now()}")

        self.ctx = zmq.Context()
        self.sock = self.ctx.socket(zmq.SUB)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, "")
        self.sock.connect(f'tcp://127.0.0.1:{self.port}')

        self.counter = 0
        self.counter_print = 0

        while True:
            message = self.sock.recv_string()
            self.queue.put(message)
            if message[0:4] == "stop":
                sys.exit(1)

class ZMQPusher(multiprocessing.Process):
    def __init__(self, queue, port="5559"):
        self.queue = queue
        self.port = port
        multiprocessing.Process.__init__(self)

    def run(self):
        self.start_time = datetime.datetime.now()
        time_start = time.time()

        print(f"\nStarting Pusher\t{self.start_time}")

        self.ctx = zmq.Context()
        self.push_socket = self.ctx.socket(zmq.PUSH)
        self.push_socket.bind(f'tcp://*:{self.port}')

        self.counter = 0
        self.counter_print = 0

        while True:
            message = self.queue.get()

            self.push_socket.send_string(message)

            self.counter += 1
            self.counter_print += 1

            # print stats every 5000 messages
            if self.counter_print >= 5000:
                print(f'\n[INFO/ZMQPusher] ->'
                      f'\t {datetime.datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")}')

                print(f"\n[INFO/ZMQPusher] ->"
                      f"\t {message[0:100]}")

                print(f"The size of queue is {self.queue.qsize()}")
                self.counter_print = 0

            # shutdown if receive stop message
            if message[0:4] == "stop":
                self.end_time = datetime.datetime.now()
                # the producer emits None to indicate that it is done
                [self.push_socket.send_string(message) for message in ["stop"] * 5]
                print(f'\nShutdown Received\t{self.end_time}')
                print(f'\nStart Time:\t{self.start_time}')
                time_end = time.time()
                total_time = time_end - time_start
                print(f'\n\tTime Elapsed:\t{round(total_time, 2)} seconds'
                      f'\n\tTotal Messages:\t{self.counter}\n'
                      f'\tMessages Per Second:\t{round(self.counter / total_time, 3)}\n')

                self.push_socket.close()
                self.ctx.term()
                sys.exit(1)


if __name__ == "__main__":

    queue = multiprocessing.Queue()

    process_subscriber = ZMQSubscriber(queue)
    process_pusher = ZMQPusher(queue)

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
