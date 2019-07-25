"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""

import zmq
import datetime
import multiprocessing
import sys
import time

class ZMQSubscriber(multiprocessing.Process):
    def __init__(self, pipe_conn, port="5558"):
        self.pipe_conn = pipe_conn
        self.port = port
        multiprocessing.Process.__init__(self)

    def run(self):
        print(f"\nStarting SUBSCRIBE\t{datetime.datetime.now()}")
        self.url = f'tcp://127.0.0.1:{self.port}'
        self.ctx = zmq.Context()
        self.sock = self.ctx.socket(zmq.SUB)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, "")
        self.sock.connect(self.url)

        self.counter = 0
        self.counter_print = 0

        while True:
            message = self.sock.recv_string()
            self.pipe_conn.send(message)
            if message[0:4] == "stop":
                sys.exit(1)

class ZMQPusher(multiprocessing.Process):
    def __init__(self, pipe_conn, port="5559"):
        self.pipe_conn = pipe_conn
        self.port = port
        self.start_time = datetime.datetime.now()
        multiprocessing.Process.__init__(self)

    def run(self):
        print(f"\nStarting Pusher\t{self.start_time}")

        self.url = f'tcp://*:{self.port}'
        time_start = time.time()

        self.ctx = zmq.Context()
        self.push_socket = self.ctx.socket(zmq.PUSH)
        self.push_socket.bind(self.url)

        self.counter = 0
        self.counter_print = 0

        while True:
            message = self.pipe_conn.recv()
            self.push_socket.send_string(message)
            self.counter += 1
            self.counter_print += 1

            # print stats every 5000 messages
            if self.counter_print >= 5000:
                print(f'\n[INFO/01_PUSH] ->'
                      f'\t {datetime.datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")}')

                print(f"\n[INFO/01_PUSH] ->"
                      f"\t {message[0:100]}")

                # print(f"[INFO/01_PUSH] Queue Size:\t{.qsize()}")
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

def foo(w):
    for i in range(10):
        w.send((i, current_process().name))
    w.close()

if __name__ == "__main__":
    from multiprocessing import Process, Pipe, current_process
    #
    # from multiprocessing.connection import wait
    # pushers = []
    #
    # for i in range(4):
    #     # r, w = Pipe(duplex=False)
    #     push, sub = Pipe(duplex=False)
    #
    #     pushers.append(push)
    #     p = Process(target=foo, args=(sub,))
    #     p.start()
    #     # We close the writable end of the pipe now to be sure that
    #     # p is the only process which owns a handle for it.  This
    #     # ensures that when p closes its handle for the writable end,
    #     # wait() will promptly report the readable end as being ready.
    #     sub.close()
    #
    # while pushers:
    #     for push in wait(pushers):
    #         try:
    #             msg = push.recv()
    #         except EOFError:
    #             pushers.remove(push)
    #         else:
    #             print(msg)

    (push, sub) = Pipe(duplex=False)

    process_subscriber = ZMQSubscriber(sub)
    process_pusher = ZMQPusher(push)

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
