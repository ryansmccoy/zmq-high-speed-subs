
import pandas as pd

import zmq
from itertools import cycle
import time

from feed_generator import FakeFeed

def server_pub(port="5558"):

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://*:{port}")
    socket.setsockopt(zmq.SNDHWM, 100000)

    fakefeed = FakeFeed()

    counter = 1

    while True:
        # Wait for next request from client
        time_start = time.time()
        for item in fakefeed:
            print(item[0:100])
            socket.send_string(item)
            counter += 1
            if (time.time() - time_start) > 60:
                time_end = time.time()
                total_time = time_end - time_start
                print(f'\nTime Elapsed:\t{round(total_time,2)} seconds \tMessages Per Second:\t{counter/total_time}\n')
                time_start = time.time()
                counter = 1


if __name__ == "__main__":

    server_pub(port="5558")

