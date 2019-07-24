
import pandas as pd

import zmq
from itertools import cycle
import time

class FakeFeed:
    def __init__(self, filepath=f'black_box_pattern/test_data.csv'):
        self.filepath = filepath

    def __iter__(self):
        df = pd.read_csv(self.filepath)
        df.reset_index(drop=True, inplace=True)
        counter = 0
        for idx, num in cycle(df.iterrows()):
            counter += 1
            msg = f"{counter}," + ",".join(list(num.astype(str).to_dict().values()))
            yield msg
            time.sleep(1)


def server_pub(port="5558"):

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://*:{port}")
    socket.setsockopt(zmq.SNDHWM, 100000)

    fakefeed = FakeFeed()

    while True:
        # Wait for next request from client
        time_start = time.time()
        for item in fakefeed:
            print(item[0:100])
            socket.send_string(item)
            if (time.time() - time_start) > 60:
                time_end = time.time()
                total_time = time_end - time_start
                print(f'\nTime Elapsed:\t{round(total_time,2)} seconds \tMessages Per Second:\t{counter/total_time}\n')
                time_start = time.time()

if __name__ == "__main__":

    server_pub(port="5558")

