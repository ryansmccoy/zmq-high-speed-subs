
import datetime
import zmq
import time

from feed_generator import FakeFeed
from datetime import datetime

def server_pub(port="5558"):

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://*:{port}")
    socket.setsockopt(zmq.SNDHWM, 100000)
    socket.setsockopt(zmq.RCVHWM, 100000)

    fakefeed = FakeFeed(sleep_time=0)

    counter = 0
    counter_print = 0

    start_time = datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")
    print(f'\nStarting \t{start_time}')

    while True:
        # Wait for next request from client
        time_start = time.time()
        for item in fakefeed:
            # print(item[0:100])

            timestamp = datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")
            message = "run," + timestamp + item
            socket.send_string("run," + timestamp + item)

            counter_print += 1
            counter  += 1

            if counter_print >= 5000:
                print(message[0:100])
                counter_print = 0

            if (time.time() - time_start) > 15:
                time_end = time.time()
                total_time = time_end - time_start
                print(f'\n\tTime Elapsed:\t{round(total_time,2)} seconds\n\tTotal Messages:\t{counter}\n \tMessages Per Second:\t{round(counter/total_time,3)}\n')

                for x in range(4):

                    shutdown = "stop," + timestamp
                    socket.send_string(shutdown)

                print("Shutting Down...")
                end_time = datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")
                print(f'\nEnd \t{end_time}')

                time.sleep(30)

                return total_time


if __name__ == "__main__":

    total_time = server_pub(port="5558")
    print(total_time)

