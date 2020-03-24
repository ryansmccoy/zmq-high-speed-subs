"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import logging
import multiprocessing

from _11_subscriber import ZMQSubscriberQueue
from _12_pusher import ZMQPusherQueue
from utils import initializer

if __name__ == "__main__":

    # multiprocessing.log_to_stderr(logging.DEBUG)
    initializer(logging.DEBUG)

    queue = multiprocessing.Queue()
    kill_switch = multiprocessing.Event()

    # subscriber = ZMQSubscriberQueue(queue, kill_switch, show_messages=False)
    # subscriber.start()

    pusher = ZMQPusherQueue(queue, kill_switch, show_messages=False)
    pusher.start()

    try:
        import time
        time.sleep(1999)
        pusher.join()
        subscriber.join()

    except KeyboardInterrupt:
        print('parent received ctrl-c')
        subscriber.terminate()
        pusher.terminate()
        subscriber.join()
        pusher.join()
