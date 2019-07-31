"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import logging.config
import multiprocessing

from _11_subscriber import ZMQSubscriberPipe
from _12_pusher import ZMQPusherPipe

if __name__ == "__main__":

    multiprocessing.log_to_stderr(logging.DEBUG)

    (pipe_pusher, pipe_subscriber) = multiprocessing.Pipe(duplex=False)

    stop_event = multiprocessing.Event()

    process_subscriber = ZMQSubscriberPipe(pipe_subscriber, stop_event)
    process_pusher = ZMQPusherPipe(pipe_pusher, stop_event)

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
