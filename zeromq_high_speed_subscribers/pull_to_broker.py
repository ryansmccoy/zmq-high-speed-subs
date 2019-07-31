"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import sys
import logging
import multiprocessing
from multiprocessing import Queue

from dotenv import load_dotenv

from _10_manager import ServiceManager

load_dotenv()
try:
    from arrayqueues.shared_arrays import ArrayQueue
except Exception as e:
    print(e)
    sys.exit(0)

try:
    from message_transformer import MessageValidator
except:
    from utils import MessageTransformer

from utils import initializer

# set the date and time format
date_format = "%m-%d-%Y %H:%M:%S"

if __name__ == "__main__":
    results_queue = Queue()
    kill_switch = multiprocessing.Event()

    initializer(logging.DEBUG)

    workers = {}
    num_workers = 2

    manager = ServiceManager(kill_switch)

    try:
        manager.start()
    except KeyboardInterrupt:
        print('parent received ctrl-c')
        kill_switch.set()
        for consumer in manager.consumers:
            consumer.join()
