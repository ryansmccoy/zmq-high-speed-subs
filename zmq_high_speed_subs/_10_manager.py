"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""
import logging
import multiprocessing
import time
from datetime import datetime
from multiprocessing.managers import SyncManager

from _11_subscriber import ZMQSubscriberQueue
from _12_pusher import ZMQPusherQueue
from _13_puller import ZMQPuller

from utils import setup_logging, initializer


class ServiceManager(SyncManager):
    """
    Manages The Pipeline
    Publisher -> Subscriber -> Pusher -> Puller -> Workers
    """

    def __init__(self, subscriber=None, pusher=None, puller=None, kill_switch=None, port=50000):
        super().__init__(address=('127.0.0.1', port), authkey=b'gotem')
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()

        self.logger.info("Initializing Services")
        self.logger.info(f"Starting \t{datetime.now()}")

        self.kill_switch = kill_switch

        self.subscriber = subscriber

        if self.subscriber:
            self.subscriber.start()

        self.pusher = pusher

        if pusher:
            self.pusher.start()

        self.puller = puller

        if puller:
            self.puller.start()


if __name__ == "__main__":
    kill_switch = multiprocessing.Event()

    initializer(logging.INFO)

    queue_sub_to_push = multiprocessing.Queue()

    pusher = ZMQPusherQueue(queue_sub_to_push, kill_switch, host='127.0.0.1', port="5559")
    subscriber = ZMQSubscriberQueue(queue_sub_to_push, kill_switch, host='127.0.0.1', port="5558")
    puller = ZMQPuller(kill_switch, pull_host='127.0.0.1', pull_port="5559")

    service_manager = ServiceManager(subscriber, pusher, puller, kill_switch)

    try:
        s = service_manager.get_server()
        s.serve_forever()
    except KeyboardInterrupt:
        print('parent received ctrl-c')

        kill_switch.set()
        subscriber.join()
        pusher.join()
        puller.join()
        time.sleep(3)

    finally:
        subscriber.terminate()
        pusher.terminate()
        puller.terminate()
