from datetime import datetime
import multiprocessing
import time
from multiprocessing import JoinableQueue, Queue
from multiprocessing import Queue
import msgpack
import zmq
from arrayqueues.shared_arrays import ArrayQueue
import logging

from _11_subscriber import ZMQSubscriberQueue
from _12_pusher import ZMQPusherQueue
from _13_consumer import MessageConsumer, DatabaseConsumer

from utils import MessageHandler, setup_logging, initializer

from multiprocessing.managers import SyncManager
from collections import defaultdict
from multiprocessing.managers import BaseManager

class BrokerExit(Exception):
    pass

class ServiceBroker(BaseManager):
    """ Manages Queue -> Consumer -> DatabaseConsumer """
    def __init__(self, subscriber, pusher, kill_switch, check_messages=False, show_messages=False):
        self._consumers = (MessageConsumer, DatabaseConsumer,)
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()
        self.logger.info("Initializing Services")
        self.logger.info(f"Starting \t{datetime.now()}")
        self.kill_switch = kill_switch

        self.subscriber = subscriber
        self.pusher = pusher

        # self.service_url = f'tcp://{host}:{port}'

        self.check_messages = check_messages
        self.show_messages = show_messages

        self.max_consumers = 2
        self.max_tasks = 1000
        super().__init__(address=('', 50000), authkey=b'abc')


    def launch_consumers(self):
        import uuid
        uuid = uuid.uuid4()
        time.sleep(3)
        try:
            if not getattr(self, "consumers"):
                self.consumers = defaultdict(list)
        except:
            self.consumers = defaultdict(list)

        # start message consumer
        statusQueue = JoinableQueue()
        databaseQueue = ArrayQueue(max_mbytes=300)

        for Consumer in self._consumers:
            self.logger.info(f"Initializing {Consumer}")
            consumer = Consumer(databaseQueue, statusQueue, self.kill_switch)
            consumer.start()
            self.consumers[uuid].append(consumer)
            time.sleep(5)

    def monitor_consumers(self):

        while True:
            time.sleep(1)
            aliveConsumerCount = sum(consumer.is_alive() for consumer in self.consumers)

            self.logger.info(f'Total [{aliveConsumerCount} alive / {len(self.consumers)}] consumers and {self.messageQueue.qsize()} tasks are there.')

            if (self.messageQueue.qsize() > self.max_tasks and len(self.consumers) < self.max_consumers):

                newConsumer = MessageConsumer(self.messageQueue, self.resultQueue, self.stopEventQueue)

                self.consumers.append(newConsumer)

                newConsumer.start()

                self.logger.info("Adding new consumer")

            elif self.messageQueue.qsize() == 0:
                for _ in range(aliveConsumerCount):
                    self.messageQueue.put(None)
                break

            self.messageQueue.put(None)
            self.logger.info("Killing a consumer.")

if __name__ == "__main__":
    kill_switch = multiprocessing.Event()

    # multiprocessing.log_to_stderr(logging.DEBUG)
    initializer(logging.DEBUG)

    queue_sub_to_push = multiprocessing.Queue()

    pusher = ZMQPusherQueue(queue_sub_to_push, kill_switch,host=r'127.0.0.1', port="5559", show_messages=False)
    pusher.start()

    subscriber = ZMQSubscriberQueue(queue_sub_to_push, kill_switch, host=r'127.0.0.1', port="5558", show_messages=False)
    subscriber.start()

    service_manager = ServiceBroker(subscriber, pusher, kill_switch)

    try:
        service_manager.launch_consumers()
        s = service_manager.get_server()
        s.serve_forever()
    except KeyboardInterrupt:
        print('parent received ctrl-c')
        subscriber.terminate()
        pusher.terminate()
        subscriber.join()
        pusher.join()
