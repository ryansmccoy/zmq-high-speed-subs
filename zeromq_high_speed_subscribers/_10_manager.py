from datetime import datetime
import multiprocessing
import time
from multiprocessing import JoinableQueue, Queue, Lock
from arrayqueues.shared_arrays import ArrayQueue
import logging

from _11_subscriber import ZMQSubscriberQueue
from _12_pusher import ZMQPusherQueue
from _13_consumer import ZMQPuller, Worker

from utils import setup_logging, initializer

from multiprocessing.managers import BaseManager, SyncManager

class BrokerExit(Exception):
    pass

class ServiceBroker(SyncManager):
    """ Manages Queue -> Consumer -> Worker """
    def __init__(self, subscriber, pusher, kill_switch):
        super().__init__(address=('127.0.0.1', 50000), authkey=b'gotem')
        self.logger = multiprocessing.get_logger()
        self.logger.handlers[0] = setup_logging()
        self.logger.info("Initializing Services")
        self.logger.info(f"Starting \t{datetime.now()}")

        self.lock = Lock()
        self.kill_switch = kill_switch

        self.subscriber = subscriber
        self.subscriber.start()
        self.pusher = pusher
        self.pusher.start()

        # self.service_url = f'tcp://{host}:{port}'
        self.puller = []
        self.databaseConsumers = []
        # start message consumer
        # self.databaseQueue = ArrayQueue(max_mbytes=1000)

        self.statusQueue = JoinableQueue()

    def launch_puller(self):
        self.logger.info(f"Initializing Message Consumer")
        status_queue = multiprocessing.Queue()
        puller = ZMQPuller(status_queue, kill_switch, pull_host=r'127.0.0.1', pull_port="5559")
        puller.start()
        self.puller.append(puller)

    def monitor_consumers(self):

        while True:
            aliveConsumerCount = 0

            time.sleep(5)

            # for uid, workers in self.consumers.items():
            #     print(workers)
            #     for worker in workers:
            #         print(worker)
            #         if worker.is_alive():
            #             aliveConsumerCount += 1
            #         else:
            #             print("Not Alive")

            # aliveConsumerCount = sum(consumer.is_alive() for consumer in self.consumers)
            #
            # self.logger.info(f'Total [{aliveConsumerCount} alive / {len(self.consumers)}] consumers and {self.messageQueue.qsize()} tasks are there.')
            #
            # if (self.messageQueue.qsize() > self.max_tasks and len(self.consumers) < self.max_consumers):
            #
            #     newConsumer = ZMQPuller(self.messageQueue, self.resultQueue, self.stopEventQueue)
            #
            #     self.consumers.append(newConsumer)
            #
            #     newConsumer.start()
            #
            #     self.logger.info("Adding new consumer")
            #
            # elif self.messageQueue.qsize() == 0:
            #     for _ in range(aliveConsumerCount):
            #         self.messageQueue.put(None)
            #     break
            #
            # self.messageQueue.put(None)
            # self.logger.info("Killing a consumer.")

if __name__ == "__main__":
    kill_switch = multiprocessing.Event()

    # multiprocessing.log_to_stderr(logging.DEBUG)
    initializer(logging.DEBUG)

    queue_sub_to_push = multiprocessing.Queue()

    pusher = ZMQPusherQueue(queue_sub_to_push, kill_switch,host=r'127.0.0.1', port="5559")
    subscriber = ZMQSubscriberQueue(queue_sub_to_push, kill_switch, host=r'127.0.0.1', port="5558")

    service_manager = ServiceBroker(subscriber, pusher, kill_switch)

    try:
        service_manager.launch_puller()
        # service_manager.monitor_consumers()
        s = service_manager.get_server()
        s.serve_forever()
    except KeyboardInterrupt:
        print('parent received ctrl-c')

        kill_switch.set()
        time.sleep(2)
        subscriber.terminate()
        pusher.terminate()

        subscriber.join()
        pusher.join()
