import asyncio
import zmq
from zmq.asyncio import Context
import os

os.environ['PYTHONASYNCIODEBUG'] = '1'

queue = asyncio.Queue()

class ZMQSubscriber:
    def __init__(self):
        self.ctx = zmq.asyncio.Context()
        self.sock = self.ctx.socket(zmq.SUB)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, "")

    async def handle(self, queue):
        self.sock.connect('tcp://127.0.0.1:5558')

        while True:
            msg = await self.sock.recv_string()
            print(f"SUB -> {msg[0:100]}")
            await queue.put(msg)

class ZMQPusher:
    def __init__(self):
        self.ctx = zmq.asyncio.Context()
        self.sock = self.ctx.socket(zmq.PUSH)

    async def handle(self, queue):
        self.sock.bind('tcp://*:5559')

        while True:

            msg = await queue.get()

            if msg is None:
                # the producer emits None to indicate that it is done
                break
            else:
                print(f"PUSH -> {msg[0:100]}")
                self.sock.send_string(msg)
                queue.task_done()

def main():

    subscriber = ZMQSubscriber()
    pusher = ZMQPusher()

    loop = asyncio.get_event_loop()

    try:
        loop.create_task(subscriber.handle(queue))
        loop.create_task(pusher.handle(queue))
        loop.run_forever()
    finally:
        pusher.ctx.destroy()

if __name__ == '__main__':
    main()

