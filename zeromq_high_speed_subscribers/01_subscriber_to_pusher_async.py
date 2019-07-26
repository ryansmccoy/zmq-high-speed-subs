"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""

import asyncio
import zmq
from zmq.asyncio import Context
import os
from datetime import datetime
os.environ['PYTHONASYNCIODEBUG'] = '1'

queue = asyncio.Queue()

class ZMQSubscriber:
    def __init__(self, port="5558"):
        self.ctx = zmq.asyncio.Context()
        self.sock = self.ctx.socket(zmq.SUB)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, "")
        self.port = port
        self.counter = 0
        self.counter_print = 0
        print(f"\n\tStarting Subscriber on {port}")

    async def handle(self, queue):
        self.sock.connect(f'tcp://127.0.0.1:{self.port}')
        self.counter += 0

        while True:
            msg = await self.sock.recv_string()
            # print(f"SUB -> {msg[0:100]}")
            await queue.put(msg)

class ZMQPusher:
    def __init__(self, port="5559"):
        self.ctx = zmq.asyncio.Context()
        self.push_socket = self.ctx.socket(zmq.PUSH)
        self.port = port
        self.counter = 0
        self.counter_print = 0
        self.shutdown_messages = ["stop"] * 5
        print(f"\n\tStarting Pusher on {port}")

    async def handle(self, queue):
        self.push_socket.bind(f'tcp://*:{self.port}')

        while True:
            msg = await queue.get()
            self.counter += 1
            self.counter_print += 1

            if self.counter_print >= 1000:
                print(f'\n[INFO/ZMQPusher] ->\t {datetime.now().strftime("%m-%d-%Y_%H:%M:%S:%f,")}')
                print(f"\n[INFO/ZMQPusher] ->\t {msg[0:100]}")
                self.counter_print = 0

            if msg[0:4] == "stop":
                # the producer emits None to indicate that it is done
                [self.push_socket.send_string(message) for message in self.shutdown_messages]
                print("\nStarting Shutdown...")
                await asyncio.sleep(10)
                self.push_socket.close()
                self.ctx.term()
                return
            else:
                # print(f"PUSH -> {msg[0:100]}")
                self.push_socket.send_string(msg)
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

