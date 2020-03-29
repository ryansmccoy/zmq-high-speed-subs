"""

Copyright (C) 2019 Ryan S. McCoy <github@ryansmccoy.com>

MIT License

"""

import asyncio
import os

from _11_subscriber import ZMQSubscriberAsync
from _12_pusher import ZMQPusherAsync

os.environ['PYTHONASYNCIODEBUG'] = '1'

queue = asyncio.Queue()


def main():

    subscriber = ZMQSubscriberAsync()
    pusher = ZMQPusherAsync()

    loop = asyncio.get_event_loop()

    try:
        loop.create_task(subscriber.handle(queue))
        loop.create_task(pusher.handle(queue))
        loop.run_forever()
    finally:
        pusher.ctx.destroy()

if __name__ == '__main__':
    main()

