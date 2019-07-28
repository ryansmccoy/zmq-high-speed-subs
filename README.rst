===============================
ZeroMQ - High-Speed Subscribers
===============================

Example of High-Speed Subscriber Patterns in ZeroMQ

.. image:: https://raw.githubusercontent.com/ryansmccoy/zeromq-high-speed-subscribers/master/docs/fig56.png
    :width: 700px
    :align: center
    :height: 500px
    :alt: The Simple Black Box Pattern

* ZeroMQ Guide: http://zguide.zeromq.org/py:chapter5#toc5

Setup Environment & Run Example  (Windows):

.. code-block:: batch

    git clone https://github.com/ryansmccoy/zeromq-high-speed-subscribers
    cd zeromq-high-speed-subscribers
    conda create -n zeromq-high-speed-subscribers1 python=3.7 -y
    activate zeromq-high-speed-subscribers1
    pip install -r requirements.txt

Setup Environment & Run Example (Linux):

.. code-block:: batch

    git clone https://github.com/ryansmccoy/zeromq-high-speed-subscribers
    cd zeromq-high-speed-subscribers
    conda create -n zeromq-high-speed-subscribers python=3.7 -y
    pip install -r requirements.txt

To run, open seperate terminal windows, run:

Publisher

.. code-block:: bash

    $   activate zeromq-high-speed-subscribers
    $   python zeromq_high_speed_subscribers/00_publish_data_feed.py

    Output:

        2019-07-27 21:04:10,669 INFO     [MainProcess(16508)]
        2019-07-27 21:04:10,670 INFO     [MainProcess(16508)] run,07-27-2019_21:04:10:6
        2019-07-27 21:04:10,670 INFO     [MainProcess(16508)]
        2019-07-27 21:04:10,670 INFO     [MainProcess(16508)] Time Elapsed:     10.0 seconds
        2019-07-27 21:04:10,670 INFO     [MainProcess(16508)] Messages During Period:   6328
        2019-07-27 21:04:10,671 INFO     [MainProcess(16508)] Messages Per Second:      632.74
        2019-07-27 21:04:10,671 INFO     [MainProcess(16508)]
        2019-07-27 21:04:10,671 INFO     [MainProcess(16508)] Total Messages Published:    59359
        2019-07-27 21:04:10,672 INFO     [MainProcess(16508)]
        2019-07-27 21:04:10,674 INFO     [MainProcess(16508)]

Subscriber-to-Pusher

.. code-block:: bash

    $   activate zeromq-high-speed-subscribers
    $   python zeromq_high_speed_subscribers/01_subscriber_to_pusher_queue.py

    Output:

        2019-07-27 21:04:50,995 INFO     [ZMQSubscriber-1(45260)]
        2019-07-27 21:04:50,996 INFO     [ZMQSubscriber-1(45260)]
        2019-07-27 21:04:50,996 INFO     [ZMQSubscriber-1(45260)] Time Elapsed: 10.0 seconds
        2019-07-27 21:04:50,996 INFO     [ZMQSubscriber-1(45260)] Messages During Period:       83388
        2019-07-27 21:04:50,996 INFO     [ZMQSubscriber-1(45260)] Messages Per Second:  626.46
        2019-07-27 21:04:50,997 INFO     [ZMQSubscriber-1(45260)] Total Subscriber Messages:    83388
        2019-07-27 21:04:50,997 INFO     [ZMQSubscriber-1(45260)] Current Queue Size:   18
        2019-07-27 21:04:50,997 INFO     [ZMQSubscriber-1(45260)] b'\xda\x01\xc6run,07-27-2019_21:04:50:995213,83375,FB192'
        2019-07-27 21:04:50,998 INFO     [ZMQSubscriber-1(45260)]
        2019-07-27 21:04:50,998 INFO     [ZMQSubscriber-1(45260)]


        2019-07-27 21:04:55,096 INFO     [ZMQPusher-2(21312)]
        2019-07-27 21:04:55,096 INFO     [ZMQPusher-2(21312)]
        2019-07-27 21:04:55,097 INFO     [ZMQPusher-2(21312)] Time Elapsed:     10.05 seconds
        2019-07-27 21:04:55,097 INFO     [ZMQPusher-2(21312)] Messages During Period:   85605
        2019-07-27 21:04:55,098 INFO     [ZMQPusher-2(21312)] Messages Per Second:      594.63
        2019-07-27 21:04:55,099 INFO     [ZMQPusher-2(21312)] Total Pusher Messages:    85605
        2019-07-27 21:04:55,099 INFO     [ZMQPusher-2(21312)] Current Queue Size:       49
        2019-07-27 21:04:55,100 INFO     [ZMQPusher-2(21312)] b'\xda\x01\xe2run,07-27-2019_21:04:54:994628,85592,FB192'
        2019-07-27 21:04:55,100 INFO     [ZMQPusher-2(21312)]
        2019-07-27 21:04:55,101 INFO     [ZMQPusher-2(21312)]


Pull-to-Workers

.. code-block:: bash

    $   activate zeromq-high-speed-subscribers
    $   python zeromq_high_speed_subscribers/02_pull_to_workers.py

    Output:

        2019-07-28 00:38:51,177 INFO     [MainProcess(42868)]
        2019-07-28 00:38:51,178 INFO     [MainProcess(42868)] ['run', '07-28-2019_00:38:51:076861', 'Q', 'XLF', 'None', '28.64']
        2019-07-28 00:38:51,182 INFO     [MainProcess(42868)]
        2019-07-28 00:38:51,184 INFO     [MainProcess(42868)] Time Elapsed:     10.06 seconds
        2019-07-28 00:38:51,185 INFO     [MainProcess(42868)] Messages During Period:   6621
        2019-07-28 00:38:51,186 INFO     [MainProcess(42868)] Messages Per Second:      658.2
        2019-07-28 00:38:51,187 INFO     [MainProcess(42868)]
        2019-07-28 00:38:51,188 INFO     [MainProcess(42868)] Total Message Broker Messages:    108931
        2019-07-28 00:38:51,189 INFO     [MainProcess(42868)]
        2019-07-28 00:38:51,190 INFO     [MainProcess(42868)] Message Broker Queue Size:        0
        2019-07-28 00:38:51,191 INFO     [MainProcess(42868)]
        2019-07-28 00:38:51,192 INFO     [MainProcess(42868)]

        2019-07-28 00:38:46,282 INFO     [MessageConsumer-1(33732)]
        2019-07-28 00:38:46,283 INFO     [MessageConsumer-1(33732)]
        2019-07-28 00:38:46,284 INFO     [MessageConsumer-1(33732)] Time Elapsed:       10.02 seconds
        2019-07-28 00:38:46,295 INFO     [MessageConsumer-1(33732)] Messages During Period:     6256
        2019-07-28 00:38:46,299 INFO     [MessageConsumer-1(33732)] Messages Per Second:        624.08
        2019-07-28 00:38:46,304 INFO     [MessageConsumer-1(33732)]
        2019-07-28 00:38:46,308 INFO     [MessageConsumer-1(33732)] Total Consumer Messages:    106677
        2019-07-28 00:38:46,312 INFO     [MessageConsumer-1(33732)]
        2019-07-28 00:38:46,317 INFO     [MessageConsumer-1(33732)] Current Queue Size: 44
        2019-07-28 00:38:46,320 INFO     [MessageConsumer-1(33732)]
        2019-07-28 00:38:46,323 INFO     [MessageConsumer-1(33732)]

        2019-07-28 00:38:37,991 INFO     [DatabaseConsumer-2(24932)]
        2019-07-28 00:38:37,992 INFO     [DatabaseConsumer-2(24932)]
        2019-07-28 00:38:37,993 INFO     [DatabaseConsumer-2(24932)] Time Elapsed:      15.96 seconds
        2019-07-28 00:38:37,994 INFO     [DatabaseConsumer-2(24932)] Messages During Period:    10001
        2019-07-28 00:38:37,996 INFO     [DatabaseConsumer-2(24932)] Messages Per Second:       626.78
        2019-07-28 00:38:37,996 INFO     [DatabaseConsumer-2(24932)]
        2019-07-28 00:38:37,997 INFO     [DatabaseConsumer-2(24932)] Total Database Messages:   100000
        2019-07-28 00:38:37,997 INFO     [DatabaseConsumer-2(24932)]
        2019-07-28 00:38:37,998 INFO     [DatabaseConsumer-2(24932)]



* Free software: MIT license
* Documentation: https://zeromq-high-speed-subscribers.readthedocs.io.

