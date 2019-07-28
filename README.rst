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

        2019-07-27 21:03:58,977 INFO     [MainProcess(43648)]
        2019-07-27 21:03:58,977 INFO     [MainProcess(43648)] ['run', '07-27-2019_21:03:58:877426', 'Q', 'XLF', 'None', '28.64']
        2019-07-27 21:03:58,981 INFO     [MainProcess(43648)]
        2019-07-27 21:03:58,984 INFO     [MainProcess(43648)] Time Elapsed:     10.1 seconds
        2019-07-27 21:03:58,986 INFO     [MainProcess(43648)] Messages During Period:   7048
        2019-07-27 21:03:58,988 INFO     [MainProcess(43648)] Messages Per Second:      697.89
        2019-07-27 21:03:58,990 INFO     [MainProcess(43648)]
        2019-07-27 21:03:58,993 INFO     [MainProcess(43648)] Total Message Broker Messages:    51816
        2019-07-27 21:03:58,996 INFO     [MainProcess(43648)]
        2019-07-27 21:03:59,000 INFO     [MainProcess(43648)] Message Broker Queue Size:        0
        2019-07-27 21:03:59,003 INFO     [MainProcess(43648)]
        2019-07-27 21:03:59,006 INFO     [MainProcess(43648)]


        2019-07-27 21:04:02,058 INFO     [MessageConsumer-1(10880)]
        2019-07-27 21:04:02,058 INFO     [MessageConsumer-1(10880)]
        2019-07-27 21:04:02,058 INFO     [MessageConsumer-1(10880)] Time Elapsed:       10.0 seconds
        2019-07-27 21:04:02,059 INFO     [MessageConsumer-1(10880)] Messages During Period:     6443
        2019-07-27 21:04:02,059 INFO     [MessageConsumer-1(10880)] Messages Per Second:        644.28
        2019-07-27 21:04:02,060 INFO     [MessageConsumer-1(10880)]
        2019-07-27 21:04:02,060 INFO     [MessageConsumer-1(10880)] Total Consumer Messages:    53320
        2019-07-27 21:04:02,060 INFO     [MessageConsumer-1(10880)]
        2019-07-27 21:04:02,060 INFO     [MessageConsumer-1(10880)] Current Queue Size: 670
        2019-07-27 21:04:02,061 INFO     [MessageConsumer-1(10880)]
        2019-07-27 21:04:02,061 INFO     [MessageConsumer-1(10880)]


        2019-07-27 21:04:05,170 INFO     [DatabaseConsumer-2(28376)]
        2019-07-27 21:04:05,170 INFO     [DatabaseConsumer-2(28376)]
        2019-07-27 21:04:05,172 INFO     [DatabaseConsumer-2(28376)] Time Elapsed:      14.44 seconds
        2019-07-27 21:04:05,175 INFO     [DatabaseConsumer-2(28376)] Messages During Period:    5001
        2019-07-27 21:04:05,177 INFO     [DatabaseConsumer-2(28376)] Messages Per Second:       346.23
        2019-07-27 21:04:05,178 INFO     [DatabaseConsumer-2(28376)]
        2019-07-27 21:04:05,179 INFO     [DatabaseConsumer-2(28376)] Total Database Messages:   55000
        2019-07-27 21:04:05,182 INFO     [DatabaseConsumer-2(28376)]
        2019-07-27 21:04:05,183 INFO     [DatabaseConsumer-2(28376)]


* Free software: MIT license
* Documentation: https://zeromq-high-speed-subscribers.readthedocs.io.

