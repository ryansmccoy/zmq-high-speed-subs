===============================
ZeroMQ - High-Speed Subscribers
===============================
Example of High-Speed Subscriber Patterns in ZeroMQ

.. image:: https://storage.googleapis.com/ryansmccoy/zeromq_high_speed.png
    :width: 700px
    :height: 500px
    :alt: The Simple Black Box Pattern

* ZeroMQ Guide: http://zguide.zeromq.org/py:chapter5#toc5

Setup Environment & Run Example  (Windows):

.. code-block:: batch

    git clone https://github.com/ryansmccoy/zmq-high-speed-subs
    cd zmq-high-speed-subs
    conda create -n zmq-high-speed-subs python=3.7 -y
    activate zmq-high-speed-subs
    pip install -r requirements.txt

Setup Environment & Run Example (Linux):

.. code-block:: batch

    git clone https://github.com/ryansmccoy/zmq-high-speed-subs
    cd zmq-high-speed-subs
    conda create -n zmq-high-speed-subs python=3.7 -y
    pip install -r requirements.txt

To run, open seperate terminal windows, run:

Publisher

.. code-block:: bash

    $   activate zmq-high-speed-subs
    $   python zmq_high_speed_subs\_00_publisher_fakefeed.py

    Output:

        2019-07-31 06:21:45,898 INFO     [Publisher-1(30632)]
        2019-07-31 06:21:45,967 INFO     [Publisher-1(30632)]
        2019-07-31 06:21:45,986 INFO     [Publisher-1(30632)] Time Elapsed: 10.1 seconds
        2019-07-31 06:21:46,018 INFO     [Publisher-1(30632)] Messages During Period:       55008
        2019-07-31 06:21:46,026 INFO     [Publisher-1(30632)] Messages Per Second:  5443.9
        2019-07-31 06:21:46,032 INFO     [Publisher-1(30632)]
        2019-07-31 06:21:46,041 INFO     [Publisher-1(30632)] Total Messages Published :    32950459
        2019-07-31 06:21:46,059 INFO     [Publisher-1(30632)]
        2019-07-31 06:21:46,065 INFO     [Publisher-1(30632)]

Subscriber-to-Pusher

.. code-block:: bash

    $   activate zmq-high-speed-subs
    $   python zmq_high_speed_subs/_10_manager.py

    Output:

        2019-07-31 04:57:47,588 INFO     [ZMQSubscriber(31384)]
        2019-07-31 04:57:47,588 INFO     [ZMQSubscriber(31384)] Time Elapsed:	10 seconds
        2019-07-31 04:57:47,589 INFO     [ZMQSubscriber(31384)] Messages During Period:	7359215
        2019-07-31 04:57:47,589 INFO     [ZMQSubscriber(31384)] Messages Per Second:	5502.83
        2019-07-31 04:57:47,589 INFO     [ZMQSubscriber(31384)]
        2019-07-31 04:57:47,589 INFO     [ZMQSubscriber(31384)] Total Time Elapsed:	1516.85 seconds
        2019-07-31 04:57:47,590 INFO     [ZMQSubscriber(31384)] Total Messages:	7359215
        2019-07-31 04:57:47,590 INFO     [ZMQSubscriber(31384)] Total Messages Per Second:	4851.64
        2019-07-31 04:57:47,591 INFO     [ZMQSubscriber(31384)]
        2019-07-31 04:57:47,591 INFO     [ZMQSubscriber(31384)] Current _Queue Size:	23
        2019-07-31 04:57:47,592 INFO     [ZMQSubscriber(31384)]

        2019-07-31 04:57:55,951 INFO     [ZMQPusher(45028)]
        2019-07-31 04:57:55,951 INFO     [ZMQPusher(45028)] Time Elapsed:	10 seconds
        2019-07-31 04:57:55,952 INFO     [ZMQPusher(45028)] Messages During Period:	55008
        2019-07-31 04:57:55,952 INFO     [ZMQPusher(45028)] Messages Per Second:	5494.9
        2019-07-31 04:57:55,952 INFO     [ZMQPusher(45028)]
        2019-07-31 04:57:55,953 INFO     [ZMQPusher(45028)] Total Time Elapsed:	1524.16 seconds
        2019-07-31 04:57:55,953 INFO     [ZMQPusher(45028)] Total Messages:	7414199
        2019-07-31 04:57:55,953 INFO     [ZMQPusher(45028)] Total Messages Per Second:	4864.46
        2019-07-31 04:57:55,954 INFO     [ZMQPusher(45028)]
        2019-07-31 04:57:55,954 INFO     [ZMQPusher(45028)] Current _Queue Size:	0
        2019-07-31 04:57:55,954 INFO     [ZMQPusher(45028)]

        2019-07-31 04:57:56,005 INFO     [ZMQPuller (30656)]
        2019-07-31 04:57:56,005 INFO     [ZMQPuller (30656)] Time Elapsed:	10 seconds
        2019-07-31 04:57:56,006 INFO     [ZMQPuller (30656)] Messages During Period:	55000
        2019-07-31 04:57:56,006 INFO     [ZMQPuller (30656)] Messages Per Second:	5484.78
        2019-07-31 04:57:56,006 INFO     [ZMQPuller (30656)]
        2019-07-31 04:57:56,007 INFO     [ZMQPuller (30656)] Total Time Elapsed:	1517.3 seconds
        2019-07-31 04:57:56,007 INFO     [ZMQPuller (30656)] Total Messages Distributed to finished Workers:	7390000
        2019-07-31 04:57:56,008 INFO     [ZMQPuller (30656)] Total Messages Per Second:	4870.48
        2019-07-31 04:57:56,008 INFO     [ZMQPuller (30656)]
        2019-07-31 04:57:56,010 INFO     [ZMQPuller (30656)] 	Total Messages in _Queue:	 1699
        2019-07-31 04:57:56,010 INFO     [ZMQPuller (30656)] 	Currently Running Workers:	 12
        2019-07-31 04:57:56,011 INFO     [ZMQPuller (30656)]

        2019-07-31 04:57:53,607 INFO     [Worker    (23848)]
        2019-07-31 04:57:53,607 INFO     [Worker    (23848)] Worker Messages Time Elapsed:	1.71 seconds
        2019-07-31 04:57:53,607 INFO     [Worker    (23848)] Worker Messages:	2501
        2019-07-31 04:57:53,608 INFO     [Worker    (23848)] Worker Messages Per Second:	1458.61
        2019-07-31 04:57:53,608 INFO     [Worker    (23848)]
        2019-07-31 04:57:53,608 INFO     [Worker    (23848)] COMPLETED
        2019-07-31 04:57:53,608 INFO     [Worker    (23848)]


If want to connect to database, rename .env.template to .env and enter your data base info:

.. code-block:: bash

    # .env
    DB_HOST=localhost
    DB_DATABASE=zmq-example
    DB_TABLE=data
    DB_USERNAME=python
    DB_PASSWORD=h1ghsp33d


* Free software: MIT license
* Documentation: https://zmq-high-speed-subs.readthedocs.io.

