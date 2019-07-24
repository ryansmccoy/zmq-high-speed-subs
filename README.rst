===============================
ZeroMQ - High-Speed Subscribers
===============================

.. image:: https://raw.githubusercontent.com/ryansmccoy/zmq-high-speed/master/docs/fig56.png
    :align: center

Example of High-Speed Subscriber Patterns in ZeroMQ

Quick Start Guides

Setup Environment & Run Example  (Windows):

    $ git clone https://github.com/ryansmccoy/zmq-high-speed
    $ cd zmq-high-speed
    $ conda create -n zmq-high-speed python=3.7
    $ activate zmq-high-speed
    $ pip install -r requirements.txt

#### Setup Environment & Run Example (Linux):

    $ git clone https://github.com/ryansmccoy/zmq-high-speed
    $ cd zmq-high-speed
    $ conda create -n zmq-high-speed python=3.7
    $ activate zmq-high-speed
    $ pip install -r requirements.txt

To run, open seperate terminal windows, run from the main directory:

    python zmq_high_speed/00_publish_data_feed.py

    python zmq_high_speed/01_subscriber_to_pusher.py

    python zmq_high_speed/02_pull_to_workers.py

* Free software: MIT license
* Documentation: https://zmq-high-speed.readthedocs.io.

