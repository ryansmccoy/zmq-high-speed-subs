===============================
ZeroMQ - High-Speed Subscribers
===============================

Example of High-Speed Subscriber Patterns in ZeroMQ

.. figure:: https://raw.githubusercontent.com/ryansmccoy/zmq-high-speed/master/docs/fig56.png
    :align: center

* ZeroMQ Guide: http://zguide.zeromq.org/py:chapter5#toc5

Setup Environment & Run Example  (Windows):

.. code-block:: batch

    git clone https://github.com/ryansmccoy/zmq-high-speed
    cd zmq-high-speed
    conda create -n zmq-high-speed python=3.7 -y
    activate zmq-high-speed
    pip install -r requirements.txt

Setup Environment & Run Example (Linux):

.. code-block:: batch

    git clone https://github.com/ryansmccoy/zmq-high-speed
    cd zmq-high-speed
    conda create -n zmq-high-speed python=3.7 -y
    activate zmq-high-speed
    pip install -r requirements.txt

To run, open seperate terminal windows, run from the main directory:

.. code-block:: bash

    python zmq_high_speed/00_publish_data_feed.py

    python zmq_high_speed/01_subscriber_to_pusher.py

    python zmq_high_speed/02_pull_to_workers.py

* Free software: MIT license
* Documentation: https://zmq-high-speed.readthedocs.io.

