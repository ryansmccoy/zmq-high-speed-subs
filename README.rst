===============================
ZeroMQ - High-Speed Subscribers
===============================

Example of High-Speed Subscriber Patterns in ZeroMQ

.. image:: https://raw.githubusercontent.com/ryansmccoy/zmq-high-speed/master/docs/fig56.png
    :width: 700px
    :align: center
    :height: 500px
    :alt: The Simple Black Box Pattern

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

To run, open seperate terminal windows, run each from the main directory:

.. code-block:: bash

    $ python zmq_high_speed/00_publish_data_feed.py

    Starting        07-24-2019_06:02:53:926534,
    run,07-24-2019_06:02:54:935560,5000,    Q,EES,nan,1994.4,nan,11,200,03:02.2,nan,nan,1990.0,-0.45,5,1
    run,07-24-2019_06:03:07:639177,70000,    Q,EEQT,nan,298.49,nan,5,1000,03:02.9,nan,nan,298.45,0.02,11

    Time Elapsed:   15.0 seconds
    Total Messages: 74907
    Messages Per Second:    4993.573


.. code-block:: bash

    $ python zmq_high_speed/01_subscriber_to_pusher_multi.py

        Time Elapsed:   17.08 seconds
        Total Messages: 76216
        Messages Per Second:    4463.351

.. code-block:: bash

    python zmq_high_speed/02_pull_to_workers.py



Shutting Down...

* Free software: MIT license
* Documentation: https://zmq-high-speed.readthedocs.io.

