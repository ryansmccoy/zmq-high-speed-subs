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
    conda create -n zeromq-high-speed-subscribers python=3.7 -y
    activate zeromq-high-speed-subscribers
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

    $ activate zeromq-high-speed-subscribers
    $ python zeromq_high_speed_subscribers/00_publish_data_feed.py

    Output:

        Starting        07-24-2019_06:02:53:926534,
        run,07-24-2019_06:02:54:935560,5000,    Q,EES,nan,1994.4,nan,11,200,03:02.2,nan,nan,1990.0,-0.45,5,1
        run,07-24-2019_06:03:07:639177,70000,    Q,EEQT,nan,298.49,nan,5,1000,03:02.9,nan,nan,298.45,0.02,11

        Time Elapsed:   15.0 seconds
        Total Messages: 74,907
        Messages Per Second:    4,993.57

Subscriber-to-Pusher

.. code-block:: bash

    $ activate zeromq-high-speed-subscribers
    $ python zeromq_high_speed_subscribers/01_subscriber_to_pusher_multi.py

    Output:
        Time Elapsed:   17.08 seconds
        Total Messages: 76216
        Messages Per Second:    4463.351

Pull-to-Workers

.. code-block:: bash

    $ activate zeromq-high-speed-subscribers
    $ python zeromq_high_speed_subscribers/02_pull_to_workers.py

    Output:
        [INFO/Process 1-pid:51780]      Spawning Worker 1
        [INFO/Process 2-pid:47856]      Spawning Worker 2
        [INFO/Process 3-pid:29372]      Spawning Worker 3
        [INFO/Process 4-pid:38376]      Spawning Worker 4

        [INFO/Process 1-pid:51780]      Time Elapsed:   14.72 seconds
        [INFO/Process 1-pid:51780]      Total Messages: 19628
        [INFO/Process 1-pid:51780]      Messages Per Second:    1333.159

        [INFO/Process 3-pid:29372]      Time Elapsed:   14.72 seconds
        [INFO/Process 3-pid:29372]      Total Messages: 19627
        [INFO/Process 3-pid:29372]      Messages Per Second:    1333.091

        [INFO/Process 4-pid:38376]      Time Elapsed:   14.72 seconds
        [INFO/Process 4-pid:38376]      Total Messages: 19627
        [INFO/Process 4-pid:38376]      Messages Per Second:    1333.544

        [INFO/Process 2-pid:47856]      Time Elapsed:   14.72 seconds
        [INFO/Process 2-pid:47856]      Total Messages: 19628
        [INFO/Process 2-pid:47856]      Messages Per Second:    1333.159


* Free software: MIT license
* Documentation: https://zeromq-high-speed-subscribers.readthedocs.io.

