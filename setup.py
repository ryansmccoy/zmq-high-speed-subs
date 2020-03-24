#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=6.0', 'pyzmq', 'pandas',"git+git://github.com/portugueslab/arrayqueues.git", "sqlalchemy", "numpy", "msgpack", "pyodbc", "requests", ""]

setup_requirements = [ ]

test_requirements = [ ]

setup(
    author="Ryan S. McCoy",
    author_email='github@ryansmccoy.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="Examples of High-Speed Subscriber Patterns in ZeroMQ",
    entry_points={
        'console_scripts': [
            'zmq_high_speed_subs=zmq_high_speed_subs.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='zmq_high_speed_subs',
    name='zmq_high_speed_subs',
    packages=find_packages(include=['zmq_high_speed_subs']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/ryansmccoy/zeromq-high-speed-subscribers',
    version='0.1.0',
    zip_safe=False,
)
