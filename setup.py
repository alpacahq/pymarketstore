#!/usr/bin/env python
from setuptools import setup
from pymarketstore import __version__

setup(
    name='pymarketstore',
    version=__version__,
    description='Marketstore python driver',
    author='Alpaca',
    author_email='oss@alpaca.markets',
    url='https://github.com/alpacahq/pymarketstore',
    keywords='database,pandas,financial,timeseries',
    packages=['pymarketstore', ],
    install_requires=[
        'msgpack-python',
        'numpy',
        'requests',
        'six',
        'urllib3',
        'pytest',
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'coverage>=4.4.1',
        'mock>=1.0.1'
    ],
    setup_requires=['pytest-runner', 'flake8'],
)
