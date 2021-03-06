#!/usr/bin/env python

import ast
import re
import os
import sys
from setuptools import setup, find_packages
from setuptools.command.install import install

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('pymarketstore/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

with open('README.md') as readme_file:
    README = readme_file.read()


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self):
        tag = os.getenv('CIRCLE_TAG')

        if tag != version:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, version
            )
            sys.exit(info)


setup(
    name='pymarketstore',
    version=version,
    description='Marketstore python driver',
    long_description=README,
    long_description_content_type='text/markdown',
    author='Alpaca',
    author_email='oss@alpaca.markets',
    url='https://github.com/alpacahq/pymarketstore',
    keywords='database,pandas,financial,timeseries',
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        'msgpack-python',
        'numpy',
        'requests',
        'pandas',
        'six',
        'urllib3',
        'pytest',
        'websocket-client',
        'protobuf>=3.11.3',
        'grpcio'
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'coverage>=4.4.1',
        'mock>=1.0.1',
        'grpcio-tools'
    ],
    setup_requires=['pytest-runner', 'flake8'],
    cmdclass={
        'verify': VerifyVersionCommand,
    },
)
