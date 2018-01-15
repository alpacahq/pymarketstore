from distutils.core import setup

setup(
    name='pymarketstore',
    version='0.9',
    description='Marketstore python driver',
    author='Alpaca',
    author_email='oss@alpaca.markets',
    url='https://github.com/alpacahq/pymarketstore',
    packages=['pymarketstore',],
    install_requires=[
        'msgpack-python',
        'numpy',
        'requests',
        'six',
        'urllib3',
        'pytest',
    ],
)
