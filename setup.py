import time
from distutils.core import setup


setup(version=time.strftime('%y%m%d'),
      name='basicpaxos',
      py_modules=['basicpaxos'],
      description='A client library to provide a strongly consistent KeyValue storage using Paxos/MySQL',
      author='Bhupendra Singh',
      author_email='bhsingh@gmail.com',
      url='https://github.com/magicray/BasicPaxos')

# python3 setup.py sdist upload
