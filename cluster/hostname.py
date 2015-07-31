#!/usr/bin/python
from __future__ import print_function
import socket

if __name__ == '__main__':
    print('hostname: ' + socket.gethostname())
    print('fqdn: ' + socket.getfqdn())    