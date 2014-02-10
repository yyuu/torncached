#!/usr/bin/env python

from __future__ import unicode_literals
import collections
import functools
import logging
import os
import re
import struct
import sys
import time
import tornado.autoreload
import tornado.ioloop
import tornado.options
import tornado.tcpserver
from tornado.util import bytes_type
import torncached.ascii
import torncached.binary
import torncached.errors
import torncached.options
import torncached.storage

class MemcacheServer(tornado.tcpserver.TCPServer):
    def handle_stream(self, stream, address):
        MemcacheConnection(stream, address)

class MemcacheConnection(object):
    def __init__(self, stream, address):
        stream.read_bytes(1, functools.partial(self.detect_protocol, stream, address))

    def detect_protocol(self, stream, address, buf):
        try:
            self._protocol = torncached.binary.MemcacheBinaryProtocol(stream, address, buf)
        except torncached.errors.ProtocolError:
            self._protocol = torncached.ascii.MemcacheAsciiProtocol(stream, address, buf)

def main():
    torncached.options.define_options()
    tornado.options.parse_command_line(sys.argv)
    server = torncached.server.MemcacheServer()
    server.listen(tornado.options.options.port)
    logging.info("server listening (%d/tcp)" % tornado.options.options.port)
    if tornado.options.options.autoreload:
        logging.info("autoreload is enabled")
        tornado.autoreload.start()
    if tornado.options.options.slowdown:
        logging.info("simulate response slowdown of %.1f second(s)" % tornado.options.options.slowdown)
    tornado.ioloop.IOLoop.instance().start()

# vim:set ft=python :
