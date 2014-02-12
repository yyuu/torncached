#!/usr/bin/env python

from __future__ import unicode_literals
import collections
import datetime
import functools
import logging
import os
import re
import struct
import sys
import tornado.autoreload
import tornado.ioloop
import tornado.options
import tornado.tcpserver
import tornado.stack_context
from tornado.util import bytes_type
import torncached.errors
import torncached.options
import torncached.storage

class MemcacheServer(tornado.tcpserver.TCPServer):
    def __init__(self, *args, **kwargs):
        super(MemcacheServer, self).__init__()
        self._storage = torncached.storage.MemcacheStorage()

    def handle_stream(self, stream, address):
        MemcacheConnection(stream, address, self._storage)

class MemcacheConnection(object):
    def __init__(self, stream, address, storage):
        stream.read_bytes(1, functools.partial(self.detect_protocol, stream, address, storage))

    def detect_protocol(self, stream, address, storage, buf):
        try:
            self._protocol = MemcacheBinaryProtocolHandler(stream, address, storage, buf)
        except torncached.errors.ProtocolError:
            self._protocol = MemcacheAsciiProtocolHandler(stream, address, storage, buf)

class MemcacheProtocolHandler(object):
    def __init__(self, stream, address, storage):
        self.stream = stream
        self.address = address
        self.storage = storage

class MemcacheAsciiProtocolHandler(MemcacheProtocolHandler):
    def __init__(self, stream, address, storage, buf=None):
        super(MemcacheAsciiProtocolHandler, self).__init__(stream, address, storage)
        self._request_finished = False
        self._header_callback = tornado.stack_context.wrap(self._on_headers)
        self._write_callback = None
        logging.info("%d: Client using the ascii protocol" % (stream.fileno()))
        self.read_next_command(buf)

    def close(self):
        logging.info("<%d connection closed." % (self.stream.fileno()))
        self.stream.close()

    def write(self, chunk, callback=None):
        assert self._request, "Request closed"
        if not self.stream.closed():
            logging.info(">%d %s" % (self.stream.fileno(), chunk.rstrip().decode("utf-8")))
            self._write_callback = tornado.stack_context.wrap(callback)
            self.stream.write(chunk, self._on_write_complete)

    def finish(self):
        assert self._request, "Request closed"
        self._request_finished = True
        if not self.stream.writing():
            self._finish_request()

    def _on_write_complete(self):
        if self._write_callback is not None:
            callback = self._write_callback
            self._write_callback = None
            callback()
        if self._request_finished and not self.stream.writing():
            self._finish_request()

    def _finish_request(self):
        self._request = None
        self._request_finished = False
        self.close()

    STORAGE_COMMANDS = re.compile(r'^([a-z]+) +(\S+) +(\d+) +(\d+) +(\d+)(?: +(noreply))?$')
    RETRIEVAL_COMMANDS = re.compile(r'^([a-z]+)(?: +(.*))?$')

    def _on_headers(self, data):
        data = data.rstrip().decode("utf-8")
        logging.info("<%d %s" % (self.stream.fileno(), data))
        s = self.STORAGE_COMMANDS.match(data)
        if s is not None:
            command, key, flags, exptime, _bytes, noreply = s.groups()
            self._request = MemcacheAsciiCommand(command, key,
                    flags=0 if flags is None else int(flags),
                    exptime=0 if exptime is None else int(exptime),
                    noreply=noreply=="noreply")
            content_length = 0 if _bytes is None else int(_bytes)
            if 0 < content_length:
                self.stream.read_bytes(content_length, self._on_request_body)
            else:
                self.write(b"ERROR\r\n")
        else:
          r = self.RETRIEVAL_COMMANDS.match(data)
          if r is not None:
              command, key = r.groups()
              self._request = MemcacheAsciiCommand(command, "" if key is None else key)
              self.request_callback(self._request)
          else:
              self._request = MemcacheAsciiCommand("", "")
              self.write(b"ERROR\r\n")
              self.read_next_command()

    def _on_request_body(self, data):
        def __on_request_body(newline):
            self._request.body = data
            self.request_callback(self._request)
        self.stream.read_until_regex(b"\r?\n", __on_request_body) # skip trailing newline

    def request_callback(self, request):
        command = "on_%s_command" % request.command
        if hasattr(self, command):
            getattr(self, command)(request)
        else:
            self.write(b"ERROR\r\n")
            self.read_next_command()

    def read_next_command(self, buf=None):
        def prepend_buffer(data):
            if buf is not None:
                data = buf + data
            self._header_callback(data)

        def read_command():
            self._request = None
            self.stream.read_until_regex(b"\r?\n", prepend_buffer)

        if 0.0 < tornado.options.options.slowdown:
            timedelta = datetime.timedelta(seconds=tornado.options.options.slowdown)
            self.stream.io_loop.add_timeout(timedelta, read_command)
        else:
            read_command()

    ## Storage commands
    def on_set_command(self, request):
        if not request.noreply:
            if self.storage.set(request.key, request.body, request.flags, request.exptime):
                self.write(b"STORED\r\n")
        self.read_next_command()

    def on_add_command(self, request):
        if not request.noreply:
            if self.storage.add(request.key, request.body, request.flags, request.exptime):
                self.write(b"STORED\r\n")
            else:
                self.write(b"NOT_STORED\r\n")
        self.read_next_command()

    def on_replace_command(self, request):
        if not request.noreply:
            if self.storage.replace(request.key, request.body, request.flags, request.exptime):
                self.write(b"STORED\r\n")
            else:
                self.write(b"NOT_STORED\r\n")
        self.read_next_command()

    def on_append_command(self, request):
        if not request.noreply:
            if self.storage.append(request.key, request.body, request.flags, request.exptime):
                self.write(b"STORED\r\n")
            else:
                self.write(b"NOT_STORED\r\n")
        self.read_next_command()

    def on_prepend_command(self, request):
        if not request.noreply:
            if self.storage.prepend(request.key, request.body, request.flags, request.exptime):
                self.write(b"STORED\r\n")
            else:
                self.write(b"NOT_STORED\r\n")
        self.read_next_command()

    ## Retrieval commands
    def on_get_command(self, request):
        # text protocol allows multiple get
        for key in re.split(r' +', request.key):
            body, flags = self.storage.get(key)
            if body is not None:
                self.write(("VALUE %s %d %d\r\n" % (key, flags, len(body))).encode("utf-8"))
                self.write(body + b"\r\n")
        self.write(b"END\r\n")
        self.read_next_command()

    def on_delete_command(self, request):
        if not request.noreply:
            if self.storage.delete(request.key):
                self.write(b"DELETED\r\n")
            else:
                self.write(b"NOT_FOUND\r\n")
        self.read_next_command()

    def on_touch_command(self, request):
        if not request.noreply:
            if self.storage.touch(request.key):
                self.write(b"TOUCHED\r\n")
            else:
                self.write(b"NOT_FOUND\r\n")
        self.read_next_command()

    ## other commands
    def on_quit_command(self, request):
        self.finish()

    def on_stats_command(self, request):
        for (key, val) in sorted(self.storage.stats().items()):
            self.write(("STAT %s %s\r\n" % (key, str(val))).encode("utf-8"))
        self.write(b"END\r\n")
        self.read_next_command()

    def on_version_command(self, request):
        self.write(("VERSION %s\r\n" % self.storage.version()).encode("utf-8"))
        self.read_next_command()

class MemcacheBinaryProtocolHandler(MemcacheProtocolHandler):
    def __init__(self, stream, address, storage, buf=None):
        if buf is not None:
            magic = struct.unpack(b"B", buf[0:1])
            if magic != 0x80:
                raise torncached.errors.ProtocolError("not binary protocol")

        super(MemcacheBinaryProtocolHandler, self).__init__(stream, address, storage)
        raise torncached.errors.ProtocolError("not implemented")

class MemcacheCommand(object):
    pass

class MemcacheAsciiCommand(MemcacheCommand):
    def __init__(self, command, key, flags=None, exptime=None, noreply=False, body=None):
        super(MemcacheAsciiCommand, self).__init__()
        self.command = command
        self.key = key
        self.flags = 0 if flags is None else flags
        self.exptime = 0 if exptime is None else exptime
        self.noreply = not not noreply
        if isinstance(body, str):
            self.body = body.encode("utf-8")
        else:
            self.body = body or b""

class MemcacheBinaryCommand(MemcacheCommand):
    def __init__(self, *args, **kwargs):
        super(MemcacheBinaryCommand, self).__init__()

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
