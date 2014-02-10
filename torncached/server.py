#!/usr/bin/env python

from __future__ import unicode_literals
import collections
import logging
import os
import re
import sys
import time
import tornado.autoreload
import tornado.ioloop
import tornado.options
import tornado.tcpserver
import tornado.stack_context
from tornado.util import bytes_type
import torncached.options
import torncached.storage

class MemcacheServer(tornado.tcpserver.TCPServer):
    def handle_stream(self, stream, address):
        MemcacheConnection(stream, address)

class MemcacheConnection(object):
    def __init__(self, stream, address):
        self.stream = stream
        self.address = address
        self._request_finished = False
        self._header_callback = tornado.stack_context.wrap(self._on_headers)
        self._write_callback = None
        self.storage = torncached.storage.MemcacheStorage()
        logging.info("%d: Client using the ascii protocol" % (stream.fileno()))
        self.read_next_command()

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
        if s:
            command, key, flags, exptime, _bytes, noreply = s.groups()
            self._request = MemcacheRequest(command, key,
                    flags=int(flags) if flags else 0,
                    exptime=int(exptime) if exptime else 0,
                    noreply=noreply=="noreply")
            content_length = int(_bytes) if _bytes else 0
            if 0 < content_length:
                self.stream.read_bytes(content_length, self._on_request_body)
            else:
                self.write(b"ERROR\r\n")
        else:
          r = self.RETRIEVAL_COMMANDS.match(data)
          if r:
              command, key = r.groups()
              self._request = MemcacheRequest(command, key if key else "")
              self.request_callback(self._request)
          else:
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

    def read_next_command(self):
        def _read_next_command():
            self._request = None
            self.stream.read_until_regex(b"\r?\n", self._header_callback)
        if 0.0 < tornado.options.options.slowdown:
            deadline = time.time() + tornado.options.options.slowdown
            self.stream.io_loop.add_timeout(deadline, _read_next_command)
        else:
            _read_next_command()

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
            if body and flags:
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
        for (key, val) in self.storage.stats().items():
            self.write(("STAT %s %s\r\n" % (key, str(val))).encode("utf-8"))
        self.write(b"END\r\n")
        self.read_next_command()

    def on_version_command(self, request):
        self.write(("VERSION %s\r\n" % self.storage.version()).encode("utf-8"))
        self.read_next_command()

class MemcacheRequest(object):
    def __init__(self, command, key, flags=None, exptime=None, noreply=False, body=None):
        self.command = command
        self.key = key
        self.flags = 0 if flags is None else flags
        self.exptime = 0 if exptime is None else exptime
        self.noreply = not not noreply
        if isinstance(body, str):
            self.body = body.encode("utf-8")
        else:
            self.body = body or b""

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
