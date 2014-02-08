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

class MemcacheServer(tornado.tcpserver.TCPServer):
    def handle_stream(self, stream, address):
        MemcacheConnection(stream, address)

class MemcacheConnection(object):
    MEMCACHED_VERSION = "1.4.17"

    def __init__(self, stream, address):
        self.stream = stream
        self.address = address
        self._request_finished = False
        self._header_callback = tornado.stack_context.wrap(self._on_headers)
        self._write_callback = None
        self._storage = dict() # TODO: pluggable storage
        logging.info("%d: Client using the ascii protocol" % (stream.fileno()))
        self.next_command()

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

    RETRIEVAL_COMMANDS = re.compile(r'^([a-z]+)(?: +(.*))?$')
    STORAGE_COMMANDS = re.compile(r'^([a-z]+) +(\S+) +(\d+) +(\d+) +(\d+)(?: +(noreply))?$')

    def _on_headers(self, data):
        data = data.rstrip().decode("utf-8")
        s = self.STORAGE_COMMANDS.match(data)
        if s:
            _command, _key, _flags, _exptime, _bytes, _noreply = s.groups()
            logging.info("<%d %s %s %s %s %s %s" % (self.stream.fileno(), _command, _key, _flags, _exptime, _bytes, _noreply or ""))
            self._request = MemcacheRequest(_command, _key, flags=int(_flags), exptime=int(_exptime), noreply=(_noreply=="noreply"))
            content_length = int(_bytes)
            if 0 < content_length:
                self.stream.read_bytes(content_length, self._on_request_body)
            else:
                self.write(b"ERROR\r\n")
        else:
          r = self.RETRIEVAL_COMMANDS.match(data)
          if r:
              _command, _key = r.groups()
              logging.info("<%d %s %s" % (self.stream.fileno(), _command, _key))
              self._request = MemcacheRequest(_command, _key if _key else "")
              self.request_callback(self._request)
          else:
              self.write(b"ERROR\r\n")
              self.next_command()

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
            self.next_command()

    def next_command(self):
        def _next_command():
            self._request = None
            self.stream.read_until_regex(b"\r?\n", self._header_callback)
        if 0.0 < tornado.options.options.slowdown:
            deadline = time.time() + tornado.options.options.slowdown
            self.stream.io_loop.add_timeout(deadline, _next_command)
        else:
            _next_command()

    ## Storage commands
    def on_set_command(self, request):
        self._storage[request.key] = request
        if not request.noreply:
            self.write(b"STORED\r\n")
        self.next_command()

    def on_add_command(self, request):
        if request.key in self._storage:
            if not request.noreply:
                self.write(b"NOT_STORED\r\n")
        else:
            self._storage[request.key] = request
            if not request.noreply:
                self.write(b"STORED\r\n")
        self.next_command()

    def on_replace_command(self, request):
        if request.key in self._storage:
            self._storage[request.key] = request
            if not request.noreply:
                self.write(b"STORED\r\n")
        else:
            if not reuqest.noreply:
                self.write(b"NOT_STORED\r\n")
        self.next_command()

    def on_append_command(self, request):
        if request.key in self._storage:
            request.body = self._storage[request.key].body + request.body
            self._storage[request.key] = request
            if not request.noreply:
                self.write(b"STORED\r\n")
        else:
            if not request.noreply:
                self.write(b"NOT_STORED\r\n")
        self.next_command()

    def on_prepend_command(self, request):
        if request.key in self._storage:
            request.body = request.body + self._storage[request.key].body
            self._storage[request.key] = request
            if not request.noreply:
                self.write(b"STORED\r\n")
        else:
            if not request.noreply:
                self.write(b"NOT_STORED\r\n")
        self.next_command()

    ## Retrieval commands
    def on_get_command(self, request):
        for key in request.keys():
            if key in self._storage:
                val = self._storage[key]
                if not val.expired():
                    self.write(("VALUE %s %d %d\r\n" % (key, val.flags, val.content_length())).encode("utf-8"))
                    self.write(val.body + b"\r\n")
        self.write(b"END\r\n")
        self.next_command()

    def on_delete_command(self, request):
        existed = request.key in self._storage
        del self._storage, request.key
        if not request.noreply:
            if existed:
                self.write(b"DELETED\r\n")
            else:
                self.write(b"NOT_FOUND\r\n")
        self.next_command()

    def on_touch_command(self, request):
        if request.key in self._storage:
            self._storage[request.key] = request
            if not request.noreply:
                self.write(b"TOUCHED\r\n")
        else:
            if not request.noreply:
                self.write(b"NOT_FOUND\r\n")
        self.next_command()

    ## other commands
    def on_quit_command(self, request):
        self.finish()

    def on_stats_command(self, request):
        self.write(("STAT pid %d\r\n" % os.getpid()).encode("utf-8"))
        self.write(("STAT time %d\r\n" % int(time.time())).encode("utf-8"))
        self.write(("STAT version %s\r\n" % self.MEMCACHED_VERSION).encode("utf-8"))
        self.write(("STAT bytes %d\r\n" % sum([ val.content_length for val in self._storage.values() ])).encode("utf-8"))
        self.write(("STAT total_items %d\r\n" % (len(self._storage))).encode("utf-8"))
        self.write(b"END\r\n")
        self.next_command()

    def on_version_command(self, request):
        self.write(("VERSION %s\r\n" % self.MEMCACHED_VERSION).encode("utf-8"))
        self.next_command()

class MemcacheRequest(object):
    def __init__(self, command, key, flags=None, exptime=None, noreply=False, body=None):
        self.command = command
        self.key = key
        self.flags = flags
        self.exptime = exptime
        self.noreply = noreply
        if isinstance(body, str):
            self.body = body.encode("utf-8")
        else:
            self.body = body or b""
        self._created = time.time()

    def keys(self):
        return re.split(r' +', self.key)

    def content_length(self):
        return len(self.body)

    def expired(self):
        if self.exptime == 0:
            return False
        else:
            if self.exptime < 60*60*24*30:
                return (self._created + self.exptime) < time.time()
            else:
                return self.exptime < time.time()

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
