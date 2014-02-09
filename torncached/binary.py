#!/usr/bin/env python

from __future__ import unicode_literals
import logging
import os
import re
import struct
import sys
import tornado.stack_context
import torncached.storage

class MemcacheBinaryConnection(object):
    REQUEST_MAGIC = 0x80
    RESPONES_MAGIC = 0x81

    def __init__(self, stream, address, data=None):
        self.stream = stream
        self.address = address
        self._request_finished = False
        self._header_callback = tornado.stack_context.wrap(self._on_headers)
        self._write_callback = None
        self.storage = torncached.storage.MemcacheStorage()
        logging.info("%d: Client using the binary protocol" % (stream.fileno()))
        self.read_next_command(data)

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

    NO_ERROR = 0x0000
    KEY_NOT_FOUND = 0x0001
    KEY_EXISTS = 0x0002
    VALUE_TOO_LARGE = 0x0003
    INVALID_ARGUMENTS = 0x0004
    ITEM_NOT_STORED = 0x0005
    NON_NUMERIC_VALUE = 0x0006
    UNKNOWN_COMMAND = 0x0081
    OUT_OF_MEMORY = 0x0082

    def _on_headers(self, data):
        magic, opcode, key_length, extra_length, data_type, status, total_body, opaque, cas = struct.unpack("BBHBBHIIL", data)
        self._request = MemcacheBinaryCommand(opcode, key_length, extra_length, data_type, status, total_body, opaque, cas)
        if self._request.command:
            self.stream.read_bytes(self._request.key_length, self._on_key)
        else:
            self.send_error(status=self.UNKNOWN_COMMAND)
            self.read_next_command()

    def _on_key(self, data):
        self._request.key = data.decode("utf-8")
        self.stream.read_bytes(self._request.total_body-self._request.key_length, self._on_body)

    def _on_body(self, data):
        self._request.body = data
        command = "on_%s_command" % self._request.command
        if hasattr(self, command):
            getattr(self, command)(self._request)
        else:
            self.send_error(status=self.UNKNOWN_COMMAND)
            self.read_next_command()

    def start_response(self, magic=0x81, opcode=0x00, key_length=0x0000, extra_length=0x00, data_type=0x00, status=0x00, total_body=0x00, opaque=0x00, cas=0x00):
        data = struct.pack("BBHBBHIIL", magic, opcode, key_length, extra_length, data_type, status, total_body, opaque, cas)
        self.write(data)

    def send_error(self, *args, **kwargs):
        self.start_response(*args, **kwargs)

    def read_next_command(self, _data=None):
        def wrapper(data):
            data = (_data+data) if _data else data
            self._header_callback(data)
        self._request = None
        remaining = 24 - (len(_data) if _data else 0)
        self.stream.read_bytes(remaining, wrapper)

    ## Storage commands
    def on_set_command(self, request):
        self.storage.set(request.key, request.body, request.flags, request.exptime)
        self.start_response()
        self.read_next_command()

    def on_add_command(self, request):
        raise NotImplemented

    def on_replace_command(self, request):
        raise NotImplemented

    def on_append_command(self, request):
        raise NotImplemented

    def on_prepend_command(self, request):
        raise NotImplemented

    ## Retrieval commands
    def on_get_command(self, request):
        body, flags = self.storage.get(request.key)
        if body and flags:
            extra_length = 4
            self.start_response(extra_length=extra_length, total_body=len(body)-extra_length)
            self.write(struct.pack("i", flags))
            self.write(body)
        else:
            self.send_error(status=self.KEY_NOT_FOUND)
        self.read_next_command()

    def on_delete_command(self, request):
        raise NotImplemented

    def on_touch_command(self, request):
        raise NotImplemented

    ## other commands
    def on_quit_command(self, request):
        self.finish()

    def on_stats_command(self, request):
        raise NotImplemented

    def on_version_command(self, request):
        raise NotImplemented

#   magic=0x81, opcode=0x00, key_length=0x0000, extra_length=0x00, data_type=0x00, status=0x0000, total_body=0x00000000, opaque=0x00000000, cas=0x0000000000000000):
class MemcacheBinaryCommand(object):
    COMMANDS = {
        0x00: "get",
        0x01: "set",
        0x02: "add",
        0x03: "replace",
        0x04: "delete",
        0x05: "increment",
        0x06: "decrement",
        0x07: "quit",
        0x08: "flush",
#       0x09: "getQ",
#       0x0A: "no-op",
        0x0B: "version",
#       0x0C: "getK",
#       0x0D: "getKQ",
        0x0E: "append",
        0x0F: "prepend",
        0x10: "stat",
#       0x11: "setQ",
#       0x12: "addQ",
#       0x13: "replaceQ",
#       0x14: "deleteQ",
#       0x15: "incrementQ",
#       0x16: "decrementQ",
#       0x17: "quitQ",
#       0x18: "flushQ",
#       0x19: "appendQ",
#       0x1A: "prependQ",
    }

    def __init__(self, magic=0x81, opcode=0x00, key_length=0x0000, extra_length=0x00, data_type=0x00, status=0x0000, total_body=0x00000000, opaque=0x00000000, cas=0x0000000000000000):
        pass

# vim:set ft=python :
