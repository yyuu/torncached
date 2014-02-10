#!/usr/bin/env python

from __future__ import unicode_literals
import collections
import os
import sys
import time

class MemcacheStorage(object):
    MEMCACHED_VERSION = "1.4.17"

    def __init__(self):
        self._counter = collections.Counter()
        self._storage = {}
        self._created = time.time()

    def exists(self, key):
        return key in self._storage and not self._storage[key].expired()

    def set(self, key, val, flags=None, exptime=None):
        self._counter["cmd_set"] += 1
        self._storage[key] = MemcacheRecord(val, flags, exptime)
        self._counter["bytes_written"] += len(val)
        return True

    def add(self, key, val, flags=None, exptime=None):
        if self.exists(key):
            self._storage[key] = MemcacheRecord(val, flags, exptime)
            self._counter["bytes_written"] += len(val)
            return True
        else:
            return False

    def replace(self, key, val, flags=None, exptime=None):
        if self.exists(key):
            self._storage[key] = MemcacheRecord(val, flags, exptime)
            self._counter["bytes_written"] += len(val)
            return True
        else:
            return False

    def append(self, key, val, flags=None, exptime=None):
        if self.exists(key):
            self._storage[key].append(val, flags, exptime)
            self._counter["bytes_written"] += len(val)
            return True
        else:
            return False

    def prepend(self, key, val, flags=None, exptime=None):
        if self.exists(key):
            self._storage[key].prepend(val, flags, exptime)
            self._counter["bytes_written"] += len(val)
            return True
        else:
            return False

    def get(self, key):
        self._counter["cmd_get"] += 1
        if self.exists(key):
            record = self._storage[key]
            val, flags = record.body, record.flags
            self._counter["get_hits"] += 1
            self._counter["bytes_read"] += len(val)
            return (val, flags)
        else:
            self._counter["get_misses"] += 1
            return (None, None)

    def delete(self, key):
        if self.exists(key):
            del self._storage[key]
            self._counter["delete_hits"] += 1
            return True
        else:
            self._counter["delete_misses"] += 1
            return False

    def touch(self, key):
        self._counter["cmd_touch"] += 1
        if self.exists(key):
            self._storage[key].touch()
            self._counter["touch_hits"] += 1
            return True
        else:
            self._counter["touch_misses"] += 1
            return False

    def stats(self):
        stats = dict(self._counter)
        stats["pid"] = os.getpid()
        stats["uptime"] = int(time.time() - self._created)
        stats["time"] = int(time.time())
        stats["version"] = self.MEMCACHED_VERSION
        stats["curr_connections"] = 1
        stats["total_connections"] = 1
        stats["threads"] = 1
        stats["bytes"] = sum([ len(x.body) for x in self._storage.values() ])
        stats["curr_items"] = len([ True for key in self._storage.keys() if self.exists(key) ])
        stats["total_items"] = len(self._storage)
        stats["evictions"] = 0
        return stats

    def version(self):
        return self.MEMCACHED_VERSION

class MemcacheRecord(object):
    def __init__(self, body, flags=None, exptime=None):
        self.body = b"" if body is None else body
        self.flags = 0 if flags is None else flags
        self.exptime = 0 if exptime is None else exptime
        self.touch()

    def append(self, body, flags=None, exptime=None):
        if flags is not None:
            self.flags = flags
        if exptime is not None:
            self.exptime = exptime
        self.body = self.body + body

    def prepend(self, body, flags=None, exptime=None):
        if flags is not None:
            self.flags = flags
        if exptime is not None:
            self.exptime = exptime
        self.body = body + self.body

    def touch(self):
        self._created = time.time()

    def expired(self):
        if self.exptime == 0:
            return False
        else:
            if self.exptime < 60*60*24*30:
                return (self._created + self.exptime) < time.time()
            else:
                return self.exptime < time.time()

# vim:set ft=python :
