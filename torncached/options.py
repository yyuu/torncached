#!/usr/bin/env python

import tornado.options

def define_options():
    tornado.options.define("autoreload", default=True, type=bool, help="enable autoreload.")
    tornado.options.define("port", default=11211, type=int, help="memcached port")

# vim:set ft=python :
