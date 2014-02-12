#!/usr/bin/env python

import tornado.options

def define_options():
    tornado.options.define("autoreload", default=True, type=bool, help="enable autoreload.")
    tornado.options.define("slowdown", default=0.0, type=float, help="response interval.")
    tornado.options.define("port", default=11211, type=int, help="memcached port.")
    tornado.options.define("extra_stats", default=True, type=bool, help="extra command statistics.")

# vim:set ft=python :
