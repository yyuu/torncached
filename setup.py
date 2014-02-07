#!/usr/bin/env python

from __future__ import with_statement
import contextlib
import logging
import re

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

install_requires = []
with contextlib.closing(open("requirements.txt")) as fp:
    for s in fp:
        package = re.sub(r'#.*$', '', s.strip())
        if 0 < len(package):
            install_requires.append(package)

setup(
    name="torncached",
    version="0.0.1-dev",
    description="a pseudo memcached",
    author="Yamashita, Yuu",
    author_email="yamashita@geishatokyo.com",
    url="http://www.geishatokyo.com/",
    install_requires=install_requires,
    packages=["torncached"],
    entry_points={
        "console_scripts": [
            "torncached=torncached.server:main",
        ],
    },
)

# vim:set ft=python :
