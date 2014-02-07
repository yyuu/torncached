# torncached - a pseudo memcached on tornado

## Overview

An implementation of memcached compatible server, based on Tornado IOLoop.

This is an *EXPRIMENTAL* implementation and not intended to be used in any production environment.

## Requirements

* [tornado](https://github.com/facebook/tornado)

## Usage

Run `torncached` module,

    $ python -m torncached

Or, install and then run,

    $ python setup.py install
    $ torncached --port=11211

## License

MIT

## Author

* Copyright (C) 2014 Yamashita, Yuu <<yamashita@geishatokyo.com>>
* Copyright (C) 2014 Geisha Tokyo Entertainment Inc.
