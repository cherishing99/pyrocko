# http://pyrocko.org - GPLv3
#
# The Pyrocko Developers, 21st Century
# ---|P------/S----------~Lg----------

from __future__ import absolute_import, print_function

from pyrocko import squirrel as sq

def setup(subparsers):
    p = subparsers.add_parser(
        'init',
        help='create local environment')

    return p


def call(parser, args):
    sq.init_environment()
