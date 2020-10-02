# http://pyrocko.org - GPLv3
#
# The Pyrocko Developers, 21st Century
# ---|P------/S----------~Lg----------

from __future__ import absolute_import, print_function

from pyrocko import squirrel as sq

def setup(subparsers):
    p = subparsers.add_parser(
        'info',
        help='show environment information')

    return p


def call(parser, args):
    print(sq.get_environment())
    s = sq.Squirrel()
    print(s.get_database().get_stats())
