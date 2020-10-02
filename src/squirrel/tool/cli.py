# http://pyrocko.org - GPLv3
#
# The Pyrocko Developers, 21st Century
# ---|P------/S----------~Lg----------

import sys
import argparse

from pyrocko import util

from .commands import command_modules
from pyrocko import squirrel as sq


g_program_name = 'squirrel'


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(prog=g_program_name)

    parser.add_argument(
        '-l', '--loglevel',
        choices=['critical', 'error', 'warning', 'info', 'debug'],
        default='info',
        help='set logger level. Default: %(default)s.')

    subparsers = parser.add_subparsers(
        title='subcommands')

    for mod in command_modules:
        subparser = mod.setup(subparsers)
        subparser.set_defaults(target=mod.call)

    args = parser.parse_args(args)

    loglevel = args.__dict__.pop('loglevel')
    util.setup_logging(g_program_name, loglevel)

    target = args.__dict__.pop('target', None)
    if target:
        try:
            target(parser, args)
        except sq.SquirrelError as e:
            sys.exit(str(e))


__all__ = [
    'main',
]
