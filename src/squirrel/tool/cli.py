# http://pyrocko.org - GPLv3
#
# The Pyrocko Developers, 21st Century
# ---|P------/S----------~Lg----------

import sys

from pyrocko import util

from . import common
from .commands import command_modules
from pyrocko import squirrel as sq


g_program_name = 'squirrel'


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = common.PyrockoArgumentParser(
        prog=g_program_name,
        add_help=False,
        description='''
Pyrocko Squirrel - Prompt seismological data access with a fluffy tail.

This utility program provides a command line front-end to Pyrocko's new data
access infrastructure "Squirrel". This infrastructure offers meta-data caching,
blazingly fast data lookup for huge datasets and transparent online data access
to application building on it.

In most cases, the Squirrel can operate discretely under the hood, without
requiring any human interaction or awareness. However, with this tool, some
aspects can be configured for the benefit of greater performance or
convenience, including:

* using a separate (local) environment for a specific project.
* using named selections to speed up access to very large datasets.
* pre-scanning/indexing files.
''')

    parser.add_argument(
        '--help', '-h',
        action='store_true',
        help='Show this help message and exit.')

    parser.add_argument(
        '--loglevel', '-l',
        choices=['critical', 'error', 'warning', 'info', 'debug'],
        default='info',
        help='Set logger level. Default: %(default)s')

    subparsers = parser.add_subparsers(
        title='subcommands')

    for mod in command_modules:
        subparser = mod.setup(subparsers)
        subparser.set_defaults(target=mod.call, subparser=subparser)

    args = parser.parse_args(args)
    subparser = args.__dict__.pop('subparser', None)
    if args.help:
        (subparser or parser).print_help()
        sys.exit(0)

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
