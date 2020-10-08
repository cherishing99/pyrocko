# http://pyrocko.org - GPLv3
#
# The Pyrocko Developers, 21st Century
# ---|P------/S----------~Lg----------

from __future__ import absolute_import, print_function

import argparse


class PyrockoHelpFormatter(argparse.RawDescriptionHelpFormatter):
    def __init__(self, *args, **kwargs):
        kwargs['width'] = 79
        argparse.RawDescriptionHelpFormatter.__init__(self, *args, **kwargs)


class PyrockoArgumentParser(argparse.ArgumentParser):

    def __init__(self, *args, **kwargs):

        kwargs['formatter_class'] = PyrockoHelpFormatter

        argparse.ArgumentParser.__init__(self, *args, **kwargs)

        if hasattr(self, '_action_groups'):
            for group in self._action_groups:
                if group.title == 'positional arguments':
                    group.title = 'Positional arguments'

                elif group.title == 'optional arguments':
                    group.title = 'Optional arguments'


def csvtype(choices):
    def splitarg(arg):
        values = arg.split(',')
        for value in values:
            if value not in choices:
                raise argparse.ArgumentTypeError(
                    'Invalid choice: {!r} (choose from {})'
                    .format(value, ', '.join(map(repr, choices))))
        return values
    return splitarg


def add_parser(subparsers, *args, **kwargs):
    kwargs['add_help'] = False
    p = subparsers.add_parser(*args, **kwargs)
    p.add_argument(
        '--help', '-h',
        action='store_true',
        help='Show this help message and exit.')
    return p
