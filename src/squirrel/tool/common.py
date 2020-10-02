# http://pyrocko.org - GPLv3
#
# The Pyrocko Developers, 21st Century
# ---|P------/S----------~Lg----------

from __future__ import absolute_import, print_function

import argparse


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
