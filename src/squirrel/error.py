# http://pyrocko.org - GPLv3
#
# The * Pyrocko Developers, 21st Century
# ---|P------/S----------~Lg----------

from __future__ import absolute_import, print_function


class SquirrelError(Exception):
    pass


class NotAvailable(SquirrelError):
    pass


__all__ = [
    'SquirrelError',
    'NotAvailable']
