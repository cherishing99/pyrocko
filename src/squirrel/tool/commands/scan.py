# http://pyrocko.org - GPLv3
#
# The Pyrocko Developers, 21st Century
# ---|P------/S----------~Lg----------

from __future__ import absolute_import, print_function

from pyrocko import squirrel as sq
from .. import common


def setup(subparsers):
    p = common.add_parser(
        subparsers, 'scan',
        help='Scan and index files and directories.',
        description='''Scan and index given files and directories.

Read and cache meta-data of all files in formats understood by Squirrel under
selected paths. Subdirectories are recursively traversed and file formats are
auto-detected unless a specific format is forced with the --format option.
Modification times of files already known to Squirrel are checked by default
and re-indexed as needed. To speed up scanning, these checks can be disabled
with the --optimistic option. With this option, only new files are indexed
during scanning and modifications are handled "last minute" (i.e. just before
the actual data (e.g. waveform samples) are requested by the application).

Usually, the contents of files given to Squirrel are made available within the
application through a runtime selection which is discarded again when the
application quits. Getting the cached meta-data into the runtime selection can
be a bottleneck for application startup with large datasets. To speed up
startup of Squirrel-based applications, persistent selections created with the
--persistent option can be used.

After scanning, information about the current data selection is printed.
''')

    p.add_argument(
        'paths',
        nargs='*',
        help='Files and directories with waveforms, metadata and events.')

    p.add_argument(
        '--optimistic', '-o',
        action='store_false',
        dest='check',
        default=True,
        help='Disable checking file modification times.')

    p.add_argument(
        '--persistent', '-p',
        dest='persistent',
        metavar='NAME',
        help='Create/use persistent selection with given NAME. Persistent '
             'selections can be used to speed up startup of Squirrel-based '
             'applications.')

    p.add_argument(
        '--format', '-f',
        dest='format',
        metavar='FORMAT',
        default='detect',
        choices=sq.supported_formats(),
        help='Assume input files are of given FORMAT. Choices: %(choices)s. '
             'Default: %(default)s.')

    p.add_argument(
        '--content', '-c',
        type=common.csvtype(sq.supported_content_kinds()),
        dest='kinds',
        help='Restrict meta-data scanning to given content kinds. '
             'KINDS is a comma-separated list of content kinds, choices: %s. '
             'By default, all content kinds are indexed.'
             % ', '.join(sq.supported_content_kinds()))

    return p


def call(parser, args):
    s = sq.Squirrel(persistent=args.persistent)
    kinds = args.kinds or None
    s.add(args.paths, check=args.check, format=args.format, kinds=kinds)
    print(s)
