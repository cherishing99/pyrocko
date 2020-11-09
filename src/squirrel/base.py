# http://pyrocko.org - GPLv3
#
# The * Pyrocko Developers, 21st Century
# ---|P------/S----------~Lg----------

from __future__ import absolute_import, print_function

import sys
import os
import re
import math
import threading
import sqlite3
import logging
from collections import defaultdict

from pyrocko.io_common import FileLoadError
from pyrocko.guts import Object, Int, List, Tuple, String, Timestamp, Dict
from pyrocko import util

from . import model, io, cache

from .model import to_kind_id, to_kind, separator, WaveformOrder
from .client import fdsn, catalog
from . import client, environment, error, pile

logger = logging.getLogger('pyrocko.squirrel.base')

guts_prefix = 'pf'

# TODO remove debugging stuff
# from matplotlib import pyplot as plt
#
# class SFig(object):
#     def __init__(self):
#         self.line = 0
#
#     def draw(self, eles, color='black'):
#         if not isinstance(eles, list):
#             eles = [eles]
#
#         for ele in eles:
#             plt.plot(
#                 [ele.tmin, ele.tmax],
#                 [self.line, self.line], color=color, alpha=0.5)
#             plt.plot(
#                 [ele.tmin, ele.tmax],
#                 [self.line, self.line], 'o', color=color, alpha=0.5, ms=2)
#
#     def draw_span(self, tmin, tmax, color='orange'):
#         plt.plot(
#             [tmin, tmax],
#             [self.line, self.line], color=color)
#
#     def next(self):
#         self.line += 1
#
#     def show(self):
#         plt.show()
#
#     def set_tspan(self, tmin, tmax):
#         plt.xlim(tmin, tmax)
#
#
# f = SFig()


def lpick(condition, seq):
    ft = [], []
    for ele in seq:
        ft[int(bool(condition(ele)))].append(ele)

    return ft


def execute_get1(connection, sql, args):
    return list(connection.execute(sql, args))[0]


g_icount = 0
g_lock = threading.Lock()


def make_unique_name():
    with g_lock:
        global g_icount
        name = '%i_%i' % (os.getpid(), g_icount)
        g_icount += 1

    return name


def codes_fill(n, codes):
    return codes[:n] + ('*',) * (n-len(codes))


kind_to_ncodes = {
    'station': 4,
    'channel': 5,
    'waveform': 6,
    'waveform_promise': 6}


def codes_patterns_for_kind(kind, codes):
    cfill = codes_fill(kind_to_ncodes[kind], codes)

    if kind == 'station':
        cfill2 = list(cfill)
        cfill2[3] = '[*]'
        return cfill, cfill2

    return (cfill,)


def group_channels(channels):
    groups = defaultdict(list)
    for channel in channels:
        codes = channel.codes
        gcodes = codes[:-1] + (codes[-1][:-1],)
        groups[gcodes].append(channel)

    return groups


def pyrocko_station_from_channel_group(group, extra_args):
    list_of_args = [channel._get_pyrocko_station_args() for channel in group]
    args = util.consistency_merge(list_of_args + extra_args)
    from pyrocko import model as pmodel
    return pmodel.Station(
        network=args[0],
        station=args[1],
        location=args[2],
        lat=args[3],
        lon=args[4],
        elevation=args[5],
        depth=args[6],
        channels=[ch.get_pyrocko_channel() for ch in group])


def blocks(tmin, tmax, deltat, nsamples_block=100000):
    tblock = deltat * nsamples_block
    iblock_min = int(math.floor(tmin / tblock))
    iblock_max = int(math.ceil(tmax / tblock))
    for iblock in range(iblock_min, iblock_max):
        yield iblock * tblock, (iblock+1) * tblock


def gaps(avail, tmin, tmax):
    assert tmin < tmax

    # f.next()
    # f.draw_span(tmin, tmax, 'purple')
    # f.next()

    data = [(tmax, 1), (tmin, -1)]
    for (tmin_a, tmax_a) in avail:
        # f.draw_span(tmin_a, tmax_a, 'cyan')
        assert tmin_a < tmax_a
        data.append((tmin_a, 1))
        data.append((tmax_a, -1))

    # f.next()
    data.sort()
    s = 1
    gaps = []
    tmin_g = None
    for t, x in data:
        if s == 1 and x == -1:
            tmin_g = t
        elif s == 0 and x == 1 and tmin_g is not None:
            tmax_g = t
            if tmin_g != tmax_g:
                gaps.append((tmin_g, tmax_g))

        s += x

    # for tmin, tmax in gaps:
    #     f.draw_span(tmin, tmax, 'pink')
    #
    # f.next()
    return gaps


def order_key(order):
    return (order.codes, order.tmin, order.tmax)


class Selection(object):

    '''
    Database backed file selection.

    :param database: :py:class:`Database` object or path to database
    :param str persistent: if given a name, create a persistent selection

    By default, a temporary table in the database is created to hold the names
    of the files in the selection. This table is only visible inside the
    application which created it. If a name is given to ``persistent``, a named
    selection is created, which is visible also in other applications using the
    same database. Paths of files can be added to the selection using the
    :py:meth:`add` method.
    '''

    def __init__(self, database, persistent=None):
        self._conn = None

        if not isinstance(database, Database):
            database = get_database(database)

        if persistent is not None:
            assert isinstance(persistent, str)
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', persistent):
                raise error.SquirrelError(
                    'invalid persistent selection name: %s' % persistent)

            self.name = 'psel_' + persistent
        else:
            self.name = 'sel_' + make_unique_name()

        self._persistent = persistent is not None
        self._database = database
        self._conn = self._database.get_connection()
        self._sources = []

        self._names = {
            'db': 'main' if self._persistent else 'temp',
            'file_states': self.name + '_file_states',
            'bulkinsert': self.name + '_bulkinsert'}

        self._conn.execute(self._register_table(self._sql(
            '''
                CREATE TABLE IF NOT EXISTS %(db)s.%(file_states)s (
                    file_id integer PRIMARY KEY,
                    file_state integer,
                    kind_mask integer,
                    format text)
            ''')))

        self._conn.execute(self._sql(
            '''
                CREATE INDEX
                IF NOT EXISTS %(db)s.%(file_states)s_index_file_state
                ON %(file_states)s (file_state)
            '''))

    def __del__(self):
        if hasattr(self, '_conn') and self._conn:
            if not self._persistent:
                self._delete()
            else:
                self._conn.commit()

    def _register_table(self, s):
        return self._database._register_table(s)

    def _sql(self, s):
        return s % self._names

    def get_database(self):
        '''
        Get the database to which this selection belongs.

        :returns: :py:class:`Database` object
        '''
        return self._database

    def _delete(self):
        '''
        Destroy the tables assoctiated with this selection.
        '''
        self._conn.execute(self._sql(
            'DROP TABLE %(db)s.%(file_states)s'))

        self._conn.commit()

    def silent_touch(self, file_path):
        '''
        Update modification time of file without initiating reindexing.

        Useful to prolong validity period of data with expiration date.
        '''

        # sql = 'BEGIN TRANSACTION'
        # self._conn.execute(sql)
        try:

            sql = 'SELECT format, size FROM files WHERE path = ?'
            fmt, size = execute_get1(self._conn, sql, (file_path,))

            mod = io.get_backend(fmt)
            mod.touch(file_path)
            file_stats = mod.get_stats(file_path)

            if file_stats[1] != size:
                raise FileLoadError(
                    'Silent update for file "%s" failed: size has changed.'
                    % file_path)

            sql = '''
                UPDATE files
                SET mtime = ?
                WHERE path = ?
            '''
            self._conn.execute(sql, (file_stats[0], file_path))
            self._conn.commit()

        except FileLoadError:
            # self._conn.execute('ROLLBACK')
            raise

    def add(
            self,
            file_paths,
            kind_mask=model.g_kind_mask_all,
            format='detect'):

        '''
        Add files to the selection.

        :param file_paths: Paths to files to be added to the selection.
        :type file_paths: iterator yielding ``str`` objects
        '''

        if isinstance(file_paths, str):
            file_paths = [file_paths]

        file_paths = util.short_to_list(100, file_paths)

        try:
            if len(file_paths) < 100:
                # short non-iterator file_paths: can do without temp table

                self._conn.executemany(
                    '''
                        INSERT OR IGNORE INTO files
                        VALUES (NULL, ?, NULL, NULL, NULL)
                    ''', ((x,) for x in file_paths))

                self._conn.executemany(self._sql(
                    '''
                        DELETE FROM %(db)s.%(file_states)s
                        WHERE file_id IN (
                            SELECT files.file_id
                                FROM files
                                WHERE files.path == ? )
                            AND kind_mask != ? OR format != ?
                    '''), (
                        (path, kind_mask, format) for path in file_paths))

                self._conn.executemany(self._sql(
                    '''
                        INSERT OR IGNORE INTO %(db)s.%(file_states)s
                        SELECT files.file_id, 0, ?, ?
                        FROM files
                        WHERE files.path = ?
                    '''), ((kind_mask, format, path) for path in file_paths))

                self._conn.executemany(self._sql(
                    '''
                        UPDATE %(db)s.%(file_states)s
                        SET file_state = 1
                        WHERE file_id IN (
                            SELECT files.file_id
                                FROM files
                                WHERE files.path == ? )
                            AND file_state != 0
                    '''), ((path,) for path in file_paths))

                return

        except TypeError:
            pass

        self._conn.execute(self._sql(
            '''
                CREATE TEMP TABLE temp.%(bulkinsert)s
                (path text)
            '''))

        self._conn.executemany(self._sql(
            'INSERT INTO temp.%(bulkinsert)s VALUES (?)'),
            ((x,) for x in file_paths))

        self._conn.execute(self._sql(
            '''
                INSERT OR IGNORE INTO files
                SELECT NULL, path, NULL, NULL, NULL
                FROM temp.%(bulkinsert)s
            '''))

        self._conn.execute(self._sql(
            '''
                DELETE FROM %(db)s.%(file_states)s
                WHERE file_id IN (
                    SELECT files.file_id
                        FROM temp.%(bulkinsert)s
                        INNER JOIN files
                        ON temp.%(bulkinsert)s.path == files.path)
                    AND kind_mask != ? OR format != ?
            '''), (kind_mask, format))

        self._conn.execute(self._sql(
            '''
                INSERT OR IGNORE INTO %(db)s.%(file_states)s
                SELECT files.file_id, 0, ?, ?
                FROM temp.%(bulkinsert)s
                INNER JOIN files
                ON temp.%(bulkinsert)s.path == files.path
            '''), (kind_mask, format))

        self._conn.execute(self._sql(
            '''
                UPDATE %(db)s.%(file_states)s
                SET file_state = 1
                WHERE file_id IN (
                    SELECT files.file_id
                        FROM temp.%(bulkinsert)s
                        INNER JOIN files
                        ON temp.%(bulkinsert)s.path == files.path)
                    AND file_state != 0
            '''))

        self._conn.execute(self._sql(
            'DROP TABLE temp.%(bulkinsert)s'))

    def remove(self, file_paths):
        '''
        Remove files from the selection.

        :param file_paths: Paths to files to be removed from the selection.
        :type file_paths: ``list`` of ``str``
        '''
        if isinstance(file_paths, str):
            file_paths = [file_paths]

        self._conn.executemany(self._sql(
            '''
                DELETE FROM %(db)s.%(file_states)s
                WHERE %(db)s.%(file_states)s.file_id IN
                    (SELECT files.file_id
                     FROM files
                     WHERE files.path == ?)
            '''), ((path,) for path in file_paths))

    def _set_file_states_known(self):
        '''
        Set file states to "known" (2).
        '''
        self._conn.execute(self._sql(
            '''
                UPDATE %(db)s.%(file_states)s
                SET file_state = 2
                WHERE file_state < 2
            '''))

    def _set_file_states_force_check(self):
        '''
        Set file states to "known" (2).
        '''
        self._conn.execute(self._sql(
            '''
                UPDATE %(db)s.%(file_states)s
                SET file_state = 1
            '''))

    def undig_grouped(self, skip_unchanged=False):
        '''
        Get content inventory of all files in selection.

        :param: skip_unchanged: if ``True`` only inventory of modified files
            is yielded (:py:meth:`_flag_modified` must be called beforehand).

        This generator yields tuples ``((format, path), nuts)`` where ``path``
        is the path to the file, ``format`` is the format assignation or
        ``'detect'`` and ``nuts`` is a list of :py:class:`pyrocko.squirrel.Nut`
        objects representing the contents of the file. '''

        if skip_unchanged:
            where = '''
                WHERE %(db)s.%(file_states)s.file_state == 0
            '''
        else:
            where = ''

        sql = self._sql('''
            SELECT
                %(db)s.%(file_states)s.format,
                files.path,
                files.format,
                files.mtime,
                files.size,
                nuts.file_segment,
                nuts.file_element,
                kind_codes.kind_id,
                kind_codes.codes,
                nuts.tmin_seconds,
                nuts.tmin_offset,
                nuts.tmax_seconds,
                nuts.tmax_offset,
                nuts.deltat
            FROM %(db)s.%(file_states)s
            LEFT OUTER JOIN files
                ON %(db)s.%(file_states)s.file_id = files.file_id
            LEFT OUTER JOIN nuts
                ON files.file_id = nuts.file_id
            LEFT OUTER JOIN kind_codes
                ON nuts.kind_codes_id == kind_codes.kind_codes_id
        ''' + where + '''
            ORDER BY %(db)s.%(file_states)s.file_id
        ''')

        nuts = []
        format_path = None
        for values in self._conn.execute(sql):
            if format_path is not None and values[1] != format_path[1]:
                yield format_path, nuts
                nuts = []

            if values[2] is not None:
                nuts.append(model.Nut(values_nocheck=values[1:]))

            format_path = values[:2]

        if format_path is not None:
            yield format_path, nuts

    def iter_paths(self):
        sql = self._sql('''
            SELECT
                files.path
            FROM %(db)s.%(file_states)s
            INNER JOIN files
            ON files.file_id = %(db)s.%(file_states)s.file_id
            ORDER BY %(db)s.%(file_states)s.file_id
        ''')

        for values in self._conn.execute(sql):
            yield values[0]

    def _flag_modified(self, check=True):
        '''
        Mark files which have been modified.

        :param check: if ``True`` query modification times of known files on
            disk. If ``False``, only flag unknown files.

        Assumes file state is 0 for newly added files, 1 for files added again
        to the selection (forces check), or 2 for all others (no checking is
        done for those).

        Sets file state to 0 for unknown or modified files, 2 for known and not
        modified files.
        '''

        sql = self._sql('''
            UPDATE %(db)s.%(file_states)s
            SET file_state = 0
            WHERE (
                SELECT mtime
                FROM files
                WHERE files.file_id == %(db)s.%(file_states)s.file_id) IS NULL
                AND file_state == 1
        ''')

        self._conn.execute(sql)

        if not check:

            sql = self._sql('''
                UPDATE %(db)s.%(file_states)s
                SET file_state = 2
                WHERE file_state == 1
            ''')

            self._conn.execute(sql)

            return

        def iter_file_states():
            sql = self._sql('''
                SELECT
                    files.file_id,
                    files.path,
                    files.format,
                    files.mtime,
                    files.size
                FROM %(db)s.%(file_states)s
                INNER JOIN files
                    ON %(db)s.%(file_states)s.file_id == files.file_id
                WHERE %(db)s.%(file_states)s.file_state == 1
                ORDER BY %(db)s.%(file_states)s.file_id
            ''')

            for (file_id, path, fmt, mtime_db,
                    size_db) in self._conn.execute(sql):

                try:
                    mod = io.get_backend(fmt)
                    file_stats = mod.get_stats(path)

                except FileLoadError:
                    yield 0, file_id
                    continue
                except io.UnknownFormat:
                    continue

                if (mtime_db, size_db) != file_stats:
                    yield 0, file_id
                else:
                    yield 2, file_id

        # could better use callback function here...

        sql = self._sql('''
            UPDATE %(db)s.%(file_states)s
            SET file_state = ?
            WHERE file_id = ?
        ''')

        self._conn.executemany(sql, iter_file_states())

    def get_paths(self):
        return list(self.iter_paths())


class SquirrelStats(Object):
    '''
    Container to hold statistics about contents available through a squirrel.
    '''

    nfiles = Int.T(
        help='number of files in selection')
    nnuts = Int.T(
        help='number of index nuts in selection')
    codes = List.T(
        Tuple.T(content_t=String.T()),
        help='available code sequences in selection, e.g. '
             '(agency, network, station, location) for stations nuts.')
    kinds = List.T(
        String.T(),
        help='available content types in selection')
    total_size = Int.T(
        help='aggregated file size of files is selection')
    counts = Dict.T(
        String.T(), Dict.T(Tuple.T(content_t=String.T()), Int.T()),
        help='breakdown of how many nuts of any content type and code '
             'sequence are available in selection, ``counts[kind][codes]``')
    tmin = Timestamp.T(
        optional=True,
        help='earliest start time of all nuts in selection')
    tmax = Timestamp.T(
        optional=True,
        help='latest end time of all nuts in selection')

    def __str__(self):
        kind_counts = dict(
            (kind, sum(self.counts[kind].values())) for kind in self.kinds)

        codes = ['.'.join(x) for x in self.codes]

        if len(codes) > 20:
            codes = codes[:10] \
                + ['[%i more]' % (len(codes) - 20)] \
                + codes[-10:]

        scodes = '\n' + util.ewrap(codes, indent='  ') if codes else '<none>'
        stmin = util.tts(self.tmin) if self.tmin is not None else '<none>'
        stmax = util.tts(self.tmax) if self.tmax is not None else '<none>'

        s = '''
available codes:               %s
number of files:               %i
total size of known files:     %s
number of index nuts:          %i
available nut kinds:           %s
time span of indexed contents: %s - %s''' % (
            scodes,
            self.nfiles,
            util.human_bytesize(self.total_size),
            self.nnuts,
            ', '.join('%s: %i' % (
                kind, kind_counts[kind]) for kind in sorted(self.kinds)),
            stmin, stmax)

        return s


class Squirrel(Selection):
    '''
    Prompt, lazy, indexing, caching, dynamic seismological dataset access.

    :param database: :py:class:`Database` object or path to database
    :param str persistent: if given a name, create a persistent selection

    By default, temporary tables are created in the attached database to hold
    the names of the files in the selection as well as various indices and
    counters. These tables are only visible inside the application which
    created it. If a name is given to ``persistent``, a named selection is
    created, which is visible also in other applications using the same
    database. Paths of files can be added to the selection using the
    :py:meth:`add` method.
    '''

    def __init__(
            self, env=None, database=None, cache_path=None, persistent=None):

        if not isinstance(env, environment.SquirrelEnvironment):
            env = environment.get_environment(env)

        if database is None:
            database = env.database_path

        if cache_path is None:
            cache_path = env.cache_path

        if persistent is None:
            persistent = env.persistent

        Selection.__init__(self, database=database, persistent=persistent)
        c = self._conn
        self._content_caches = {
            'waveform': cache.ContentCache(),
            'default': cache.ContentCache()}

        self._cache_path = cache_path

        self._pile = None

        self._names.update({
            'nuts': self.name + '_nuts',
            'kind_codes_count': self.name + '_kind_codes_count'})

        c.execute(self._register_table(self._sql(
            '''
                CREATE TABLE IF NOT EXISTS %(db)s.%(nuts)s (
                    nut_id integer PRIMARY KEY,
                    file_id integer,
                    file_segment integer,
                    file_element integer,
                    kind_id integer,
                    kind_codes_id integer,
                    tmin_seconds integer,
                    tmin_offset float,
                    tmax_seconds integer,
                    tmax_offset float,
                    deltat float,
                    kscale integer)
            ''')))

        c.execute(self._sql(
            '''
                CREATE UNIQUE INDEX IF NOT EXISTS %(db)s.%(nuts)s_file_element
                    ON %(nuts)s (file_id, file_segment, file_element)
            '''))

        c.execute(self._register_table(self._sql(
            '''
                CREATE TABLE IF NOT EXISTS %(db)s.%(kind_codes_count)s (
                    kind_codes_id integer PRIMARY KEY,
                    count integer)
            ''')))

        c.execute(self._sql(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_file_id
                ON %(nuts)s (file_id)
            '''))

        c.execute(self._sql(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_tmin_seconds
                ON %(nuts)s (tmin_seconds)
            '''))

        c.execute(self._sql(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_tmax_seconds
                ON %(nuts)s (tmax_seconds)
            '''))

        c.execute(self._sql(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_kscale
                ON %(nuts)s (kind_id, kscale, tmin_seconds)
            '''))

        c.execute(self._sql(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_delete_nuts
                BEFORE DELETE ON main.files FOR EACH ROW
                BEGIN
                  DELETE FROM %(nuts)s WHERE file_id == old.file_id;
                END
            '''))

        c.execute(self._sql(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_delete_nuts2
                BEFORE UPDATE ON main.files FOR EACH ROW
                BEGIN
                  DELETE FROM %(nuts)s WHERE file_id == old.file_id;
                END
            '''))

        c.execute(self._sql(
            '''
                CREATE TRIGGER IF NOT EXISTS
                    %(db)s.%(file_states)s_delete_files
                BEFORE DELETE ON %(db)s.%(file_states)s FOR EACH ROW
                BEGIN
                    DELETE FROM %(nuts)s WHERE file_id == old.file_id;
                END
            '''))

        c.execute(self._sql(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_inc_kind_codes
                BEFORE INSERT ON %(nuts)s FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO %(kind_codes_count)s VALUES
                    (new.kind_codes_id, 0);
                    UPDATE %(kind_codes_count)s
                    SET count = count + 1
                    WHERE new.kind_codes_id
                        == %(kind_codes_count)s.kind_codes_id;
                END
            '''))

        c.execute(self._sql(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_dec_kind_codes
                BEFORE DELETE ON %(nuts)s FOR EACH ROW
                BEGIN
                    UPDATE %(kind_codes_count)s
                    SET count = count - 1
                    WHERE old.kind_codes_id
                        == %(kind_codes_count)s.kind_codes_id;
                END
            '''))

    def _delete(self):
        '''Delete database tables associated with this squirrel.'''

        for s in '''
                DROP TRIGGER %(db)s.%(nuts)s_delete_nuts;
                DROP TRIGGER %(db)s.%(nuts)s_delete_nuts2;
                DROP TRIGGER %(db)s.%(file_states)s_delete_files;
                DROP TRIGGER %(db)s.%(nuts)s_inc_kind_codes;
                DROP TRIGGER %(db)s.%(nuts)s_dec_kind_codes;
                DROP TABLE %(db)s.%(nuts)s;
                DROP TABLE %(db)s.%(kind_codes_count)s;
                '''.strip().splitlines():

            self._conn.execute(self._sql(s))

        Selection._delete(self)

        self._conn.commit()

    def print_tables(self, table_names=None, stream=None):
        if stream is None:
            stream = sys.stdout

        if isinstance(table_names, str):
            table_names = [table_names]

        if table_names is None:
            table_names = [
                'selection_file_states',
                'selection_nuts',
                'selection_kind_codes_count',
                'files', 'nuts', 'kind_codes', 'kind_codes_count']

        m = {
            'selection_file_states': '%(db)s.%(file_states)s',
            'selection_nuts': '%(db)s.%(nuts)s',
            'selection_kind_codes_count': '%(db)s.%(kind_codes_count)s',
            'files': 'files',
            'nuts': 'nuts',
            'kind_codes': 'kind_codes',
            'kind_codes_count': 'kind_codes_count'}

        for table_name in table_names:
            self._database.print_table(
                m[table_name] % self._names, stream=stream)

    def iter_expand_dirs(self, file_paths):
        for path in file_paths:
            if os.path.isdir(path):
                dpaths = sorted(util.select_files([path], show_progress=False))
                for dpath in dpaths:
                    yield dpath

            else:
                yield path

    def add(self, file_paths, kinds=None, format='detect', check=True):
        '''
        Add files to the selection.

        :param file_paths: iterator yielding paths to files or directories to
            be added to the selection. Recurses into directories given. If
            given a `str`, it is treated as a single path to be added.
        :param kinds: if given, allowed content types to be made available
            through the squirrel selection.
        :type kinds: ``list`` of ``str``
        :param format: file format identifier or ``'detect'`` for
            auto-detection
        :type format: str

        Complexity: O(log N)
        '''
        if isinstance(kinds, str):
            kinds = (kinds,)

        if isinstance(file_paths, str):
            file_paths = [file_paths]

        kind_mask = model.to_kind_mask(kinds)

        Selection.add(
            self, self.iter_expand_dirs(file_paths), kind_mask, format)

        self._load(check)
        self._update_nuts()

    def reload(self):
        self._set_file_states_force_check()
        self._load(check=True)
        self._update_nuts()

    def add_virtual(self, nuts, virtual_file_paths=None):
        '''
        Add content which is not backed by files.

        Stores to the main database and the selection.

        If ``virtual_file_paths`` are given, this prevents creating a temp list
        of the nuts while aggregating the file paths for the selection.
        '''

        if isinstance(virtual_file_paths, str):
            virtual_file_paths = [virtual_file_paths]

        if virtual_file_paths is None:
            nuts_add = []
            virtual_file_paths = set()
            for nut in nuts:
                virtual_file_paths.add(nut.file_path)
                nuts_add.append(nut)
        else:
            nuts_add = nuts

        Selection.add(self, virtual_file_paths)
        self.get_database().dig(nuts_add)
        self._update_nuts()

    def _load(self, check):
        for _ in io.iload(
                self,
                content=[],
                skip_unchanged=True,
                check=check):
            pass

    def _update_nuts(self):
        self._conn.execute(self._sql(
            '''
                INSERT INTO %(db)s.%(nuts)s
                SELECT NULL,
                    nuts.file_id, nuts.file_segment, nuts.file_element,
                    nuts.kind_id, nuts.kind_codes_id,
                    nuts.tmin_seconds, nuts.tmin_offset,
                    nuts.tmax_seconds, nuts.tmax_offset,
                    nuts.deltat, nuts.kscale
                FROM %(db)s.%(file_states)s
                INNER JOIN nuts
                    ON %(db)s.%(file_states)s.file_id == nuts.file_id
                INNER JOIN kind_codes
                    ON nuts.kind_codes_id ==
                       kind_codes.kind_codes_id
                WHERE %(db)s.%(file_states)s.file_state != 2
                    AND (((1 << kind_codes.kind_id)
                        & %(db)s.%(file_states)s.kind_mask) != 0)
            '''))

        self._set_file_states_known()

    def add_source(self, source):
        '''
        Add remote resource.
        '''

        self._sources.append(source)
        source.setup(self)

    def add_fdsn(self, *args, **kwargs):
        '''
        Add FDSN site for transparent remote data access.
        '''

        self.add_source(fdsn.FDSNSource(*args, **kwargs))

    def add_catalog(self, *args, **kwargs):
        '''
        Add online catalog for transparent event data access.
        '''

        self.add_source(catalog.CatalogSource(*args, **kwargs))

    def get_selection_args(
            self, obj=None, tmin=None, tmax=None, time=None, codes=None):

        if time is not None:
            tmin = time
            tmax = time

        if obj is not None:
            tmin = tmin if tmin is not None else obj.tmin
            tmax = tmax if tmax is not None else obj.tmax
            codes = codes if codes is not None else obj.codes

        if isinstance(codes, str):
            codes = tuple(codes.split('.'))

        return tmin, tmax, codes

    def iter_nuts(self, kind=None, tmin=None, tmax=None, codes=None):
        '''
        Iterate content matching given contraints.

        :param kind: ``str``, content kind to extract or sequence of such
        :param tmin: timestamp, start time of interval
        :param tmax: timestamp, end time of interval
        :param codes: tuple of str, pattern of content codes to be matched

        Complexity: O(log N) for the time selection part due to heavy use of
        database indexes.

        Yields :py:class:`pyrocko.squirrel.Nut` objects representing the
        intersecting content.

        Query time span is treated as a half-open interval ``[tmin, tmax)``.
        However, if ``tmin`` equals ``tmax``, the edge logics are modified to
        closed-interval so that content intersecting with the time instant ``t
        = tmin = tmax`` is returned (otherwise nothing would be returned as
        ``[t, t)`` never matches anything).

        Content time spans are also treated as half open intervals, e.g.
        content span ``[0, 1)`` is matched by query span ``[0, 1)`` but not by
        ``[-1, 0)`` or ``[1, 2)``. Also here, logics are modified to
        closed-interval when the content time span is an empty interval, i.e.
        to indicate a time instant. E.g. time instant 0 is matched by
        ``[0, 1)`` but not by ``[-1, 0)`` or ``[1, 2)``.
        '''

        if not isinstance(kind, str):
            if kind is None:
                kind = model.g_content_kinds
            for kind_ in kind:
                for nut in self.iter_nuts(kind_, tmin, tmax, codes):
                    yield nut

            return

        extra_cond = []
        tmin_cond = []
        args = []

        if tmin is not None or tmax is not None:
            assert kind is not None
            if tmin is None:
                tmin = self.get_time_span()[0]
            if tmax is None:
                tmax = self.get_time_span()[1]

            tmin_seconds, tmin_offset = model.tsplit(tmin)
            tmax_seconds, tmax_offset = model.tsplit(tmax)

            tscale_edges = model.tscale_edges

            for kscale in range(tscale_edges.size + 1):
                if kscale != tscale_edges.size:
                    tscale = int(tscale_edges[kscale])
                    tmin_cond.append('''
                        (%(db)s.%(nuts)s.kind_id = ?
                            AND %(db)s.%(nuts)s.kscale == ?
                            AND %(db)s.%(nuts)s.tmin_seconds BETWEEN ? AND ?)
                    ''')
                    args.extend(
                        (to_kind_id(kind), kscale,
                         tmin_seconds - tscale - 1, tmax_seconds + 1))

                else:
                    tmin_cond.append('''
                        (%(db)s.%(nuts)s.kind_id == ?
                            AND %(db)s.%(nuts)s.kscale == ?
                            AND %(db)s.%(nuts)s.tmin_seconds <= ?)
                    ''')

                    args.extend(
                        (to_kind_id(kind), kscale, tmax_seconds + 1))

            extra_cond.append('%(db)s.%(nuts)s.tmax_seconds >= ?')
            args.append(tmin_seconds)

        elif kind is not None:
            extra_cond.append('%(db)s.%(nuts)s.kind_id == ?')
            args.append(to_kind_id(kind))

        if codes is not None:
            pats = codes_patterns_for_kind(kind, codes)
            extra_cond.append(
                ' ( %s ) ' % ' OR '.join(
                    ('kind_codes.codes GLOB ?',) * len(pats)))
            args.extend(separator.join(pat) for pat in pats)

        sql = ('''
            SELECT
                files.path,
                files.format,
                files.mtime,
                files.size,
                %(db)s.%(nuts)s.file_segment,
                %(db)s.%(nuts)s.file_element,
                kind_codes.kind_id,
                kind_codes.codes,
                %(db)s.%(nuts)s.tmin_seconds,
                %(db)s.%(nuts)s.tmin_offset,
                %(db)s.%(nuts)s.tmax_seconds,
                %(db)s.%(nuts)s.tmax_offset,
                %(db)s.%(nuts)s.deltat
            FROM files
            INNER JOIN %(db)s.%(nuts)s
                ON files.file_id == %(db)s.%(nuts)s.file_id
            INNER JOIN kind_codes
                ON %(db)s.%(nuts)s.kind_codes_id == kind_codes.kind_codes_id
            ''')

        cond = []
        if tmin_cond:
            cond.append(' ( ' + ' OR '.join(tmin_cond) + ' ) ')

        cond.extend(extra_cond)

        if cond:
            sql += ''' WHERE ''' + ' AND '.join(cond)

        sql = self._sql(sql)
        if tmin is None and tmax is None:
            for row in self._conn.execute(sql, args):
                nut = model.Nut(values_nocheck=row)
                yield nut
        else:
            if tmin is None:
                tmin = self.get_time_span()[0] - 1.
            if tmax is None:
                tmax = self.get_time_span()[1] + 1.

            if tmin == tmax:
                for row in self._conn.execute(sql, args):
                    nut = model.Nut(values_nocheck=row)
                    if (nut.tmin <= tmin < nut.tmax) \
                            or (nut.tmin == nut.tmax and tmin == nut.tmin):

                        yield nut
            else:
                for row in self._conn.execute(sql, args):
                    nut = model.Nut(values_nocheck=row)
                    if (tmin < nut.tmax and nut.tmin < tmax) \
                            or (nut.tmin == nut.tmax
                                and tmin <= nut.tmin < tmax):

                        yield nut

    def get_nuts(self, *args, **kwargs):
        return list(self.iter_nuts(*args, **kwargs))

    def _iter_nuts_naiv(self, kind, tmin=None, tmax=None):
        tmin_seconds, tmin_offset = model.tsplit(tmin)
        tmax_seconds, tmax_offset = model.tsplit(tmax)

        tmin_avail, tmax_avail = self.get_time_span()
        if tmin is None:
            tmin = tmin_avail
            tmax = tmax_avail

        sql = self._sql('''
            SELECT
                files.path,
                files.format,
                files.mtime,
                files.size,
                %(db)s.%(nuts)s.file_segment,
                %(db)s.%(nuts)s.file_element,
                kind_codes.kind_id,
                kind_codes.codes,
                %(db)s.%(nuts)s.tmin_seconds,
                %(db)s.%(nuts)s.tmin_offset,
                %(db)s.%(nuts)s.tmax_seconds,
                %(db)s.%(nuts)s.tmax_offset,
                %(db)s.%(nuts)s.deltat
            FROM files
            INNER JOIN %(db)s.%(nuts)s
                ON files.file_id == %(db)s.%(nuts)s.file_id
            INNER JOIN kind_codes
                ON %(db)s.%(nuts)s.kind_codes_id == kind_codes.kind_codes_id
            WHERE %(db)s.%(nuts)s.kind_id = ?
                AND %(db)s.%(nuts)s.tmax_seconds >= ?
                AND %(db)s.%(nuts)s.tmin_seconds <= ?
        ''')

        for row in self._conn.execute(
                sql, (to_kind_id(kind), tmin_seconds, tmax_seconds+1)):

            nut = model.Nut(values_nocheck=row)
            if nut.tmin < tmax and tmin < nut.tmax:
                yield nut

    def split_nuts(
            self, kind, tmin=None, tmax=None, codes=None, file_path=None):

        tmin_seconds, tmin_offset = model.tsplit(tmin)
        tmax_seconds, tmax_offset = model.tsplit(tmax)

        tscale_edges = model.tscale_edges

        tmin_cond = []
        args = []
        for kscale in range(tscale_edges.size + 1):
            if kscale != tscale_edges.size:
                tscale = int(tscale_edges[kscale])
                tmin_cond.append('''
                    (%(db)s.%(nuts)s.kind_id = ?
                        AND %(db)s.%(nuts)s.kscale == ?
                        AND %(db)s.%(nuts)s.tmin_seconds BETWEEN ? AND ?)
                ''')
                args.extend(
                    (to_kind_id(kind), kscale,
                     tmin_seconds - tscale - 1, tmax_seconds + 1))

            else:
                tmin_cond.append('''
                    (%(db)s.%(nuts)s.kind_id == ?
                        AND %(db)s.%(nuts)s.kscale == ?
                        AND %(db)s.%(nuts)s.tmin_seconds <= ?)
                ''')

                args.extend(
                    (to_kind_id(kind), kscale, tmax_seconds + 1))

        extra_cond = ['%(db)s.%(nuts)s.tmax_seconds >= ?']
        args.append(tmin_seconds)
        if codes is not None:
            pats = codes_patterns_for_kind(kind, codes)
            extra_cond.append(
                ' ( %s ) ' % ' OR '.join(
                    ('kind_codes.codes GLOB ?',) * len(pats)))
            args.extend(separator.join(pat) for pat in pats)

        if file_path is not None:
            extra_cond.append('files.path == ?')
            args.append(file_path)

        sql = self._sql('''
            SELECT
                %(db)s.%(nuts)s.nut_id,
                %(db)s.%(nuts)s.tmin_seconds,
                %(db)s.%(nuts)s.tmin_offset,
                %(db)s.%(nuts)s.tmax_seconds,
                %(db)s.%(nuts)s.tmax_offset,
                %(db)s.%(nuts)s.deltat
            FROM files
            INNER JOIN %(db)s.%(nuts)s
                ON files.file_id == %(db)s.%(nuts)s.file_id
            INNER JOIN kind_codes
                ON %(db)s.%(nuts)s.kind_codes_id == kind_codes.kind_codes_id
            WHERE ( ''' + ' OR '.join(tmin_cond) + ''' )
                AND ''' + ' AND '.join(extra_cond))

        insert = []
        delete = []
        for row in self._conn.execute(sql, args):
            nut_id, nut_tmin_seconds, nut_tmin_offset, \
                nut_tmax_seconds, nut_tmax_offset, nut_deltat = row

            nut_tmin = model.tjoin(
                nut_tmin_seconds, nut_tmin_offset, nut_deltat)
            nut_tmax = model.tjoin(
                nut_tmax_seconds, nut_tmax_offset, nut_deltat)

            if nut_tmin < tmax and tmin < nut_tmax:
                if nut_tmin < tmin:
                    insert.append((
                        nut_tmin_seconds, nut_tmin_offset,
                        tmin_seconds, tmin_offset,
                        model.tscale_to_kscale(
                            tmin_seconds - nut_tmin_seconds),
                        nut_id))

                if tmax < nut_tmax:
                    insert.append((
                        tmax_seconds, tmax_offset,
                        nut_tmax_seconds, nut_tmax_offset,
                        model.tscale_to_kscale(
                            nut_tmax_seconds - tmax_seconds),
                        nut_id))

                delete.append((nut_id,))

        sql_add = '''
            INSERT INTO %(db)s.%(nuts)s (
                    file_id, file_segment, file_element, kind_id,
                    kind_codes_id, deltat, tmin_seconds, tmin_offset,
                    tmax_seconds, tmax_offset, kscale )
                SELECT
                    file_id, file_segment, file_element,
                    kind_id, kind_codes_id, deltat, ?, ?, ?, ?, ?
                FROM %(db)s.%(nuts)s
                WHERE nut_id == ?
        '''
        self._conn.executemany(self._sql(sql_add), insert)

        sql_delete = '''DELETE FROM %(db)s.%(nuts)s WHERE nut_id == ?'''
        self._conn.executemany(self._sql(sql_delete), delete)

    def get_time_span(self):
        '''
        Get time interval over all content in selection.

        Complexity O(1), independent of number of nuts

        :returns: (tmin, tmax)
        '''
        sql = self._sql('''
            SELECT MIN(tmin_seconds + tmin_offset)
            FROM %(db)s.%(nuts)s WHERE
            tmin_seconds == (SELECT MIN(tmin_seconds) FROM %(db)s.%(nuts)s)
        ''')
        tmin = None
        for row in self._conn.execute(sql):
            tmin = row[0]

        sql = self._sql('''
            SELECT MAX(tmax_seconds + tmax_offset)
            FROM %(db)s.%(nuts)s WHERE
            tmax_seconds == (SELECT MAX(tmax_seconds) FROM %(db)s.%(nuts)s)
        ''')
        tmax = None
        for row in self._conn.execute(sql):
            tmax = row[0]

        return tmin, tmax

    def iter_kinds(self, codes=None):
        '''
        Iterate over content types available in selection.

        :param codes: if given, get kinds only for selected codes identifier

        Complexity: O(1), independent of number of nuts
        '''

        return self._database._iter_kinds(
            codes=codes,
            kind_codes_count='%(db)s.%(kind_codes_count)s' % self._names)

    def iter_codes(self, kind=None):
        '''
        Iterate over content identifier code sequences available in selection.

        :param kind: if given, get codes only for a given content type
        :type kind: ``str``

        Complexity: O(1), independent of number of nuts
        '''
        return self._database._iter_codes(
            kind=kind,
            kind_codes_count='%(db)s.%(kind_codes_count)s' % self._names)

    def iter_counts(self, kind=None):
        '''
        Iterate over number of occurrences of any (kind, codes) combination.

        :param kind: if given, get counts only for selected content type

        Yields tuples ``((kind, codes), count)``

        Complexity: O(1), independent of number of nuts
        '''
        return self._database._iter_counts(
            kind=kind,
            kind_codes_count='%(db)s.%(kind_codes_count)s' % self._names)

    def get_kinds(self, codes=None):
        '''
        Get content types available in selection.

        :param codes: if given, get kinds only for selected codes identifier

        Complexity: O(1), independent of number of nuts

        :returns: sorted list of available content types
        '''
        return sorted(list(self.iter_kinds(codes=codes)))

    def get_codes(self, kind=None):
        '''
        Get identifier code sequences available in selection.

        :param kind: if given, get codes only for selected content type

        Complexity: O(1), independent of number of nuts

        :returns: sorted list of available codes as tuples of strings
        '''
        return sorted(list(self.iter_codes(kind=kind)))

    def get_counts(self, kind=None):
        '''
        Get number of occurrences of any (kind, codes) combination.

        :param kind: if given, get codes only for selected content type

        Complexity: O(1), independent of number of nuts

        :returns: ``dict`` with ``counts[kind][codes] or ``counts[codes]``
            if kind is not ``None``
        '''
        d = {}
        for (k, codes), count in self.iter_counts():
            if k not in d:
                d[k] = {}

            d[k][codes] = count

        if kind is not None:
            return d[kind]
        else:
            return d

    def update(self, constraint=None, **kwargs):
        '''
        Update inventory of remote content for a given selection.

        This function triggers all attached remote sources, to check for
        updates in the metadata. The sources will only submit queries when
        their expiration date has passed, or if the selection spans into
        previously unseen times or areas.
        '''

        if constraint is None:
            constraint = client.Constraint(**kwargs)

        for source in self._sources:
            source.update_channel_inventory(self, constraint)
            source.update_event_inventory(self, constraint)

    def update_waveform_inventory(self, constraint=None, **kwargs):

        if constraint is None:
            constraint = client.Constraint(**kwargs)

        for source in self._sources:
            source.update_waveform_inventory(self, constraint)

    def get_nfiles(self):
        '''
        Get number of files in selection.
        '''

        sql = self._sql('''SELECT COUNT(*) FROM %(db)s.%(file_states)s''')
        for row in self._conn.execute(sql):
            return row[0]

    def get_nnuts(self):
        '''
        Get number of nuts in selection.
        '''

        sql = self._sql('''SELECT COUNT(*) FROM %(db)s.%(nuts)s''')
        for row in self._conn.execute(sql):
            return row[0]

    def get_total_size(self):
        '''
        Get aggregated file size available in selection.
        '''

        sql = self._sql('''
            SELECT SUM(files.size) FROM %(db)s.%(file_states)s
            INNER JOIN files
                ON %(db)s.%(file_states)s.file_id = files.file_id
        ''')

        for row in self._conn.execute(sql):
            return row[0] or 0

        return 0

    def get_stats(self):
        '''
        Get statistics on contents available through this selection.
        '''

        tmin, tmax = self.get_time_span()

        return SquirrelStats(
            nfiles=self.get_nfiles(),
            nnuts=self.get_nnuts(),
            kinds=self.get_kinds(),
            codes=self.get_codes(),
            total_size=self.get_total_size(),
            counts=self.get_counts(),
            tmin=tmin,
            tmax=tmax)

    def get_content(self, nut, cache='default', accessor='default'):
        '''
        Get and possibly load full content for a given index entry from file.

        Loads the actual content objects (channel, station, waveform, ...) from
        file. For efficiency sibling content (all stuff in the same file
        segment) will also be loaded as a side effect. The loaded contents are
        cached in the squirrel object.
        '''

        content_cache = self._content_caches[cache]
        if not content_cache.has(nut):
            for nut_loaded in io.iload(
                    nut.file_path,
                    segment=nut.file_segment,
                    format=nut.file_format,
                    database=self._database):

                content_cache.put(nut_loaded)

        try:
            return content_cache.get(nut, accessor)
        except KeyError:
            raise error.NotAvailable(
                'Unable to retrieve content: %s, %s, %s, %s' % nut.key)

    def advance_accessor(self, accessor, cache=None):
        for cache_ in (
                self._content_caches.keys() if cache is None else [cache]):

            self._content_caches[cache_].advance_accessor(accessor)

    def clear_accessor(self, accessor, cache=None):
        for cache_ in (
                self._content_caches.keys() if cache is None else [cache]):

            self._content_caches[cache_].clear_accessor(accessor)

    def check_duplicates(self, nuts):
        d = defaultdict(list)
        for nut in nuts:
            d[nut.codes].append(nut)

        for codes, group in d.items():
            if len(group) > 1:
                logger.warn(
                    'Multiple entries matching codes %s'
                    % '.'.join(codes.split(separator)))

    def get_stations(self, *args, **kwargs):
        args = self.get_selection_args(*args, **kwargs)
        nuts = sorted(
            self.iter_nuts('station', *args), key=lambda nut: nut.dkey)
        self.check_duplicates(nuts)
        return [self.get_content(nut) for nut in nuts]

    def get_channels(self, *args, **kwargs):
        args = self.get_selection_args(*args, **kwargs)
        nuts = sorted(
            self.iter_nuts('channel', *args), key=lambda nut: nut.dkey)
        self.check_duplicates(nuts)
        return [self.get_content(nut) for nut in nuts]

    def get_events(self, *args, **kwargs):
        args = self.get_selection_args(*args, **kwargs)
        nuts = sorted(
            self.iter_nuts('event', *args), key=lambda nut: nut.dkey)
        self.check_duplicates(nuts)
        return [self.get_content(nut) for nut in nuts]

    def _redeem_promises(self, *args):

        tmin, tmax, _ = args
        # f.draw_span(tmin, tmax)
        # f.next()

        waveforms = list(self.iter_nuts('waveform', *args))
        promises = list(self.iter_nuts('waveform_promise', *args))
        # f.draw(waveforms, 'red')
        # f.draw(promises, 'gray')
        #
        # f.next()

        codes_to_avail = defaultdict(list)
        for nut in waveforms:
            codes_to_avail[nut.codes].append((nut.tmin, nut.tmax+nut.deltat))

        def tts(x):
            if isinstance(x, tuple):
                return tuple(tts(e) for e in x)
            elif isinstance(x, list):
                return list(tts(e) for e in x)
            else:
                return util.time_to_str(x)

        orders = []
        for promise in promises:
            waveforms_avail = codes_to_avail[promise.codes]
            for block_tmin, block_tmax in blocks(
                    max(tmin, promise.tmin),
                    min(tmax, promise.tmax),
                    promise.deltat):

                orders.append(
                    WaveformOrder(
                        source_id=promise.file_path,
                        codes=tuple(promise.codes.split(separator)),
                        tmin=block_tmin,
                        tmax=block_tmax,
                        deltat=promise.deltat,
                        gaps=gaps(waveforms_avail, block_tmin, block_tmax)))

        orders_noop, orders = lpick(lambda order: order.gaps, orders)
        # f.draw(orders_noop, 'green')
        # f.draw(orders, 'blue')
        # f.next()

        order_keys_noop = set(order_key(order) for order in orders_noop)
        if len(order_keys_noop) != 0 or len(orders_noop) != 0:
            logger.info(
                'Waveform orders already satisified with cached/local data: '
                '%i (%i)' % (len(order_keys_noop), len(orders_noop)))

        source_ids = []
        sources = {}
        for source in self._sources:
            source_ids.append(source._source_id)
            sources[source._source_id] = source

        source_priority = dict(
            (source_id, i) for (i, source_id) in enumerate(source_ids))

        order_groups = defaultdict(list)
        for order in orders:
            order_groups[order_key(order)].append(order)

        for k, order_group in order_groups.items():
            order_group.sort(
                key=lambda order: source_priority[order.source_id])

        if len(order_groups) != 0 or len(orders) != 0:
            logger.info(
                'Waveform orders standing for download: %i (%i)'
                % (len(order_groups), len(orders)))

        def release_order_group(order):
            del order_groups[order_key(order)]

        def split_promise(order):
            self.split_nuts(
                'waveform_promise',
                order.tmin, order.tmax,
                codes=order.codes,
                file_path=order.source_id)

        def noop(order):
            pass

        def success(order):
            # f.draw(order, 'black')
            release_order_group(order)
            split_promise(order)

        for order in orders_noop:
            split_promise(order)

        while order_groups:
            orders_now = []
            empty = []
            for k, order_group in order_groups.items():
                try:
                    orders_now.append(order_group.pop(0))
                except IndexError:
                    empty.append(k)

            for k in empty:
                del order_groups[k]

            by_source_id = defaultdict(list)
            for order in orders_now:
                by_source_id[order.source_id].append(order)

            # TODO: parallelize this loop
            for source_id in by_source_id:
                sources[source_id].download_waveforms(
                    self, by_source_id[source_id],
                    success=success,
                    error_permanent=split_promise,
                    error_temporary=noop)

        # f.next()

    def get_waveform_nuts(self, *args, **kwargs):
        args = self.get_selection_args(*args, **kwargs)
        self._redeem_promises(*args)
        return sorted(
            self.iter_nuts('waveform', *args), key=lambda nut: nut.dkey)

    def get_waveforms(self, *args, **kwargs):
        nuts = self.get_waveform_nuts(*args, **kwargs)
        # self.check_duplicates(nuts)
        return [self.get_content(nut, 'waveform') for nut in nuts]

    def get_pyrocko_stations(self, *args, **kwargs):
        from pyrocko import model as pmodel

        by_nsl = defaultdict(lambda: (list(), list()))
        for station in self.get_stations(*args, **kwargs):
            sargs = station._get_pyrocko_station_args()
            nsl = sargs[1:4]
            by_nsl[nsl][0].append(sargs)

        for channel in self.get_channels(*args, **kwargs):
            sargs = channel._get_pyrocko_station_args()
            nsl = sargs[1:4]
            sargs_list, channels_list = by_nsl[nsl]
            sargs_list.append(sargs)
            channels_list.append(channel)

        pstations = []
        nsls = list(by_nsl.keys())
        nsls.sort()
        for nsl in nsls:
            sargs_list, channels_list = by_nsl[nsl]
            sargs = util.consistency_merge(sargs_list)

            by_c = defaultdict(list)
            for ch in channels_list:
                by_c[ch.channel].append(ch._get_pyrocko_channel_args())

            chas = list(by_c.keys())
            chas.sort()
            pchannels = []
            for cha in chas:
                list_of_cargs = by_c[cha]
                cargs = util.consistency_merge(list_of_cargs)
                pchannels.append(pmodel.Channel(
                    name=cargs[0],
                    azimuth=cargs[1],
                    dip=cargs[2]))

            pstations.append(pmodel.Station(
                network=sargs[0],
                station=sargs[1],
                location=sargs[2],
                lat=sargs[3],
                lon=sargs[4],
                elevation=sargs[5],
                depth=sargs[6] or 0.0,
                channels=pchannels))

        return pstations

    def get_pyrocko_events(self, *args, **kwargs):
        from pyrocko import model as pmodel  # noqa
        return self.get_events(*args, **kwargs)

    @property
    def pile(self):
        if self._pile is None:
            self._pile = pile.Pile(self)

        return self._pile

    def snuffle(self):
        self.pile.snuffle()

    def gather_codes_keys(self, kind, gather, selector):
        return set(
            gather(codes)
            for codes in self.iter_codes(kind)
            if selector is None or selector(codes))

    def __str__(self):
        return str(self.get_stats())


class DatabaseStats(Object):
    '''
    Container to hold statistics about contents cached in meta-information db.
    '''

    nfiles = Int.T(
        help='number of files in database')
    nnuts = Int.T(
        help='number of index nuts in database')
    codes = List.T(
        Tuple.T(content_t=String.T()),
        help='available code sequences in database, e.g. '
             '(agency, network, station, location) for stations nuts.')
    kinds = List.T(
        String.T(),
        help='available content types in database')
    total_size = Int.T(
        help='aggregated file size of files referenced in database')
    counts = Dict.T(
        String.T(), Dict.T(Tuple.T(content_t=String.T()), Int.T()),
        help='breakdown of how many nuts of any content type and code '
             'sequence are available in database, ``counts[kind][codes]``')

    def __str__(self):
        kind_counts = dict(
            (kind, sum(self.counts[kind].values())) for kind in self.kinds)

        codes = ['.'.join(x) for x in self.codes]

        if len(codes) > 20:
            codes = codes[:10] \
                + ['[%i more]' % (len(codes) - 20)] \
                + codes[-10:]

        scodes = '\n' + util.ewrap(codes, indent='  ') if codes else '<none>'

        s = '''
available codes:               %s
number of files:               %i
total size of known files:     %s
number of index nuts:          %i
available nut kinds:           %s''' % (
            scodes,
            self.nfiles,
            util.human_bytesize(self.total_size),
            self.nnuts,
            ', '.join('%s: %i' % (
                kind, kind_counts[kind]) for kind in sorted(self.kinds)))

        return s


g_databases = {}


def get_database(path=None):
    path = os.path.abspath(path)
    if path not in g_databases:
        g_databases[path] = Database(path)

    return g_databases[path]


class Database(object):
    '''
    Shared meta-information database used by squirrel.
    '''

    def __init__(self, database_path=':memory:', log_statements=False):
        self._database_path = database_path
        try:
            util.ensuredirs(database_path)
            logger.debug('Opening connection to database: %s' % database_path)
            self._conn = sqlite3.connect(database_path)
        except sqlite3.OperationalError:
            raise error.SquirrelError(
                'Cannot connect to database: %s' % database_path)

        self._conn.text_factory = str
        self._tables = {}
        self._initialize_db()
        self._need_commit = False
        if log_statements:
            self._conn.set_trace_callback(self._log_statement)

    def _log_statement(self, statement):
        logger.debug(statement)

    def get_connection(self):
        return self._conn

    def _register_table(self, s):
        m = re.search(r'(\S+)\s*\(([^)]+)\)', s)
        table_name = m.group(1)
        dtypes = m.group(2)
        table_header = []
        for dele in dtypes.split(','):
            table_header.append(dele.split()[:2])

        self._tables[table_name] = table_header

        return s

    def _initialize_db(self):
        c = self._conn.cursor()
        c.execute(
            '''PRAGMA recursive_triggers = true''')

        c.execute(self._register_table(
            '''
                CREATE TABLE IF NOT EXISTS files (
                    file_id integer PRIMARY KEY,
                    path text,
                    format text,
                    mtime float,
                    size integer)
            '''))

        c.execute(
            '''
                CREATE UNIQUE INDEX IF NOT EXISTS index_files_file_path
                ON files (path)
            ''')

        c.execute(self._register_table(
            '''
                CREATE TABLE IF NOT EXISTS nuts (
                    nut_id integer PRIMARY KEY AUTOINCREMENT,
                    file_id integer,
                    file_segment integer,
                    file_element integer,
                    kind_id integer,
                    kind_codes_id integer,
                    tmin_seconds integer,
                    tmin_offset float,
                    tmax_seconds integer,
                    tmax_offset float,
                    deltat float,
                    kscale integer)
            '''))

        c.execute(
            '''
                CREATE UNIQUE INDEX IF NOT EXISTS index_nuts_file_element
                ON nuts (file_id, file_segment, file_element)
            ''')

        c.execute(self._register_table(
            '''
                CREATE TABLE IF NOT EXISTS kind_codes (
                    kind_codes_id integer PRIMARY KEY,
                    kind_id integer,
                    codes text)
            '''))

        c.execute(
            '''
                CREATE UNIQUE INDEX IF NOT EXISTS index_kind_codes
                ON kind_codes (kind_id, codes)
            ''')

        c.execute(self._register_table(
            '''
                CREATE TABLE IF NOT EXISTS kind_codes_count (
                    kind_codes_id integer PRIMARY KEY,
                    count integer)
            '''))

        c.execute(
            '''
                CREATE INDEX IF NOT EXISTS index_nuts_file_id
                ON nuts (file_id)
            ''')

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS delete_nuts_on_delete_file
                BEFORE DELETE ON files FOR EACH ROW
                BEGIN
                  DELETE FROM nuts where file_id == old.file_id;
                END
            ''')

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS delete_nuts_on_update_file
                BEFORE UPDATE ON files FOR EACH ROW
                BEGIN
                  DELETE FROM nuts where file_id == old.file_id;
                END
            ''')

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS increment_kind_codes
                BEFORE INSERT ON nuts FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO kind_codes_count
                    VALUES (new.kind_codes_id, 0);
                    UPDATE kind_codes_count
                    SET count = count + 1
                    WHERE new.kind_codes_id == kind_codes_id;
                END
            ''')

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS decrement_kind_codes
                BEFORE DELETE ON nuts FOR EACH ROW
                BEGIN
                    UPDATE kind_codes_count
                    SET count = count - 1
                    WHERE old.kind_codes_id == kind_codes_id;
                END
            ''')

        self._conn.commit()
        c.close()

    def dig(self, nuts):
        '''
        Store or update content meta-information.

        Given ``nuts`` are assumed to represent an up-to-date and complete
        inventory of a set of files. Any old information about these files is
        first pruned from the database (via database triggers). If such content
        is part of a live selection, it is also removed there. Then the new
        content meta-information is inserted into the main database. The
        content is not automatically inserted into the live selections again.
        It is in the responsibility of the selection object to perform this
        step.
        '''

        nuts = list(nuts)

        if not nuts:
            return

        c = self._conn.cursor()
        files = set()
        kind_codes = set()
        for nut in nuts:
            files.add((
                nut.file_path,
                nut.file_format,
                nut.file_mtime,
                nut.file_size))
            kind_codes.add((nut.kind_id, nut.codes))

        c.executemany(
            'INSERT OR IGNORE INTO files VALUES (NULL,?,?,?,?)', files)

        c.executemany(
            '''UPDATE files SET
                format = ?, mtime = ?, size = ?
                WHERE path == ?
            ''',
            ((x[1], x[2], x[3], x[0]) for x in files))

        c.executemany(
            'INSERT OR IGNORE INTO kind_codes VALUES (NULL,?,?)', kind_codes)

        c.executemany(
            '''
                INSERT INTO nuts VALUES
                    (NULL, (
                        SELECT file_id FROM files
                        WHERE path == ?
                     ),?,?,?,
                     (
                        SELECT kind_codes_id FROM kind_codes
                        WHERE kind_id == ? AND codes == ?
                     ), ?,?,?,?,?,?)
            ''',
            ((nut.file_path, nut.file_segment, nut.file_element,
              nut.kind_id,
              nut.kind_id, nut.codes,
              nut.tmin_seconds, nut.tmin_offset,
              nut.tmax_seconds, nut.tmax_offset,
              nut.deltat, nut.kscale) for nut in nuts))

        self._need_commit = True
        c.close()

    def undig(self, path):
        sql = '''
            SELECT
                files.path,
                files.format,
                files.mtime,
                files.size,
                nuts.file_segment,
                nuts.file_element,
                kind_codes.kind_id,
                kind_codes.codes,
                nuts.tmin_seconds,
                nuts.tmin_offset,
                nuts.tmax_seconds,
                nuts.tmax_offset,
                nuts.deltat
            FROM files
            INNER JOIN nuts ON files.file_id = nuts.file_id
            INNER JOIN kind_codes
                ON nuts.kind_codes_id == kind_codes.kind_codes_id
            WHERE path == ?
        '''

        return [model.Nut(values_nocheck=row)
                for row in self._conn.execute(sql, (path,))]

    def undig_all(self):
        sql = '''
            SELECT
                files.path,
                files.format,
                files.mtime,
                files.size,
                nuts.file_segment,
                nuts.file_element,
                kind_codes.kind_id,
                kind_codes.codes,
                nuts.tmin_seconds,
                nuts.tmin_offset,
                nuts.tmax_seconds,
                nuts.tmax_offset,
                nuts.deltat
            FROM files
            INNER JOIN nuts ON files.file_id == nuts.file_id
            INNER JOIN kind_codes
                ON nuts.kind_codes_id == kind_codes.kind_codes_id
        '''

        nuts = []
        path = None
        for values in self._conn.execute(sql):
            if path is not None and values[0] != path:
                yield path, nuts
                nuts = []

            if values[1] is not None:
                nuts.append(model.Nut(values_nocheck=values))

            path = values[0]

        if path is not None:
            yield path, nuts

    def undig_many(self, file_paths):
        selection = self.new_selection(file_paths)

        for (_, path), nuts in selection.undig_grouped():
            yield path, nuts

        del selection

    def new_selection(self, file_paths=None):
        selection = Selection(self)
        if file_paths:
            selection.add(file_paths)
        return selection

    def commit(self):
        if self._need_commit:
            self._conn.commit()
            self._need_commit = False

    def undig_content(self, nut):
        return None

    def remove(self, path):
        '''
        Prune content meta-inforamation about a given file.

        All content pieces belonging to file ``path`` are removed from the
        main database and any attached live selections (via database triggers).
        '''

        self._conn.execute(
            'DELETE FROM files WHERE path = ?', (path,))

    def reset(self, path):
        '''
        Prune information associated with a given file, but keep the file path.

        This method is called when reading a file failed. File attributes,
        format, size and modification time are set to NULL. File content
        meta-information is removed from the database and any attached live
        selections (via database triggers).
        '''

        self._conn.execute(
            '''
                UPDATE files SET
                    format = NULL,
                    mtime = NULL,
                    size = NULL
                WHERE path = ?
            ''', (path,))

    def _iter_counts(self, kind=None, kind_codes_count='kind_codes_count'):
        args = []
        sel = ''
        if kind is not None:
            sel = 'AND kind_codes.kind_id == ?'
            args.append(to_kind_id(kind))

        sql = ('''
            SELECT
                kind_codes.kind_id,
                kind_codes.codes,
                %(kind_codes_count)s.count
            FROM %(kind_codes_count)s
            INNER JOIN kind_codes
                ON %(kind_codes_count)s.kind_codes_id
                    == kind_codes.kind_codes_id
            WHERE %(kind_codes_count)s.count > 0
                ''' + sel + '''
        ''') % {'kind_codes_count': kind_codes_count}

        for kind_id, codes, count in self._conn.execute(sql, args):
            yield (to_kind(kind_id), tuple(codes.split(separator))), count

    def _iter_codes(self, kind=None, kind_codes_count='kind_codes_count'):
        args = []
        sel = ''
        if kind is not None:
            assert isinstance(kind, str)
            sel = 'AND kind_codes.kind_id == ?'
            args.append(to_kind_id(kind))

        sql = ('''
            SELECT DISTINCT kind_codes.codes FROM %(kind_codes_count)s
            INNER JOIN kind_codes
                ON %(kind_codes_count)s.kind_codes_id
                    == kind_codes.kind_codes_id
            WHERE %(kind_codes_count)s.count > 0
                ''' + sel + '''
            ORDER BY kind_codes.codes
        ''') % {'kind_codes_count': kind_codes_count}

        for row in self._conn.execute(sql, args):
            yield tuple(row[0].split(separator))

    def _iter_kinds(self, codes=None, kind_codes_count='kind_codes_count'):
        args = []
        sel = ''
        if codes is not None:
            assert isinstance(codes, tuple)
            sel = 'AND kind_codes.codes == ?'
            args.append(separator.join(codes))

        sql = ('''
            SELECT DISTINCT kind_codes.kind_id FROM %(kind_codes_count)s
            INNER JOIN kind_codes
                ON %(kind_codes_count)s.kind_codes_id
                    == kind_codes.kind_codes_id
            WHERE %(kind_codes_count)s.count > 0
                ''' + sel + '''
            ORDER BY kind_codes.kind_id
        ''') % {'kind_codes_count': kind_codes_count}

        for row in self._conn.execute(sql, args):
            yield to_kind(row[0])

    def iter_kinds(self, codes=None):
        return self._iter_kinds(codes=codes)

    def iter_codes(self, kind=None):
        return self._iter_codes(kind=kind)

    def iter_counts(self, kind=None):
        return self._iter_counts(kind=kind)

    def get_kinds(self, codes=None):
        return list(self.iter_kinds(codes=codes))

    def get_codes(self, kind=None):
        return list(self.iter_codes(kind=kind))

    def get_counts(self, kind=None):
        d = {}
        for (k, codes), count in self.iter_counts():
            if k not in d:
                d[k] = {}

            d[k][codes] = count

        if kind is not None:
            return d[kind]
        else:
            return d

    def get_nfiles(self):
        sql = '''SELECT COUNT(*) FROM files'''
        for row in self._conn.execute(sql):
            return row[0]

    def get_nnuts(self):
        sql = '''SELECT COUNT(*) FROM nuts'''
        for row in self._conn.execute(sql):
            return row[0]

    def get_total_size(self):
        sql = '''
            SELECT SUM(files.size) FROM files
        '''

        for row in self._conn.execute(sql):
            return row[0] or 0

        return 0

    def get_stats(self):
        return DatabaseStats(
            nfiles=self.get_nfiles(),
            nnuts=self.get_nnuts(),
            kinds=self.get_kinds(),
            codes=self.get_codes(),
            counts=self.get_counts(),
            total_size=self.get_total_size())

    def __str__(self):
        return str(self.get_stats())

    def print_tables(self, stream=None):
        for table in [
                'files',
                'nuts',
                'kind_codes',
                'kind_codes_count']:

            self.print_table(table, stream=stream)

    def print_table(self, name, stream=None):

        if stream is None:
            stream = sys.stdout

        class hstr(str):
            def __repr__(self):
                return self

        w = stream.write
        w('\n')
        w('\n')
        w(name)
        w('\n')
        sql = 'SELECT * FROM %s' % name
        tab = []
        if name in self._tables:
            headers = self._tables[name]
            tab.append([None for _ in headers])
            tab.append([hstr(x[0]) for x in headers])
            tab.append([hstr(x[1]) for x in headers])
            tab.append([None for _ in headers])

        for row in self._conn.execute(sql):
            tab.append([x for x in row])

        widths = [
            max((len(repr(x)) if x is not None else 0) for x in col)
            for col in zip(*tab)]

        for row in tab:
            w(' '.join(
                (repr(x).ljust(wid) if x is not None else ''.ljust(wid, '-'))
                for (x, wid) in zip(row, widths)))

            w('\n')

        w('\n')


__all__ = [
    'Selection',
    'Squirrel',
    'SquirrelStats',
    'Database',
    'DatabaseStats',
]
