from __future__ import absolute_import, print_function

import sys
import os
import re
import threading
import sqlite3
import logging
from collections import defaultdict

from pyrocko.io_common import FileLoadError
from pyrocko.guts import Object, Int, List, Tuple, String, Timestamp, Dict
from pyrocko import config, util

from . import model, io

from .model import to_kind_ids, to_kind_id, to_kind, separator
from .client import fdsn, catalog
from . import client

logger = logging.getLogger('pyrocko.squirrel.base')


g_databases = {}


class NotAvailable(Exception):
    pass


def wrap(lines, width=80, indent=''):
    lines = list(lines)
    fwidth = max(len(s) for s in lines)
    nx = max(1, (80-len(indent)) // (fwidth+1))
    i = 0
    rows = []
    while i < len(lines):
        rows.append(indent + ' '.join(x.ljust(fwidth) for x in lines[i:i+nx]))
        i += nx

    return '\n'.join(rows)


def get_database(database=None):
    if isinstance(database, Database):
        return database

    if database is None:
        database = os.path.join(config.config().cache_dir, 'db.squirrel')

    database = os.path.abspath(database)

    if database not in g_databases:
        g_databases[database] = Database(database)

    return g_databases[database]


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
    'waveform': 6}


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


class Selection(object):

    '''
    Database backed file selection.

    :param database: :py:class:`Database` object or path to database or
        ``None`` for user's default database
    :param str persistent: if given a name, create a persistent selection

    By default, a temporary table in the database is created to hold the names
    of the files in the selection. This table is only visible inside the
    application which created it. If a name is given to ``persistent``, a named
    selection is created, which is visible also in other applications using the
    same database. Paths of files can be added to the selection using the
    :py:meth:`add` method.
    '''

    def __init__(self, database=None, persistent=None):
        if database is None and persistent is not None:
            raise Exception(
                'should not use persistent selection with shared global '
                'database as this would impair its performance')

        database = get_database(database)

        if persistent is not None:
            assert isinstance(persistent, str)
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', persistent):
                raise Exception(
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

        self._conn.execute(self._register_table(
            '''
                CREATE TABLE IF NOT EXISTS %(db)s.%(file_states)s (
                    file_id integer PRIMARY KEY,
                    file_state integer)
            ''' % self._names))

    def __del__(self):
        if not self._persistent:
            self._delete()
        else:
            self._conn.commit()

    def _register_table(self, s):
        return self._database._register_table(s)

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
        self._conn.execute(
            'DROP TABLE %(db)s.%(file_states)s' % self._names)

    def add(self, file_paths, state=0):
        '''
        Add files to the selection.

        :param file_paths: Paths to files to be added to the selection.
        :type file_paths: iterator yielding ``str`` objects
        '''

        if isinstance(file_paths, str):
            file_paths = [file_paths]

        try:
            if len(file_paths) < 100:
                # short non-iterator file_paths: can do without temp table

                self._conn.executemany(
                    '''
                        INSERT OR IGNORE INTO files
                        VALUES (NULL, ?, NULL, NULL, NULL)
                    ''', ((x,) for x in file_paths))

                self._conn.executemany(
                    '''
                        INSERT OR IGNORE INTO %(db)s.%(file_states)s
                        SELECT files.file_id, ?
                        FROM files
                        WHERE files.path = ?
                    ''' % self._names, (
                        (state, filepath) for filepath in file_paths))

                return

        except TypeError:
            pass

        self._conn.execute(
            '''
                CREATE TEMP TABLE temp.%(bulkinsert)s
                (path text)
            ''' % self._names)

        self._conn.executemany(
            'INSERT INTO temp.%(bulkinsert)s VALUES (?)' % self._names,
            ((x,) for x in file_paths))

        self._conn.execute(
            '''
                INSERT OR IGNORE INTO files
                SELECT NULL, path, NULL, NULL, NULL
                FROM temp.%(bulkinsert)s
            ''' % self._names)

        self._conn.execute(
            '''
                INSERT OR IGNORE INTO %(db)s.%(file_states)s
                SELECT files.file_id, ?
                FROM temp.%(bulkinsert)s
                INNER JOIN files
                ON temp.%(bulkinsert)s.path == files.path
            ''' % self._names, (state,))

        self._conn.execute(
            'DROP TABLE temp.%(bulkinsert)s' % self._names)

    def remove(self, file_paths):
        '''
        Remove files from the selection.

        :param file_paths: Paths to files to be removed from the selection.
        :type file_paths: ``list`` of ``str``
        '''
        if isinstance(file_paths, str):
            file_paths = [file_paths]

        self._conn.executemany(
            '''
                DELETE FROM %(db)s.%(file_states)s
                WHERE %(db)s.%(file_states)s.file_id ==
                    (SELECT files.file_id
                     FROM files
                     WHERE files.path == ?)
            ''' % self._names, ((path,) for path in file_paths))

    def undig_grouped(self, skip_unchanged=False):
        '''
        Get content inventory of all files in selection.

        :param: skip_unchanged: if ``True`` only inventory of modified files
            is yielded (:py:class:`flag_unchanged` must be called beforehand).

        This generator yields tuples ``(path, nuts)`` where ``path`` is the
        path to the file and ``nuts`` is a list of
        :py:class:`pyrocko.squirrel.Nut` objects representing the contents of
        the file.
        '''

        if skip_unchanged:
            where = '''
                WHERE %(db)s.%(file_states)s.file_state == 0
            '''
        else:
            where = ''

        sql = ('''
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
            FROM %(db)s.%(file_states)s
            LEFT OUTER JOIN files
                ON %(db)s.%(file_states)s.file_id = files.file_id
            LEFT OUTER JOIN nuts
                ON files.file_id = nuts.file_id
            LEFT OUTER JOIN kind_codes
                ON nuts.kind_codes_id == kind_codes.kind_codes_id
        ''' + where + '''
            ORDER BY %(db)s.%(file_states)s.file_id
        ''') % self._names

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

    def iter_paths(self):
        sql = ('''
            SELECT
                files.path
            FROM %(db)s.%(file_states)s
            INNER JOIN files
            ON files.file_id = %(db)s.%(file_states)s.file_id
            ORDER BY %(db)s.%(file_states)s.file_id
        ''') % self._names

        for values in self._conn.execute(sql):
            yield values[0]

    def flag_unchanged(self, check=True):
        '''
        Mark files which have not been modified.

        :param check: if ``True`` query modification times of known files on
            disk. If ``False``, only flag unknown files.
        '''

        sql = '''
            UPDATE %(db)s.%(file_states)s
            SET file_state = 0
            WHERE (
                SELECT mtime
                FROM files
                WHERE files.file_id == %(db)s.%(file_states)s.file_id) IS NULL
        ''' % self._names

        self._conn.execute(sql)

        if not check:
            return

        def iter_file_states():
            sql = '''
                SELECT
                    files.file_id,
                    files.path,
                    files.format,
                    files.mtime,
                    files.size
                FROM %(db)s.%(file_states)s
                INNER JOIN files
                    ON %(db)s.%(file_states)s.file_id == files.file_id
                WHERE %(db)s.%(file_states)s.file_state != 0
                ORDER BY %(db)s.%(file_states)s.file_id
            ''' % self._names

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
                    continue

        # could better use callback function here...

        sql = '''
            UPDATE %(db)s.%(file_states)s
            SET file_state = ?
            WHERE file_id = ?
        ''' % self._names

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

        s = '''
available codes:
%s
number of files:               %i
total size of known files:     %s
number of index nuts:          %i
available nut kinds:           %s
time span of indexed contents: %s - %s''' % (
            wrap(('.'.join(x) for x in self.codes), indent='  '),
            self.nfiles,
            util.human_bytesize(self.total_size),
            self.nnuts,
            ', '.join('%s: %i' % (
                kind, kind_counts[kind]) for kind in sorted(self.kinds)),
            util.time_to_str(self.tmin), util.time_to_str(self.tmax))

        return s


class Squirrel(Selection):
    '''
    Prompt, lazy, indexing, caching, dynamic seismological dataset access.

    :param database: :py:class:`Database` object or path to database or
        ``None`` for user's default database
    :param str persistent: if given a name, create a persistent selection

    By default, temporary tables are created in the attached database to hold
    the names of the files in the selection as well as various indices and
    counters. These tables are only visible inside the application which
    created it. If a name is given to ``persistent``, a named selection is
    created, which is visible also in other applications using the same
    database. Paths of files can be added to the selection using the
    :py:meth:`add` method.
    '''

    def __init__(self, database=None, persistent=None):
        Selection.__init__(self, database=database, persistent=persistent)
        c = self._conn
        self._contents = {}

        self._names.update({
            'nuts': self.name + '_nuts',
            'kind_codes_count': self.name + '_kind_codes_count'})

        c.execute(self._register_table(
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
            ''' % self._names))

        c.execute(
            '''
                CREATE UNIQUE INDEX IF NOT EXISTS %(db)s.%(nuts)s_file_element
                    ON %(nuts)s (file_id, file_segment, file_element)
            ''' % self._names)

        c.execute(self._register_table(
            '''
                CREATE TABLE IF NOT EXISTS %(db)s.%(kind_codes_count)s (
                    kind_codes_id integer PRIMARY KEY,
                    count integer)
            ''' % self._names))

        c.execute(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_file_id
                ON %(nuts)s (file_id)
            ''' % self._names)

        c.execute(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_tmin_seconds
                ON %(nuts)s (tmin_seconds)
            ''' % self._names)

        c.execute(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_tmax_seconds
                ON %(nuts)s (tmax_seconds)
            ''' % self._names)

        c.execute(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_kscale
                ON %(nuts)s (kind_id, kscale, tmin_seconds)
            ''' % self._names)

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_delete_nuts
                BEFORE DELETE ON main.files FOR EACH ROW
                BEGIN
                  DELETE FROM %(nuts)s WHERE file_id == old.file_id;
                END
            ''' % self._names)

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_delete_nuts2
                BEFORE UPDATE ON main.files FOR EACH ROW
                BEGIN
                  DELETE FROM %(nuts)s WHERE file_id == old.file_id;
                END
            ''' % self._names)

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS
                    %(db)s.%(file_states)s_delete_files
                BEFORE DELETE ON %(db)s.%(file_states)s FOR EACH ROW
                BEGIN
                    DELETE FROM %(nuts)s WHERE file_id == old.file_id;
                END
            ''' % self._names)

        c.execute(
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
            ''' % self._names)

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_dec_kind_codes
                BEFORE DELETE ON %(nuts)s FOR EACH ROW
                BEGIN
                    UPDATE %(kind_codes_count)s
                    SET count = count - 1
                    WHERE old.kind_codes_id
                        == %(kind_codes_count)s.kind_codes_id;
                END
            ''' % self._names)

    def _delete(self):
        '''Delete database tables associated with this squirrel.'''

        self._conn.execute(
            'DROP TABLE %(db)s.%(nuts)s' % self._names)

        self._conn.execute(
            'DROP TABLE %(db)s.%(kind_codes_count)s' % self._names)

        Selection._delete(self)

    def print_tables(self, table_names=None, stream=None):
        if stream is None:
            stream = sys.stdout

        if isinstance(table_names, str):
            table_names = [table_names]

        if table_names is None:
            table_names = [
                'selection_file_states',
                'selection_nuts',
                'selection_kind_codes_count']
            # 'files', 'nuts', 'kind_codes', 'kind_codes_count']

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
        :param str format: file format identifier or ``'detect'`` for
            auto-detection

        Complexity: O(log N)
        '''
        if isinstance(kinds, str):
            kinds = (kinds,)

        if isinstance(file_paths, str):
            file_paths = [file_paths]

        Selection.add(self, self.iter_expand_dirs(file_paths))
        self._load(format, check)
        self._update_nuts(kinds)

    def add_virtual(self, nuts, virtual_file_paths=None):
        '''
        Add content which is not backed by files.

        Stores to the main database and the selection.
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

    def add_volatile(self, nuts):
        '''
        Add run-time content to the selection.

        Stuff is not stored to the main database.
        '''

        pass

    def _load(self, format, check):
        for _ in io.iload(
                self,
                content=[],
                skip_unchanged=True,
                format=format,
                check=check):
            pass

    def _update_nuts(self, kinds=None):
        c = self._conn
        w_kinds = ''
        args = []
        if kinds is not None:
            w_kinds = 'AND kind_codes.kind_id IN (%s)' % ', '.join(
                '?'*len(kinds))
            args.extend(to_kind_ids(kinds))

        c.execute((
            '''
                INSERT INTO %(db)s.%(nuts)s
                SELECT nuts.* FROM %(db)s.%(file_states)s
                INNER JOIN nuts
                    ON %(db)s.%(file_states)s.file_id == nuts.file_id
                INNER JOIN kind_codes
                    ON nuts.kind_codes_id ==
                       kind_codes.kind_codes_id
                WHERE %(db)s.%(file_states)s.file_state != 2
            ''' + w_kinds) % self._names, args)

        c.execute(
            '''
                UPDATE %(db)s.%(file_states)s
                SET file_state = 2
            ''' % self._names)

    def add_source(self, source):
        '''
        Add remote resource.
        '''

        self._sources.append(source)

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

        tmin_avail, tmax_avail = self.get_time_span()
        if tmin is None:
            tmin = tmin_avail

        if tmax is None:
            tmax = tmax_avail

        return tmin, tmax, codes

    def get_nuts(
            self, kind, tmin=None, tmax=None, codes=None):
        '''
        Iterate content intersecting with the half open interval [tmin, tmax[.

        :param kind: ``str``, content kind to extract
        :param tmin: timestamp, start time of interval
        :param tmax: timestamp, end time of interval
        :param codes: tuple of str, pattern of content codes to be matched

        Complexity: O(log N)

        Yields :py:class:`pyrocko.squirrel.Nut` objects representing the
        intersecting content.
        '''

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
            WHERE ( ''' + ' OR '.join(tmin_cond) + ''' )
                AND ''' + ' AND '.join(extra_cond)) % self._names

        for row in self._conn.execute(sql, args):
            nut = model.Nut(values_nocheck=row)
            if nut.tmin < tmax and tmin < nut.tmax:
                yield nut

    def _get_nuts_naiv(self, kind, tmin=None, tmax=None):
        tmin_seconds, tmin_offset = model.tsplit(tmin)
        tmax_seconds, tmax_offset = model.tsplit(tmax)

        tmin_avail, tmax_avail = self.get_time_span()
        if tmin is None:
            tmin = tmin_avail
            tmax = tmax_avail

        sql = '''
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
        ''' % self._names

        for row in self._conn.execute(
                sql, (to_kind_id(kind), tmin_seconds, tmax_seconds+1)):

            nut = model.Nut(values_nocheck=row)
            if nut.tmin < tmax and tmin < nut.tmax:
                yield nut

    def get_time_span(self):
        '''
        Get time interval over all content in selection.

        Complexity O(1), independent of number of nuts

        :returns: (tmin, tmax)
        '''
        sql = '''
            SELECT MIN(tmin_seconds + tmin_offset)
            FROM %(db)s.%(nuts)s WHERE
            tmin_seconds == (SELECT MIN(tmin_seconds) FROM %(db)s.%(nuts)s)
        ''' % self._names
        tmin = None
        for row in self._conn.execute(sql):
            tmin = row[0]

        sql = '''
            SELECT MAX(tmax_seconds + tmax_offset)
            FROM %(db)s.%(nuts)s WHERE
            tmax_seconds == (SELECT MAX(tmax_seconds) FROM %(db)s.%(nuts)s)
        ''' % self._names
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

        :returns: sorted list of available codes
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
        '''Get number of files in selection.'''

        sql = '''SELECT COUNT(*) FROM %(db)s.%(file_states)s''' % self._names
        for row in self._conn.execute(sql):
            return row[0]

    def get_nnuts(self):
        '''Get number of nuts in selection.'''
        sql = '''SELECT COUNT(*) FROM %(db)s.%(nuts)s''' % self._names
        for row in self._conn.execute(sql):
            return row[0]

    def get_total_size(self):
        '''Get aggregated file size available in selection.'''
        sql = '''
            SELECT SUM(files.size) FROM %(db)s.%(file_states)s
            INNER JOIN files
                ON %(db)s.%(file_states)s.file_id = files.file_id
        ''' % self._names

        for row in self._conn.execute(sql):
            return row[0]

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

    def get_content(self, nut):
        '''
        Get and possibly load full content for a given index entry from file.

        Loads the actual content objects (channel, station, waveform, ...) from
        file. For efficiency sibling content (all stuff in the same file
        segment) will also be loaded as a side effect. The loaded contents are
        cached in the squirrel object.
        '''

        if nut.key not in self._contents:
            for nut_loaded in io.iload(
                    nut.file_path,
                    segment=nut.file_segment,
                    format=nut.file_format,
                    database=self._database):

                self._contents[nut_loaded.key] = nut_loaded.content

        if nut.key not in self._contents:
            raise NotAvailable(
                'Unable to retrieve content: %s, %s, %s' % nut.key)

        return self._contents[nut.key]

    def check_duplicates(self, nuts):
        d = defaultdict(list)
        for nut in nuts:
            d[nut.codes].append(nut)

        for codes, group in d.items():
            if len(group) > 1:
                logger.warn(
                    'Multiple entries maching codes %s'
                    % '.'.join(codes.split(separator)))

    def get_stations(self, *args, **kwargs):
        args = self.get_selection_args(*args, **kwargs)
        nuts = sorted(
            self.get_nuts('station', *args), key=lambda nut: nut.dkey)
        self.check_duplicates(nuts)
        return [self.get_content(nut) for nut in nuts]

    def get_channels(self, *args, **kwargs):
        args = self.get_selection_args(*args, **kwargs)
        nuts = sorted(
            self.get_nuts('channel', *args), key=lambda nut: nut.dkey)
        self.check_duplicates(nuts)
        return [self.get_content(nut) for nut in nuts]

    def get_events(self, *args, **kwargs):
        args = self.get_selection_args(*args, **kwargs)
        nuts = sorted(
            self.get_nuts('event', *args), key=lambda nut: nut.dkey)
        self.check_duplicates(nuts)
        return [self.get_content(nut) for nut in nuts]

    def _redeem_promises(self, *args):

        waveforms = list(self.get_nuts('waveform', *args))
        promises = list(self.get_nuts('waveform_promise', *args))

        tmin, tmax, _ = args
        codes_to_waveforms = dict(
            (nut.codes, nut) for nut in waveforms)

        orders = []
        for promise in promises:
            waveforms_have = codes_to_waveforms[promise.codes]
            for block_tmin, block_tmax in blocks(tmin, tmax, promise.deltat):
                orders.append(
                    Order(
                        promise.file_path,
                        promise.codes,
                        (block_tmin, block_tmax),
                        gaps(waveforms_have, block_tmin, block_tmax)))

                    # after successful download delete block span from promise
                    # if gaps is empty (noop query), still delete it
                    # deletion means split promise

        # setup order matrix
        #   cols (code, tmin, tmax)
        #   rows (client)

        def order_key(order):
            return (order.codes, order.block_tmin, order.block_tmax)

        client_priority = dict(
            (client, i) for (i, client) in enumerate(clients))

        order_groups = defaultdict(list)
        for order in orders:
            order_groups[order_key(order)].append(order)

        for k, order_group in order_groups.items():
            order_group.sort(
                key=lambda order: client_priority[order.client])

        while order_groups:

            orders_now = []
            for k, order_group in order_groups.items():
                try:
                    orders_now.append(order_group.pop(0))
                except XXX:
                    pass

    def get_waveforms(self, *args, **kwargs):
        args = self.get_selection_args(*args, **kwargs)
        self._redeem_promises(*args)
        nuts = list(self.get_nuts('waveform', *args))
        self.check_duplicates(nuts)
        return sorted(self.get_content(nut) for nut in nuts)

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
                depth=sargs[6],
                channels=pchannels))

        return pstations

    def get_pyrocko_events(self, *args, **kwargs):
        from pyrocko import model as pmodel  # noqa
        return self.get_events(*args, **kwargs)

    def __str__(self):
        return str(self.get_stats())


class DatabaseStats(Object):
    '''
    Container to hold statistics about contents cached in meta-information db.
    '''

    nfiles = Int.T()
    nnuts = Int.T()
    codes = List.T(List.T(String.T()))
    kinds = List.T(String.T())
    total_size = Int.T()
    counts = Dict.T(String.T(), Dict.T(String.T(), Int.T()))


class Database(object):
    '''
    Shared meta-information database used by squirrel.
    '''

    def __init__(self, database_path=':memory:'):
        self._database_path = database_path
        self._conn = sqlite3.connect(database_path)
        self._conn.text_factory = str
        self._tables = {}
        self._initialize_db()
        self._need_commit = False

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

        for path, nuts in selection.undig_grouped():
            yield path, nuts

        del selection

    def new_selection(self, file_paths=None, state=0):
        selection = Selection(self)
        if file_paths:
            selection.add(file_paths, state=state)
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
            return row[0]

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
