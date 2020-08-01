from __future__ import absolute_import, print_function

import time
import os
import copy
import logging
import tempfile
from collections import defaultdict
try:
    import cPickle as pickle
except ImportError:
    import pickle
import os.path as op
from .base import Source, Constraint
from ..model import make_waveform_promise_nut, ehash
from pyrocko.client import fdsn

from pyrocko import config, util, trace, io
from pyrocko.io_common import FileLoadError

from pyrocko.guts import Object, String

fdsn.g_timeout = 60.

logger = logging.getLogger('pyrocko.squirrel.client.fdsn')

sites_not_supporting = {
    'startbefore': ['geonet'],
    'includerestricted': ['geonet']}


def diff(fn_a, fn_b):
    try:
        if os.stat(fn_a)[8] != os.stat(fn_b)[8]:
            return True

    except OSError:
        return True

    with open(fn_a, 'rb') as fa:
        with open(fn_b, 'rb') as fb:
            while True:
                a = fa.read(1024)
                b = fb.read(1024)
                if a != b:
                    return True

                if len(a) == 0 or len(b) == 0:
                    return False


class Archive(Object):

    def add(self):
        raise NotImplementedError()


class MSeedArchive(Archive):
    template = String.T(default=op.join(
        '%(tmin_year)s',
        '%(tmin_month)s',
        '%(tmin_day)s',
        'trace_%(network)s_%(station)s_%(location)s_%(channel)s'
        + '_%(tmin_us)s_%(tmax_us)s.mseed'))

    def __init__(self, **kwargs):
        Archive.__init__(self, **kwargs)
        self._base_path = None

    def set_base_path(self, path):
        self._base_path = path

    def add(self, trs):
        path = op.join(self._base_path, self.template)
        return io.save(trs, path)


def order_summary(orders):
    codes = sorted(set(order.codes[1:-1] for order in orders))
    if len(codes) >= 2:
        return '%i orders for %s - %s' % (
            len(orders),
            '.'.join(codes[0]),
            '.'.join(codes[-1]))

    else:
        return '%i orders for %s' % (
            len(orders),
            '.'.join(codes[0]))


class FDSNSource(Source):

    def __init__(
            self, site,
            query_args=None,
            expires=None,
            cache_dir=None,
            user_credentials=None,
            auth_token=None,
            auth_token_path=None):

        Source.__init__(self)

        self._site = site
        self._constraint = None
        self._no_query_age_max = expires

        assert None in (auth_token, auth_token_path)

        if auth_token_path is not None:
            try:
                with open(auth_token_path, 'rb') as f:
                    auth_token = f.read().decode('ascii')

            except OSError as e:
                raise FileLoadError(
                    'Cannot load auth token file (%s): %s'
                    % (str(e), auth_token_path))

        s = site
        if auth_token is not None:
            s += auth_token

        if user_credentials is not None:
            s += user_credentials[0]
            s += user_credentials[1]
        if query_args is not None:
            s += ','.join(
                '%s:%s' % (k, query_args[k])
                for k in sorted(query_args.keys()))

        self._auth_token = auth_token
        self._user_credentials = user_credentials
        self._query_args = query_args

        self._hash = ehash(s)
        self.source_id = 'client:fdsn:%s' % self._hash

        self._cache_dir = op.join(
            cache_dir or config.config().cache_dir,
            'fdsn',
            self._hash)

        util.ensuredir(self._cache_dir)
        self._load_constraint()
        self._archive = MSeedArchive()
        self._archive.set_base_path(op.join(
            self._cache_dir,
            'waveforms'))

    def get_channel_file_paths(self):
        return [op.join(self._cache_dir, 'channels.stationxml')]

    def update_channel_inventory(self, squirrel, constraint=None):
        if constraint is None:
            constraint = Constraint()

        if self._constraint and self._constraint.contains(constraint) \
                and not self._stale_channel_inventory():

            logger.info(
                'using cached channel information for FDSN site %s'
                % self._site)

        else:
            if self._constraint:
                constraint_temp = copy.deepcopy(self._constraint)
                constraint_temp.expand(constraint)
                constraint = constraint_temp

            channel_sx = self._do_channel_query(constraint)
            channel_sx.created = None  # timestamp would ruin diff

            fn = self.get_channel_file_paths()[0]
            fn_temp = fn + '.%i.temp' % os.getpid()
            channel_sx.dump_xml(filename=fn_temp)

            if op.exists(fn):
                if diff(fn, fn_temp):
                    os.rename(fn_temp, fn)
                    logger.info('changed: %s' % fn)
                else:
                    os.unlink(fn_temp)
                    logger.info('no change: %s' % fn)
            else:
                os.rename(fn_temp, fn)
                logger.info('new: %s' % fn)

            self._constraint = constraint
            self._dump_constraint()

        squirrel.add(self.get_channel_file_paths())

    def _do_channel_query(self, constraint):
        extra_args = {}

        if self._site in sites_not_supporting['startbefore']:
            if constraint.tmin is not None:
                extra_args['starttime'] = constraint.tmin
            if constraint.tmax is not None:
                extra_args['endtime'] = constraint.tmax

        else:
            if constraint.tmin is not None:
                extra_args['endafter'] = constraint.tmin
            if constraint.tmax is not None:
                extra_args['startbefore'] = constraint.tmax

        if self._site not in sites_not_supporting['includerestricted']:
            extra_args.update(
                includerestricted=(
                    self._user_credentials is not None
                    or self._auth_token is not None))

        if self._query_args is not None:
            extra_args.update(self._query_args)

        logger.info(
            'querying channel information from FDSN site %s'
            % self._site)

        channel_sx = fdsn.station(
            site=self._site,
            format='text',
            level='channel',
            **extra_args)

        return channel_sx

    def _get_constraint_file_path(self):
        return op.join(self._cache_dir, 'constraint.pickle')

    def _load_constraint(self):
        fn = self._get_constraint_file_path()
        if op.exists(fn):
            with open(fn, 'rb') as f:
                self._constraint = pickle.load(f)
        else:
            self._constraint = None

    def _dump_constraint(self):
        with open(self._get_constraint_file_path(), 'wb') as f:
            pickle.dump(self._constraint, f, protocol=2)

    def _stale_channel_inventory(self):
        if self._no_query_age_max is not None:
            for file_path in self.get_channel_file_paths():
                try:
                    t = os.stat(file_path)[8]
                    return t < time.time() - self._no_query_age_max
                except OSError:
                    return True

        return False

    def update_waveform_inventory(self, squirrel, constraint):
        from pyrocko.squirrel import Squirrel

        # get meta information of stuff available through this source
        sub_squirrel = Squirrel(database=squirrel.get_database())
        sub_squirrel.add(self.get_channel_file_paths(), check=False)

        nuts = sub_squirrel.get_nuts(
            'channel', constraint.tmin, constraint.tmax)

        file_path = self.source_id
        squirrel.add_virtual(
            (make_waveform_promise_nut(
                file_path=file_path,
                **nut.waveform_promise_kwargs) for nut in nuts),
            virtual_file_paths=[file_path])

    def get_user_credentials(self):
        d = {}
        if self._user_credentials is not None:
            d['user'], d['passwd'] = self._user_credentials

        if self._auth_token is not None:
            d['token'] = self._auth_token

        return d

    def download_waveforms(self, squirrel, orders):
        neach = 20
        i = 0
        while i < len(orders):
            orders_now = orders[i:i+neach]

            selection_now = []
            for order in orders_now:
                selection_now.append(
                    order.codes[1:5] + (order.tmin, order.tmax))

            with tempfile.NamedTemporaryFile() as f:
                try:
                    logger.info(
                        'Downloading data from FDSN site "%s": %s.'
                        % (self._site, order_summary(orders_now)))

                    data = fdsn.dataselect(
                        site=self._site, selection=selection_now,
                        **self.get_user_credentials())

                    while True:
                        buf = data.read(1024)
                        if not buf:
                            break
                        f.write(buf)

                    f.flush()

                    trs = io.load(f.name)
                    by_nslc = defaultdict(list)
                    for tr in trs:
                        by_nslc[tr.nslc_id].append(tr)

                    for order in orders_now:
                        trs_order = []
                        for tr in by_nslc[order.codes[1:5]]:
                            try:
                                trs_order.append(tr.chop(
                                    order.tmin, order.tmax, inplace=False))
                            except trace.NoData:
                                pass

                        paths = self._archive.add(trs_order)
                        squirrel.add(paths)

                except fdsn.EmptyResult:
                    pass

                except util.HTTPError:
                    logger.warn(
                        'An error occurred while downloading data from '
                        'site "%s" for channels \n  %s' % (
                            self._site,
                            '\n  '.join(
                                '.'.join(x[:4]) for x in selection_now)))

            i += neach
