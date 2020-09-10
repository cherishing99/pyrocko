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
        if os.stat(fn_a).st_size != os.stat(fn_b).st_size:
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
        return '%i orders, %s - %s' % (
            len(orders),
            '.'.join(codes[0]),
            '.'.join(codes[-1]))

    else:
        return '%i orders, %s' % (
            len(orders),
            '.'.join(codes[0]))


def combine_selections(selection):
    out = []
    last = None
    for this in selection:
        if last and this[:4] == last[:4] and this[4] == last[5]:
            last = last[:5] + (this[5],)
        else:
            if last:
                out.append(last)

            last = this

    if last:
        out.append(last)

    return out


class FDSNSource(Source):

    def __init__(
            self, site,
            query_args=None,
            expires=None,
            cache_path=None,
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

        self._cache_path = op.join(
            cache_path or config.config().cache_dir,
            'fdsn',
            self._hash)

        util.ensuredir(self._cache_path)
        self._load_constraint()
        self._archive = MSeedArchive()
        self._archive.set_base_path(self._get_waveforms_path())

    def setup(self, squirrel):
        squirrel.add(self._get_waveforms_path())

    def _get_channels_path(self):
        return op.join(self._cache_path, 'channels.stationxml')

    def _get_waveforms_path(self):
        return op.join(self._cache_path, 'waveforms')

    def _log_meta(self, message, target=logger.info):
        log_prefix = 'FDSN "%s" metadata:' % self._site
        target(' '.join((log_prefix, message)))

    def _log_info_data(self, *args):
        log_prefix = 'FDSN "%s" waveforms:' % self._site
        logger.info(' '.join((log_prefix,) + args))

    def _str_due(self):
        return util.time_to_str()

    def _str_expires(self, now):
        t = self._get_expiration_time()
        if t is None:
            return 'expires: never'
        else:
            expire = 'expires' if t > now else 'expired'
            return '%s: %s' % (
                expire,
                util.time_to_str(t, format='%Y-%m-%d %H:%M:%S'))

    def update_channel_inventory(self, squirrel, constraint=None):
        if constraint is None:
            constraint = Constraint()

        expiration_time = self._get_expiration_time()
        now = time.time()

        log_target = logger.info
        if self._constraint and self._constraint.contains(constraint) \
                and (expiration_time is None or now < expiration_time):

            s_case = 'using cached'

        else:
            if self._constraint:
                constraint_temp = copy.deepcopy(self._constraint)
                constraint_temp.expand(constraint)
                constraint = constraint_temp

            try:
                channel_sx = self._do_channel_query(constraint)

                channel_sx.created = None  # timestamp would ruin diff

                fn = self._get_channels_path()
                fn_temp = fn + '.%i.temp' % os.getpid()
                channel_sx.dump_xml(filename=fn_temp)

                if op.exists(fn):
                    if diff(fn, fn_temp):
                        os.rename(fn_temp, fn)
                        s_case = 'updated'
                    else:
                        os.unlink(fn_temp)
                        squirrel.silent_touch(fn)
                        s_case = 'upstream unchanged'

                else:
                    os.rename(fn_temp, fn)
                    s_case = 'new'

                self._constraint = constraint
                self._dump_constraint()

            except OSError as e:
                s_case = 'update failed (%s)' % str(e)
                log_target = logger.error

        self._log_meta(
            '%s (%s)' % (s_case, self._str_expires(now)),
            target=log_target)

        fn = self._get_channels_path()
        if os.path.exists(fn):
            squirrel.add(fn)

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

        self._log_meta('querying...')

        channel_sx = fdsn.station(
            site=self._site,
            format='text',
            level='channel',
            **extra_args)

        return channel_sx

    def _get_constraint_file_path(self):
        return op.join(self._cache_path, 'constraint.pickle')

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

    def _get_expiration_time(self):
        if self._no_query_age_max is None:
            return None

        try:
            file_path = self._get_channels_path()
            t = os.stat(file_path)[8]
            return t + self._no_query_age_max

        except OSError:
            return True

        return None

    def update_waveform_inventory(self, squirrel, constraint):
        from pyrocko.squirrel import Squirrel

        # get meta information of stuff available through this source
        sub_squirrel = Squirrel(database=squirrel.get_database())
        fn = self._get_channels_path()
        if os.path.exists(fn):
            sub_squirrel.add([fn], check=False)

        nuts = sub_squirrel.get_nuts(
            'channel', constraint.tmin, constraint.tmax)

        file_path = self.source_id
        squirrel.add_virtual(
            (make_waveform_promise_nut(
                file_path=file_path,
                **nut.waveform_promise_kwargs) for nut in nuts),
            virtual_file_paths=[file_path])

    def _get_user_credentials(self):
        d = {}
        if self._user_credentials is not None:
            d['user'], d['passwd'] = self._user_credentials

        if self._auth_token is not None:
            d['token'] = self._auth_token

        return d

    def download_waveforms(self, squirrel, orders):
        orders.sort(key=lambda order: (order.codes, order.tmin))
        neach = 20
        i = 0
        while i < len(orders):
            orders_now = orders[i:i+neach]

            selection_now = []
            for order in orders_now:
                selection_now.append(
                    order.codes[1:5] + (order.tmin, order.tmax))

            selection_now = combine_selections(selection_now)

            with tempfile.NamedTemporaryFile() as f:
                try:
                    self._log_info_data(
                        'downloading, %s' % order_summary(orders_now))

                    data = fdsn.dataselect(
                        site=self._site, selection=selection_now,
                        **self._get_user_credentials())

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
                        'FDSN site "%s" for channels \n  %s' % (
                            self._site,
                            '\n  '.join(
                                '.'.join(x[:4]) for x in selection_now)))

            i += neach
