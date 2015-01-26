from __future__ import absolute_import, print_function

import time
import os
import copy
import logging
try:
    import cPickle as pickle
except ImportError:
    import pickle
import os.path as op
from .base import Source, Constraint
from ..model import WaveformPromise, make_waveform_promise_nut, ehash
from pyrocko.client import fdsn

from pyrocko import config, util
from pyrocko.io_common import FileLoadError

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


class FDSNSource(Source):

    def __init__(
            self, site,
            user_credentials=None,
            auth_token=None,
            auth_token_path=None,
            query_args=None,
            noquery_age_max=3600.,
            cache_dir=None):

        Source.__init__(self)

        self._site = site
        self._constraint = None
        self._noquery_age_max = noquery_age_max

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
        self._cache_dir = op.join(
            cache_dir or config.config().cache_dir,
            'fdsn',
            self._hash)

        util.ensuredir(self._cache_dir)
        self._load_constraint()

    def get_channel_file_paths(self):
        return [op.join(self._cache_dir, 'channels.stationxml')]

    def update_channel_inventory(self, squirrel, constraint=None):
        if constraint is None:
            constraint = Constraint()

        if self._constraint and self._constraint.contains(constraint) \
                and not self._stale_channel_inventory():

            logger.info(
                'using cached channel information for site %s'
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

            if diff(fn, fn_temp):
                os.rename(fn_temp, fn)
                logger.info('changed: %s' % fn)
            else:
                logger.info('no change: %s' % fn)
                os.unlink(fn_temp)

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
            'querying channel information from site %s'
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
        for file_path in self.get_channel_file_paths():
            try:
                t = os.stat(file_path)[8]
                return t < time.time() - self._noquery_age_max
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

        file_path = 'client:fdsn:%s' % self._hash
        squirrel.add_virtual(
            (make_waveform_promise_nut(
                file_path=file_path,
                **nut.waveform_promise_kwargs) for nut in nuts),
            virtual_file_paths=[file_path])
