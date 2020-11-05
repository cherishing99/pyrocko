
import logging
from pyrocko import squirrel, trace

logger = logging.getLogger('pyrocko.squirrel.pile')


def trace_selector_to_nut_selector(trace_selector):
    if trace_selector is None:
        return None

    def nut_selector(nut):
        return trace_selector(nut.dummy_trace)

    return nut_selector


class Pile(object):
    def __init__(self):
        self._squirrel = squirrel.Squirrel()

    @property
    def tmin(self):
        return self._squirrel.get_time_span()[0]

    @property
    def tmax(self):
        return self._squirrel.get_time_span()[1]

    @property
    def networks(self):
        return set(codes[1] for codes in self._squirrel.get_codes('waveform'))

    @property
    def stations(self):
        return set(codes[2] for codes in self._squirrel.get_codes('waveform'))

    @property
    def locations(self):
        return set(codes[3] for codes in self._squirrel.get_codes('waveform'))

    @property
    def channels(self):
        return set(codes[4] for codes in self._squirrel.get_codes('waveform'))

    def is_relevant(self, tmin, tmax):
        ptmin, ptmax = self._squirrel.get_time_span()

        if None in (ptmin, ptmax):
            return False

        return tmax >= ptmin and ptmax >= tmin

    def load_files(
            self, filenames,
            filename_attributes=None,
            fileformat='mseed',
            cache=None,
            show_progress=True,
            update_progress=None):

        self._squirrel.add(
            filenames, kinds='waveform', format=fileformat)

    def chop(
            self, tmin, tmax,
            nut_selector=None,
            snap=(round, round),
            include_last=False,
            load_data=True,
            accessor_id='default'):

        nuts = self._squirrel.get_waveform_nuts(tmin=tmin, tmax=tmax)

        if load_data:
            traces = [
                self._squirrel.get_content(nut, accessor_id).pyrocko_trace()
                for nut in nuts if nut_selector is None or nut_selector(nut)]

        else:
            traces = [
                trace.Trace(**nut.trace_kwargs)
                for nut in nuts if nut_selector is None or nut_selector(nut)]

        self._squirrel.advance_accessor(accessor_id)

        chopped = []
        used_files = set()
        for tr in traces:
            if not load_data and tr.ydata is not None:
                tr = tr.copy(data=False)
                tr.ydata = None

            try:
                chopped.append(tr.chop(
                    tmin, tmax,
                    inplace=False,
                    snap=snap,
                    include_last=include_last))

            except trace.NoData:
                pass

        return chopped, used_files

    def _process_chopped(
            self, chopped, degap, maxgap, maxlap, want_incomplete, wmax, wmin,
            tpad):

        chopped.sort(key=lambda a: a.full_id)
        if degap:
            chopped = trace.degapper(chopped, maxgap=maxgap, maxlap=maxlap)

        if not want_incomplete:
            chopped_weeded = []
            for tr in chopped:
                emin = tr.tmin - (wmin-tpad)
                emax = tr.tmax + tr.deltat - (wmax+tpad)
                if (abs(emin) <= 0.5*tr.deltat and abs(emax) <= 0.5*tr.deltat):
                    chopped_weeded.append(tr)

                elif degap:
                    if (0. < emin <= 5. * tr.deltat and
                            -5. * tr.deltat <= emax < 0.):

                        tr.extend(
                            wmin-tpad,
                            wmax+tpad-tr.deltat,
                            fillmethod='repeat')

                        chopped_weeded.append(tr)

            chopped = chopped_weeded

        for tr in chopped:
            tr.wmin = wmin
            tr.wmax = wmax

        return chopped

    def chopper(
            self,
            tmin=None, tmax=None, tinc=None, tpad=0.,
            trace_selector=None,
            want_incomplete=True, degap=True, maxgap=5, maxlap=None,
            keep_current_files_open=False, accessor_id='default',
            snap=(round, round), include_last=False, load_data=True):

        '''
        Get iterator for shifting window wise data extraction from waveform
        archive.

        :param tmin: start time (default uses start time of available data)
        :param tmax: end time (default uses end time of available data)
        :param tinc: time increment (window shift time) (default uses
            ``tmax-tmin``)
        :param tpad: padding time appended on either side of the data windows
            (window overlap is ``2*tpad``)
        :param trace_selector: filter callback taking
            :py:class:`pyrocko.trace.Trace` objects
        :param want_incomplete: if set to ``False``, gappy/incomplete traces
            are discarded from the results
        :param degap: whether to try to connect traces and to remove gaps and
            overlaps
        :param maxgap: maximum gap size in samples which is filled with
            interpolated samples when ``degap`` is ``True``
        :param maxlap: maximum overlap size in samples which is removed when
            ``degap`` is ``True``
        :param keep_current_files_open: whether to keep cached trace data in
            memory after the iterator has ended
        :param accessor_id: if given, used as a key to identify different
            points of extraction for the decision of when to release cached
            trace data (should be used when data is alternately extracted from
            more than one region / selection)
        :param snap: replaces Python's :py:func:`round` function which is used
            to determine indices where to start and end the trace data array
        :param include_last: whether to include last sample
        :param load_data: whether to load the waveform data. If set to
            ``False``, traces with no data samples, but with correct
            meta-information are returned
        :returns: itererator yielding a list of :py:class:`pyrocko.trace.Trace`
            objects for every extracted time window
        '''

        if tmin is None:
            if self.tmin is None:
                logger.warning('Pile\'s tmin is not set - pile may be empty.')
                return
            tmin = self.tmin + tpad

        if tmax is None:
            if self.tmax is None:
                logger.warning('Pile\'s tmax is not set - pile may be empty.')
                return
            tmax = self.tmax - tpad

        if tinc is None:
            tinc = tmax - tmin

        if not self.is_relevant(tmin-tpad, tmax+tpad):
            return

        nut_selector = trace_selector_to_nut_selector(trace_selector)
        iwin = 0
        while True:
            chopped = []
            wmin, wmax = tmin+iwin*tinc, min(tmin+(iwin+1)*tinc, tmax)
            eps = tinc*1e-6
            if wmin >= tmax-eps:
                break

            chopped, used_files = self.chop(
                wmin-tpad, wmax+tpad, nut_selector, snap,
                include_last, load_data, accessor_id)

            processed = self._process_chopped(
                chopped, degap, maxgap, maxlap, want_incomplete, wmax, wmin,
                tpad)

            yield processed

            iwin += 1

        if not keep_current_files_open:
            self._squirrel.clear_accessor(accessor_id, 'waveform')

    def reload_modified(self):
        self._squirrel.reload()

    def iter_traces(self):
        for nut in self._squirrel.get_waveform_nuts():
            yield trace.Trace(**nut.trace_kwargs)


def get_cache(_):
    return None
