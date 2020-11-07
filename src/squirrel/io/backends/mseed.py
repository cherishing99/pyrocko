from __future__ import absolute_import, print_function

from pyrocko.io.io_common import get_stats  # noqa
from ... import model


def provided_formats():
    return ['mseed']


def detect(first512):
    from pyrocko import mseed

    if mseed.detect(first512):
        return 'mseed'
    else:
        return None


def iload(format, file_path, segment, content):
    assert format == 'mseed'

    from pyrocko import mseed

    load_data = 'waveform' in content

    for itr, tr in enumerate(mseed.iload(file_path, load_data=load_data)):

        nut = model.make_waveform_nut(
            file_segment=0,
            file_element=itr,
            agency='',
            network=tr.network,
            station=tr.station,
            location=tr.location,
            channel=tr.channel,
            tmin=tr.tmin,
            tmax=tr.tmax,
            deltat=tr.deltat)

        if 'waveform' in content:
            nut.content = model.Waveform(
                data=tr.ydata,
                **nut.waveform_kwargs)

        yield nut
