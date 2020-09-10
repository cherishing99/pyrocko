from __future__ import absolute_import, print_function

from pyrocko.io.io_common import get_stats, touch  # noqa
from ... import model


def provided_formats():
    return ['datacube']


def detect(first512):
    from pyrocko import datacube

    if datacube.detect(first512):
        return 'datacube'
    else:
        return None


def iload(format, file_path, segment, content):
    assert format == 'datacube'

    from pyrocko import datacube

    load_data = 'waveform' in content

    for itr, tr in enumerate(datacube.iload(file_path, load_data=load_data)):

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
