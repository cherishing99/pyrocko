import logging

import numpy as num

from pyrocko import trace
from .io_common import FileLoadError

logger = logging.getLogger(__name__)


def get_meta(tdms_properties):
    deltat = 1. / tdms_properties['SamplingFrequency[Hz]']
    tmin = tdms_properties['GPSTimeStamp'].astype(num.float) * 1e-6

    fibre_extra = {
        'fibre_length': tdms_properties.get(
            'MeasureLength[m]', -1.),
        'fibre_spatial_resolution': tdms_properties.get(
            'SpatialResolution[m]', -1.),
        'fibre_index': tdms_properties.get(
            'FibreIndex', -1.),
        'zero_offset': tdms_properties.get(
            'Zero Offset (m)', -1.),
    }

    extra = ', '.join(['%s: %s' % (k, v) for k, v in fibre_extra.items()])

    return deltat, tmin, extra


def iload(filename, load_data=True):
    try:
        from nptdms import TdmsFile
    except ImportError:
        raise FileLoadError('Could not import Python module nptdms.')

    try:
        if load_data:
            tdms = TdmsFile.read(filename)
        else:
            tdms = TdmsFile.read_metadata(filename)
    except ValueError as e:
        raise FileLoadError('Cannot load %s: %s' % (filename, str(e)))

    deltat, tmin, extra_str = get_meta(tdms.properties)

    for group in tdms.groups():
        for channel in group.channels():

            assert int(channel.name) < 99999
            station = '%05i' % int(channel.name)
            nsamples = channel._length

            tr = trace.Trace(
                network='DA',
                station=station,
                ydata=None,
                deltat=deltat,
                tmin=tmin,
                tmax=tmin + nsamples*deltat,
                extra=extra_str)

            if load_data:
                tr.set_ydata(channel[:].astype(num.int32))

            yield tr


def detect(first512):
    return first512.startswith(b'TDSm.')
