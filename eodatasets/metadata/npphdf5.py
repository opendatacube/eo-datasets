# coding=utf-8
"""
Metadata extraction from hdf files.
"""

from __future__ import absolute_import
import datetime
import logging
import re

import eodatasets.type as ptype

_LOG = logging.getLogger(__name__)


def extract_md(base_md, directory_path):
    """
    Extract metadata from an NPP HDF5 filename if one exists.

    The NPP directory should contain VIRS ".h5" (HDF5) data file from which we can get the date
    The filename will be of the form:
        NPP: RNSCA-RVIRS_npp_d20130422_t0357358_e0410333_b07686_c20130422041225898000_nfts_drl.h5

    where:
        d: start date (YYYMMDD)
        t: start time (hhmmss.s)
        e:
        b: orbit number
        c: stop date/time (YYYMMDDhhmmss.ssssss)

    :type base_md: ptype.DatasetMetadata
    :type directory_path: pathlib.Path
    :rtype: ptype.DatasetMetadata
    """

    files = find_hdf5_files(directory_path)

    if len(files) < 1:
        _LOG.debug("No NPP HDF5 file found")
        return base_md

    filename = files[0].name

    base_md = _extract_hdf5_filename_fields(base_md, filename)

    if not base_md.acquisition:
        base_md.acquisition = ptype.AcquisitionMetadata()

    # HDF5 is raw: P00
    base_md.ga_level = 'P00'
    base_md.format_ = ptype.FormatMetadata(name='HDF5')

    return base_md


def find_hdf5_files(directory):
    """
    Find HDF5 files in the given directory

    :type directory: pathlib.Path
    :return: List of HDF5 file paths (usually only one)
    :rtype: list[pathlib.Path]
    """
    return list(directory.glob('RNSCA-RVIRS_npp*.h5'))


def _extract_hdf5_filename_fields(base_md, filename):
    """
    NPP VIRS format specifications:
        ???


    :type base_md: ptype.DatasetMetadata
    :type filename: str
    :rtype: ptype.DatasetMetadata
    """
    m = re.search(r'(?P<satsens>.{15})'
                  r'_d(?P<date>\d{8})'
                  r'_t(?P<startTime>\d{7})'
                  r'_e(?P<endTime>\d{7})'
                  r'_b(?P<orbit>\d{5})'
                  r'_c(?P<enddatetime>\d{20})'
                  r'_nfts_drl.h5', filename)
    fields = m.groupdict()

    satellite, sensor = _split_sat_sen(fields['satsens'])

    if satellite:
        base_md.platform = ptype.PlatformMetadata(code=satellite)

    if sensor:
        base_md.instrument = ptype.InstrumentMetadata(name=sensor)

    if not base_md.acquisition:
        base_md.acquisition = ptype.AcquisitionMetadata()

    start_time = fields['date'] + fields['startTime']
    base_md.acquisition.aos = datetime.datetime.strptime(start_time[:-1], "%Y%m%d%H%M%S")
    base_md.acquisition.los = datetime.datetime.strptime(fields['enddatetime'][:14], "%Y%m%d%H%M%S")

    if int(fields['orbit']) > 0:
        base_md.acquisition.platform_orbit = int(fields['orbit'])

    return base_md


def _split_sat_sen(fields_satsens_):
    """

    :param fields_satsens_:
    :return:
    >>> _split_sat_sen("RNSCA-RVIRS_npp")
    ('NPP', 'VIIRS')
    """
    satellite = None
    sensor = None
    # TODO: Do we have a cleaner way to do this? A list of mappings?
    satsen = fields_satsens_.split('-')
    if satsen:
        sat_sen_prefixed = satsen[-1].upper()
        sat_sen = sat_sen_prefixed[1:]
        try:
            sensor, satellite = sat_sen.split('_')
        except ValueError:
            # More than two values after splitting.
            _LOG.error('Unknown NPP satellite-sensor combination: %s', sat_sen)

    # Remove shorthand. TODO: An alias map?
    if sensor == 'VIRS':
        sensor = 'VIIRS'

    return satellite, sensor
