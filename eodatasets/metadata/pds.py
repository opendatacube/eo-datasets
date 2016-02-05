# coding=utf-8
"""
PDF file metadata extraction.
"""
from __future__ import absolute_import

import datetime
import logging
import re
from subprocess import check_output

from pathlib import Path

import eodatasets.type as ptype
from eodatasets.verify import find_exe

_LOG = logging.getLogger(__name__)

SATELLITE_ID_MAP = {
    "042": "TERRA",
    "154": "AQUA"
}

APID_CODE = {
    '0064': 'modis',
    # GBAD required for Aqua processing.
    '0957': 'gbad'
}


def _pds_satellite(file_name):
    """

    >>> _pds_satellite('P0420064AAAAAAAAAAAAAA09120155305001')
    'TERRA'
    >>> _pds_satellite('P1540064AAAAAAAAAAAAAA14219032341001')
    'AQUA'
    """
    return SATELLITE_ID_MAP.get(file_name[1:4])


def _pds_date(file_name):
    """
    :param file_name:
    :return:

    >>> _pds_date('P0420064AAAAAAAAAAAAAA09120155305001')
    datetime.datetime(2009, 4, 30, 15, 53, 5)
    >>> _pds_date('P1540064AAAAAAAAAAAAAA14219032341001')
    datetime.datetime(2014, 8, 7, 3, 23, 41)
    """
    return datetime.datetime.strptime(file_name[-14: -3], '%y%j%H%M%S')


def extract_md(base_md, directory_path):
    """
    Extract metadata from a directory of PDF files


    :type base_md: ptype.DatasetMetadata
    :type directory_path: pathlib.Path
    :rtype: ptype.DatasetMetadata
    """
    pds_file = find_pds_file(directory_path)
    if not pds_file:
        _LOG.debug('No PDS files found')
        return base_md

    # Extract PDS info.
    _LOG.info('Using PDS file %r', pds_file)

    base_md.format_ = ptype.FormatMetadata(name='PDS')

    base_md.platform = ptype.PlatformMetadata(code=_pds_satellite(pds_file.stem))
    base_md.instrument = ptype.InstrumentMetadata(name='MODIS')

    if not base_md.acquisition:
        base_md.acquisition = ptype.AcquisitionMetadata()

    base_md.acquisition.aos = _pds_date(pds_file.stem)

    start, end, day, night = get_pdsinfo(pds_file)

    base_md.acquisition.aos = start
    base_md.acquisition.los = end

    if not base_md.image:
        base_md.image = ptype.ImageMetadata()

    base_md.image.day_percentage_estimate = (float(day) / (day + night)) * 100.0

    return base_md


def find_pds_file(path):
    if (not path.is_dir()) and is_modis_pds_file(path):
        return path

    pds_files = list(filter(is_modis_pds_file, path.iterdir()))

    if not pds_files:
        return None

    if len(pds_files) > 1:
        _LOG.warning('Multiple PDS files founds %s', pds_files)

    return pds_files[0]


def is_modis_pds_file(file_path):
    """
    Is this an PDS file name with a MODIS APID?
    >>> is_modis_pds_file(Path('P0420064AAAAAAAAAAAAAA09120155305001.PDS'))
    True
    >>> is_modis_pds_file(Path('/tmp/something/P1540064AAAAAAAAAAAAAA14219032341001.PDS'))
    True
    >>> is_modis_pds_file(Path('P1540064AAAAAAAAAAAAAA14219032341001.PDS.MD5'))
    False
    >>> is_modis_pds_file(Path('.tmp-P1540064AAAAAAAAAAAAAA14219032341001.PDS'))
    False
    >>> # The '*000.PDS' files should be ignored.
    >>> is_modis_pds_file(Path('P1540064AAAAAAAAAAAAAA14219032341000.PDS'))
    False
    >>> # GBAD file.
    >>> is_modis_pds_file(Path('P1540957AAAAAAAAAAAAAA14219032341001.PDS'))
    False

    :type file_path: pathlib.Path
    :rtype: bool
    """
    # 0064 is modis packet APID.
    return bool(
        re.match(
            r"^P\d{3}0064AAAAAAAAAAAAAA\d{2}\d{3}\d{2}\d{2}\d{2}001\.PDS$",
            file_path.name
        )
    )


def _run_pdsinfo_exe(pds_path):
    """
    :param pathlib.Path pds_path: PDS filepath.
    :return str: raw output of pdsinfo
    """
    return check_output(
        [
            find_exe('pdsinfo'),
            str(pds_path.absolute())
        ]
    )


def get_pdsinfo(pds_path):
    out = _run_pdsinfo_exe(pds_path)
    if not out:
        raise Exception('No output from pdsinfo')

    vals = dict([d.strip().split(': ') for d in out.decode('utf-8').splitlines() if d.strip()])

    start_date = datetime.datetime.strptime(
        vals['first packet'],
        "%Y/%m/%d %H:%M:%S.%f"
    )
    end_date = datetime.datetime.strptime(
        vals['last packet'],
        "%Y/%m/%d %H:%M:%S.%f"
    )
    day_packets = int(vals['day packets'].split('/')[0])
    night_packets = int(vals['night packets'].split('/')[0])

    return start_date, end_date, day_packets, night_packets
