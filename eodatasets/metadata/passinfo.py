# coding=utf-8
"""
Metadata extraction from passinfo files.
"""
from __future__ import absolute_import
import logging
import datetime

from eodatasets import type as ptype


_log = logging.getLogger(__name__)


def extract_md(base_md, directory):
    """
    Extract metadata from a passinfo file if one exists.

    :type base_md: ptype.DatasetMetadata
    :type directory: pathlib.Path
    :rtype: ptype.DatasetMetadata
    """

    passinfos = list(directory.glob('passinfo*'))
    passinfos.extend(directory.parent.glob('passinfo*'))

    if not passinfos:
        _log.debug('No passinfo file found')
        return base_md

    if len(passinfos) > 1:
        _log.warn('Multiple passinfo files in directory: %r', passinfos)

    passinfo = passinfos[0]
    _log.info("Found passinfo '%s'", passinfo)

    try:
        with passinfo.open() as f:
            lines = f.readlines()
    except ValueError:
        _log.exception("Exception reading passinfo '%s'", passinfo)
        return None

    return _parse_passinfo_md(base_md, lines)


def station_to_gsi(station):
    if station == 'ALICE':
        gsi = 'ASA'
    elif station == 'TERSS':
        # Hobart
        gsi = 'HOA'
    else:
        _log.warn("Unknown station value %r. Falling back to RCC extraction.", station)
        gsi = None
    return gsi


def standardise_satellite(satellite_code):
    """

    :type satellite_code: str
    :rtype: str

    >>> standardise_satellite('LANDSAT-5')
    'LANDSAT_5'
    """
    if not satellite_code:
        return None

    return satellite_code.upper().replace('-', '_')


def _parse_passinfo_md(base_md, lines):
    """
    :type base_md: ptype.DatasetMetadata
    :type lines: list of str
    :rtype: ptype.DatasetMetadata
    """
    fields = {}
    for l in lines:
        tmp = l.split()
        if len(tmp) >= 2:
            k, v = tmp[:2]
            fields[k.upper()] = v.upper()

    if not base_md.platform:
        base_md.platform = ptype.PlatformMetadata()
        base_md.platform.code = standardise_satellite(fields.get('SATELLITE'))

    if not base_md.instrument:
        base_md.instrument = ptype.InstrumentMetadata()
        base_md.instrument.name = fields.get('SENSOR')

    if not base_md.acquisition:
        base_md.acquisition = ptype.AcquisitionMetadata()

    base_md.acquisition.platform_orbit = int(fields.get('ORBIT'))
    start_dt = _parse_common_date(fields.get('START'))
    if start_dt:
        base_md.acquisition.aos = start_dt

    stop_dt = _parse_common_date(fields.get('STOP'))
    if stop_dt:
        base_md.acquisition.los = stop_dt

    if not base_md.acquisition.groundstation:
        gsi = station_to_gsi(fields.get('STATION'))
        if gsi is not None:
            base_md.acquisition.groundstation = ptype.GroundstationMetadata(code=gsi)

    return base_md


def _parse_common_date(date_str):
    """
    :type date_str: str
    :rtype: struct_time or None
    """
    if date_str:
        return datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
    else:
        return None
