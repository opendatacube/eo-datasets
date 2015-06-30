# coding=utf-8
"""
Extract metadata from ADS directory names.
"""
from __future__ import absolute_import
import re
from eodatasets import metadata

import eodatasets.type as ptype


def extract_md(md, directory):
    """
    Extract metadata from typical ads3 directory names.

    Folder names contain orbit numbers.

    Eg:
        LANDSAT-7.76773.S3A1C2D2R2
        AQUA.60724.S1A1C2D2R2
        TERRA.73100.S1A2C2D4R4
        LANDSAT-8.3108
        NPP.VIIRS.10014.ALICE

    :type md: ptype.DatasetMetadata
    :type directory: pathlib.Path
    :rtype: ptype.DatasetMetadata
    """

    directory = directory.absolute()
    parent_dir = directory.parent

    orbit = _extract_orbit(directory.name) or _extract_orbit(parent_dir.name)
    rms_string = _extract_rms_string(directory.name) or _extract_rms_string(parent_dir.name)
    gsi = _extract_gsi(directory.name) or _extract_gsi(parent_dir.name)

    if rms_string:
        md.rms_string = rms_string

    if not md.acquisition:
        md.acquisition = ptype.AcquisitionMetadata()

    if not md.acquisition.platform_orbit:
        md.acquisition.platform_orbit = orbit

    if not md.acquisition.groundstation and gsi:
        md.acquisition.groundstation = ptype.GroundstationMetadata(code=gsi)

    return md


def _extract_orbit(name):
    """
    Extract orbit number from ads file conventions

    >>> _extract_orbit('LANDSAT-7.76773.S3A1C2D2R2')
    76773
    >>> _extract_orbit('AQUA.60724.S1A1C2D2R2')
    60724
    >>> _extract_orbit('TERRA.73100.S1A2C2D4R4')
    73100
    >>> _extract_orbit('LANDSAT-8.3108')
    3108
    >>> _extract_orbit('NPP.VIIRS.10014.ALICE')
    10014
    >>> _extract_orbit('not_an_ads_dir')
    >>> _extract_orbit('LANDSAT-8.FAKE')
    """
    return _extract_sat_orbit_string(name)[1]


def _extract_rms_string(name):
    """
    Extract orbit number from ads file conventions

    >>> _extract_rms_string('LANDSAT-7.76773.S3A1C2D2R2')
    'S3A1C2D2R2'
    >>> _extract_rms_string('AQUA.60724.S1A1C2D2R2')
    'S1A1C2D2R2'
    >>> _extract_rms_string('TERRA.73100.S1A2C2D4R4')
    'S1A2C2D4R4'
    >>> _extract_rms_string('LANDSAT-8.3108')
    >>> _extract_rms_string('NPP.VIIRS.10014.ALICE')
    >>> _extract_rms_string('not_an_ads_dir')
    >>> _extract_rms_string('LANDSAT-8.FAKE')
    """
    return _extract_sat_orbit_string(name)[2]


def _extract_gsi(name):
    """
    Extract a normalised groundstation if available.
    :param name:
    :rtype: str

    >>> _extract_gsi('LANDSAT-7.76773.S3A1C2D2R2')
    >>> _extract_gsi('AQUA.60724.S1A1C2D2R2')
    >>> _extract_gsi('TERRA.73100.S1A2C2D4R4')
    >>> _extract_gsi('LANDSAT-8.3108')
    >>> _extract_gsi('NPP.VIIRS.10014.ALICE')
    'ASA'
    >>> _extract_gsi('NPP_VIRS_STD-HDF5_P00_18966.ASA_0_0_20150626T053709Z20150626T055046')
    'ASA'
    >>> _extract_gsi('not_an_ads_dir')
    >>> _extract_gsi('LANDSAT-8.FAKE')
    """
    last_component = name.split('.')[-1]
    if '_' in last_component:
        last_component = last_component.split('_')[0]
    if not metadata.is_groundstation_alias(last_component):
        return None

    return metadata.normalise_gsi(last_component)


def _extract_sat_orbit_string(name):
    m = re.search(r"^(?P<sat>AQUA|TERRA|LANDSAT-\d|NPP\.VIIRS)\."
                  r"(?P<orbit>\d+)"
                  r"(\.(?P<rmsstring>S\dA\d[A-Z0-9]+))?", name)

    if m is None:
        return None, None, None

    fields = m.groupdict()

    sat = fields['sat']
    orbit = int(fields['orbit']) if 'orbit' in fields else None
    rms_string = fields['rmsstring']

    return sat, orbit, rms_string
