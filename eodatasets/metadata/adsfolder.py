# coding=utf-8
"""
Extract metadata from ADS directory names.
"""
from __future__ import absolute_import
import re

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

    if not md.acquisition:
        md.acquisition = ptype.AcquisitionMetadata()

    if not md.acquisition.platform_orbit:
        md.acquisition.platform_orbit = orbit

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
    m = re.search(r"(?P<sat>AQUA|TERRA|LANDSAT-\d|NPP\.VIIRS)\.(?P<orbit>\d+)(\.\w+)?", name)

    if m is None:
        return None

    fields = m.groupdict()
    return int(fields['orbit'])
