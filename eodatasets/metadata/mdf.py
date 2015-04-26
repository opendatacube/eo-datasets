"""
Metadata extraction from MDF files.
"""
from __future__ import absolute_import
import calendar
import datetime
import logging
import re
import time

import eodatasets.type as ptype


LS8_SENSORS = {"C": "OLI_TIRS", "O": "OLI", "T": "TIRS"}

_log = logging.getLogger(__name__)


def _before_underscore(s):
    """
    :type s: str
    :rtype: str

    >>> _before_underscore('LC80880750762013254ASA00_IDF.xml')
    'LC80880750762013254ASA00'
    >>> _before_underscore('LC80880750762013254ASA00_MD5.txt')
    'LC80880750762013254ASA00'
    >>> _before_underscore('383.000.2013137232105971.ASA')
    >>> _before_underscore('_MD5.txt')
    ''
    """
    if '_' not in s:
        return None

    return s.split('_')[0]


def extract_md(base_md, directory_path):
    """
    Extract metadata from a directory of MDF files

    From http://landsat.usgs.gov/documents/LDCM-DFCB-001.pdf the MDF directory will have a name of the form...

        VINpppRRRrrrYYYYdddGSIvv

    where
        V    = the vehicle (L=Landsat)
        I    = instrument (O=OLI T=TIRS C=combined OLI/TIRS)
        N    = vehicle number (8 = Landsat 8)
        ppp  = WRS-2 starting path (001-233)
        RRR  = WRS-2 starting row (001-248)
        rrr  = WRS-2 ending row (001-248)
        YYYY = Acquisition starting year
        DOY  = Acquisition starting day of year
        GSI  = Ground station identifier (e.g. ASA)
        vv   = version (00-99)

    for example LC80850800822013137ASA00

    :type base_md: ptype.DatasetMetadata
    :type directory_path: Path
    :rtype: ptype.DatasetMetadata
    """

    directory_path, files = find_mdf_files(directory_path)

    if len(files) < 1:
        _log.debug("No MDF files found")
        return base_md

    usgs_id = directory_path.name if directory_path else None

    if not usgs_id:
        # Look at siblings of the mdf files.
        for f in list(files)[0].parent.iterdir():
            prefix = _before_underscore(f.name)
            if is_mdf_usgs_id(prefix):
                _log.info('Found usgs id %r', usgs_id)
                usgs_id = prefix

    if not usgs_id:
        _log.debug('No MDF id matched. Assuming not MDF.')
        return base_md

    _log.info("Found MDF files %r in directory %r", files, directory_path)

    if usgs_id:
        base_md = _extract_mdf_id_fields(base_md, usgs_id)

    if files:
        base_md = _extract_mdf_file_fields(base_md, [f.name for f in files])

    base_md.product_type = 'RAW'
    base_md.ga_level = 'P00'

    if not base_md.format_:
        base_md.format_ = ptype.FormatMetadata()
    base_md.format_.name = 'MDF'

    return base_md


def _extract_mdf_id_fields(base_md, mdf_usgs_id):
    """
    V I N ppp RRR rrr YYYY ddd GSI vv
    :type base_md: ptype.DatasetMetadata
    :type mdf_usgs_id: str
    :rtype: ptype.DatasetMetadata
    """
    m = re.search(
        "(?P<vehicle>L)" +
        "(?P<instrument>[OTC])" +
        "(?P<vehicle_number>\d)" +
        "(?P<path>\d{3})" +
        "(?P<row_start>\d{3})" +
        "(?P<row_end>\d{3})" +
        "(?P<acq_date>\d{7})" +
        "(?P<gsi>\w{3})" +
        "(?P<version>\d{2})",
        mdf_usgs_id)

    fields = m.groupdict()

    base_md.usgs_dataset_id = mdf_usgs_id

    if not base_md.platform:
        base_md.platform = ptype.PlatformMetadata()

    base_md.platform.code = "LANDSAT_" + fields["vehicle_number"]

    if not base_md.instrument:
        base_md.instrument = ptype.InstrumentMetadata()
    base_md.instrument.name = LS8_SENSORS[fields["instrument"]]

    path = int(fields["path"])

    if not base_md.image:
        base_md.image = ptype.ImageMetadata()
    base_md.image.satellite_ref_point_start = ptype.Point(path, int(fields["row_start"]))
    base_md.image.satellite_ref_point_end = ptype.Point(path, int(fields["row_end"]))

    # base_md.version = int(fields["version"]) or base_md.version

    # Probably less than useful without time.
    # if not base_md.extent:
    #     base_md.extent = ptype.ExtentMetadata()
    #
    # base_md.extent.center_dt = base_md.extent.center_dt or datetime.strptime(fields["acq_date"], "%Y%j").date()

    if not base_md.acquisition:
        base_md.acquisition = ptype.AcquisitionMetadata()

    base_md.acquisition.groundstation = ptype.GroundstationMetadata(code=fields["gsi"])

    return base_md


def _extract_mdf_file_fields(base_md, mdf_file_names):
    """
    From http://landsat.usgs.gov/documents/LDCM-DFCB-001.pdf...

        RRR.ZZZ.YYYYDOYHHMMSS.sss.XXX

      where
        RRR  = the root file directory number on the SSR the data was stored (001-511)
        ZZZ  = the sequence (or sub-file) of the file within the root file (000-127)
        YYYY = the year the data was received (2012-2999)
        DOY  = the day of the year the data was received (001-366)
        HH   = the hour of the day the data was received (00-23)
        MM   = the minute of the hour the data was received (00-59)
        SS   = the second of the minute the data was received (00-60)
        sss  = the fraction of the second the data was received (000-999)
        XXX  = the ground station identifier (e.g. ASA)

      for example 383.000.2013137232105971.ASA

    :type base_md: ptype.DatasetMetadata
    :type mdf_file_names: list of str
    :rtype: ptype.DatasetMetadata
    """

    times = []

    for f_name in mdf_file_names:
        m = re.search(
            "(?P<root_file_number>\d{3})" +
            "\." +
            "(?P<root_file_sequence>\d{3})" +
            "\." +
            "(?P<date_time>\d{16})" +
            "\." +
            "(?P<gsi>\w{3})",
            f_name)

        fields = m.groupdict()

        t = datetime.datetime.strptime(fields["date_time"], "%Y%j%H%M%S%f")

        times.append(t)

    # TODO: This calculation comes from the old jobmanger code. Is it desirable?
    start = min(times) - datetime.timedelta(seconds=60)  # 60 seconds before first segment's acquisition complete time
    stop = max(times)  # the last segment's acquisition complete time

    if not base_md.acquisition:
        base_md.acquisition = ptype.AcquisitionMetadata()
    base_md.acquisition.aos = base_md.acquisition.aos or start
    base_md.acquisition.los = base_md.acquisition.los or stop

    return base_md


def adjust_time(t, delta):
    """
    Adjust a (UTC) struct_time by delta seconds

    :type t: struct_time
    :type delta: int
    :param delta: seconds
    :rtype: struct_time
    """
    return time.gmtime(calendar.timegm(t) + delta)


def is_mdf_usgs_id(name):
    """
    Is this an MDF directory name?
    :type directory: str or unicode or None
    :rtype: bool

    >>> is_mdf_usgs_id('LC80920740862013090LGN00')
    True
    >>> is_mdf_usgs_id('LC81070620632013228ASA00')
    True
    >>> is_mdf_usgs_id('NPP.VIIRS.7686.ALICE')
    False
    >>> is_mdf_usgs_id('TERRA.72239.S1A2C2D4R4')
    False
    >>> is_mdf_usgs_id('TERRA.72239.S1A2C2D4R4')
    False
    >>> is_mdf_usgs_id('LANDSAT-8.1725')
    False
    >>> is_mdf_usgs_id('LC80910760902013148ASA00')
    True
    >>> is_mdf_usgs_id('133.004.2013148000120310.ASA')
    False
    >>> is_mdf_usgs_id('LC80910760902013148ASA00_IDF.xml')
    False
    >>> is_mdf_usgs_id(None)
    False
    """
    if not name:
        return False
    return bool(re.match("^L[OTC]\d{17}[A-Z]{3}\d{2}$", name))


def is_mdf_file(filename):
    """
    Is this an MDF file name?
    >>> is_mdf_file('132.000.2013148000123679.ASA')
    True
    >>> is_mdf_file('133.004.2013148000120310.ASA')
    True
    >>> is_mdf_file('LC80910760902013148ASA00_MD5.txt')
    False
    >>> is_mdf_file('LC80910760902013148ASA00_IDF.xml')
    False
    >>> is_mdf_file('LC80910760902013148ASA00')
    False
    >>> is_mdf_file('133.004.2013148000120310.ASA.orig')
    False

    :type filename: str or unicode
    :rtype: bool
    """
    return bool(re.match("^\d{3}\.\d{3}.\d{16}.[A-Z]{3}$", filename))


def find_mdf_files(directory):
    """
    Find a MDF directory and list of matching mdf files.
    :type directory: pathlib.Path
    :rtype: (pathlib.Path, [pathlib.Path])
    """
    mdf_dir = None

    def _get_mdf_files(mdf_dir):
        return {f for f in mdf_dir.iterdir() if is_mdf_file(f.name)}

    # Were we given the MDF directory itself?
    if is_mdf_usgs_id(directory.name):
        mdf_dir = directory
        return mdf_dir, _get_mdf_files(mdf_dir)

    # Is there a single MDF sub-directory?
    mdf_subdirs = [d for d in directory.iterdir() if is_mdf_usgs_id(d.name)]
    if mdf_subdirs and len(mdf_subdirs) == 1:
        return mdf_subdirs[0], _get_mdf_files(mdf_subdirs[0])

    # Otherwise we may have an unconventional NCI structure...
    # Eg.
    #      LC80910760902013148ASA00/
    #           input/
    #               132.000.2013148000123679.ASA
    #               132.000.2013148000123679.ASA
    #               ...
    #           lpgsOut/
    #               ...
    #
    # Note that the MDF directory doesn't directly contain the MDF files.
    #  -> And there's usually a lot of sub directories containing other outputs which we don't want to touch.

    # Does our given folder directly contain mdf files? If so, look for parent mdf directories.
    mdf_files = _get_mdf_files(directory)

    if mdf_files:
        if is_mdf_usgs_id(directory.parent.name):
            mdf_dir = directory.parent

        if is_mdf_usgs_id(directory.parent.parent.name):
            mdf_dir = directory.parent.parent

    return mdf_dir, mdf_files

