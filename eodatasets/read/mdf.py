"""
Metadata extraction from MDF files.
"""
import calendar
from datetime import datetime
import logging
import os
import re
import time

import eodatasets.type as ptype

_log = logging.getLogger(__name__)


def extract_md(base_md, directory):
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

    :type base_md: PassMetadata
    :type directory: str
    :rtype: PassMetadata
    """

    directory, files = find_mdf_files(directory)

    if not directory or len(files) < 1:
        _log.debug("No MDF data found")
        return base_md

    _log.info("Found MDF files %r in directory %r", files, directory)

    return _extract_mdf_file_data(base_md, directory, files)


def _extract_mdf_file_data(base_md, directory, files):
    """

    :type base_md: ptype.DatasetMetadata
    :param directory:
    :param files:
    :return:
    """
    base_md = _extract_mdf_directory_fields(base_md, directory)
    base_md = _extract_mdf_file_fields(base_md, files)

    base_md.product_type = 'RAW'
    base_md.ga_level = 'P00'

    if not base_md.format_:
        base_md.format_ = ptype.FormatMetadata()
    base_md.format_.name = 'MD'

    return base_md


def _extract_mdf_directory_fields(base_md, directory):
    """
    V I N ppp RRR rrr YYYY ddd GSI vv
    :type base_md: ptype.DatasetMetadata
    :type directory: str
    :rtype: ptype.DatasetMetadata
    """
    SENSORS = {"C": "OLI_TIRS", "O": "OLI", "T": "TIRS"}

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
        directory)

    fields = m.groupdict()

    base_md.usgs_dataset_id = os.path.basename(directory)

    if not base_md.platform:
        base_md.platform = ptype.PlatformMetadata()

    base_md.platform.code = "LANDSAT_" + fields["vehicle_number"]

    if not base_md.instrument:
        base_md.instrument = ptype.InstrumentMetadata()
    base_md.instrument.name = SENSORS[fields["instrument"]]

    path = int(fields["path"])

    if not base_md.image:
        base_md.image = ptype.ImageMetadata()
    base_md.image.satellite_ref_point_start = ptype.Point(path, int(fields["row_start"]))
    base_md.image.satellite_ref_point_end = ptype.Point(path, int(fields["row_end"]))

    # base_md.version = int(fields["version"]) or base_md.version

    if not base_md.extent:
        base_md.extent = ptype.ExtentMetadata()

    base_md.extent.center_dt = base_md.extent.center_dt or datetime.strptime(fields["acq_date"], "%Y%j").date()

    if not base_md.acquisition:
        base_md.acquisition = ptype.AcquisitionMetadata()

    base_md.acquisition.groundstation = ptype.GroundstationMetadata(code=fields["gsi"])

    return base_md


def _extract_mdf_file_fields(base_md, files):
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
    :type files: list of str
    :rtype: ptype.DatasetMetadata
    """

    times = []

    for f in files:
        m = re.search(
            "(?P<root_file_number>\d{3})" +
            "\." +
            "(?P<root_file_sequence>\d{3})" +
            "\." +
            "(?P<date_time>\d{13})" +
            "(?P<date_time_ms>\d{3})" +
            "\." +
            "(?P<gsi>\w{3})",
            f)

        fields = m.groupdict()

        t = time.strptime(fields["date_time"], "%Y%j%H%M%S")

        times.append(t)

    start = adjust_time(min(times), -60)  # 60 seconds before first segment's acquisition complete time
    stop = max(times)  # the last segment's acquisition complete time

    base_md.acquisition.aos = base_md.acquisition.aos or datetime.fromtimestamp(time.mktime(start))
    base_md.acquisition.los = base_md.acquisition.los or datetime.fromtimestamp(time.mktime(stop))

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


def is_mdf_directory(directory):
    """
    Is this an MDF directory name?
    :type directory: str or unicode
    :rtype: bool

    >>> is_mdf_directory('LC80920740862013090LGN00')
    True
    >>> is_mdf_directory('LC81070620632013228ASA00')
    True
    >>> is_mdf_directory('NPP.VIIRS.7686.ALICE')
    False
    >>> is_mdf_directory('TERRA.72239.S1A2C2D4R4')
    False
    >>> is_mdf_directory('TERRA.72239.S1A2C2D4R4')
    False
    >>> is_mdf_directory('LANDSAT-8.1725')
    False
    >>> is_mdf_directory('LC80910760902013148ASA00')
    True
    >>> is_mdf_directory('133.004.2013148000120310.ASA')
    False
    >>> is_mdf_directory('LC80910760902013148ASA00_IDF.xml')
    False

    """
    return bool(re.match("^L[OTC]\d{17}[A-Z]{3}\d{2}$", directory))


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
    :rtype: (str, [str])
    """
    mdf_dir = None
    mdf_files = None

    # Were we given the MDF directory itself?
    if is_mdf_directory(os.path.basename(directory)):
        mdf_dir = directory

    # Is there a single MDF sub-directory?
    else:
        dirs = [d for d in os.listdir(directory) if is_mdf_directory(d)]
        if dirs and len(dirs) == 1:
            mdf_dir = os.path.join(directory, dirs[0])

    if mdf_dir:
        mdf_files = [f for f in os.listdir(mdf_dir) if is_mdf_file(f)]

    return mdf_dir, mdf_files
