"""
Metadata extraction from RCC files.
"""
from __future__ import absolute_import
import logging
import re
import datetime

from eodatasets import type as ptype


__author__ = 'u63606'

_log = logging.getLogger(__name__)


def extract_md(base_md, directory):
    """
    Extract metadata from an RCC filename if one exists.

    The RCC directory should contain an "I" data file from which we can get the date
    The filename will be of the form:
        Landsat 7: L7EB2012028010752ASA111I.data
        Landsat 5: L5TB2003339014237ASA111I00.data
    where the 2012 is the year and the 028 is the julian day of the year

    :type base_md: ptype.DatasetMetadata
    :type directory: Path
    :rtype: ptype.DatasetMetadata
    """
    files = list(find_rcc_files(directory))

    if len(files) < 1:
        _log.debug("No I.data file found in RCC directory")
        return base_md

    base_md = _extract_rcc_filename_fields(base_md, files[0].name)

    if not base_md.acquisition.los:
        base_md.acquisition.los = _calculate_stop_time(
            base_md.acquisition.aos,
            satellite=base_md.platform.code,
            file_path=files[0]
        )

    return base_md


def find_rcc_files(directory):
    """
    Find RCC files in the given directory

    :type directory: pathlib.Path
    :return: List of RCC file paths
    :rtype: list of Path
    """
    files = directory.glob('*I.data')
    for f in files:
        yield f
    files = directory.glob('*I[0-9][0-9].data')
    for f in files:
        yield f

#  Landsat 5 & 7 modes.
_INSTRUMENT_MODES = {
    'T': 'SAM',
    'B': 'BUMPER'
}


def _usgs_id_from_filename(filename):
    """

    :param filename:
    :return:
    >>> _usgs_id_from_filename('7EB2012028010752ASA111I.data')
    '7EB2012028010752ASA111'
    >>> _usgs_id_from_filename('L5TB2003339014237ASA111I00.data')
    'L5TB2003339014237ASA111'
    """
    return filename[:filename.rindex('I')]


def _extract_rcc_filename_fields(base_md, filename):
    """
    Landsat 5 and 7 RCC format specifications:
        http://landsat.usgs.gov/documents/LS_DFCB_01.pdf
        http://landsat.usgs.gov/documents/LS_DFCB_06.pdf

    :type base_md: ptype.DatasetMetadata
    :type filename: str
    :rtype: ptype.DatasetMetadata
    """

    m = re.search('(?P<satsens>\w{4})(?P<date>\d{13})(?P<gsi>[^\d]+).*?(?P<version>\d\d)?\.data', filename)
    fields = m.groupdict()

    if not base_md.platform or not base_md.platform.code:
        # TODO: Do we have a cleaner way to do this? A list of mappings?
        satsens_ = fields['satsens']
        vehicle = satsens_[0]
        vehicle_num = satsens_[1]
        instrument_short = satsens_[2]
        smode_short = satsens_[3]

        if vehicle == 'L':
            if not base_md.platform:
                base_md.platform = ptype.PlatformMetadata()
            base_md.platform.code = 'LANDSAT_%s' % vehicle_num

            if not base_md.instrument:
                base_md.instrument = ptype.InstrumentMetadata()

            if vehicle_num == '7':
                if instrument_short == 'E':
                    base_md.instrument.name = 'ETM'
                else:
                    _log.warn('Unknown LS7 sensor char: %s', instrument_short)
            elif vehicle_num == '5':
                if instrument_short == 'T':
                    base_md.instrument.name = 'TM'
                else:
                    _log.warn('Unknown LS4/5 sensor char: %s', instrument_short)

            base_md.instrument.operation_mode = _INSTRUMENT_MODES.get(smode_short)
        else:
            _log.warn('Unknown vehicle: %s', vehicle)

    if not base_md.acquisition:
        base_md.acquisition = ptype.AcquisitionMetadata()

    if not base_md.acquisition.aos:
        base_md.acquisition.aos = datetime.datetime.strptime(fields['date'], "%Y%j%H%M%S")

    base_md.gsi = fields['gsi']
    if not base_md.acquisition.groundstation:
        base_md.acquisition.groundstation = ptype.GroundstationMetadata(code=fields['gsi'])

    base_md.usgs_dataset_id = _usgs_id_from_filename(filename)

    # RCC is raw: P00
    base_md.ga_level = 'P00'
    version = int(fields['version']) if fields.get('version') else None
    base_md.format_ = ptype.FormatMetadata(name='RCC', version=version)
    return base_md


def _calculate_stop_time(start_time, satellite=None, file_path=None):
    """
    :type start_time: datetime.datetime
    :type satellite: str
    :type file_path: pathlib.Path
    :rtype: datetime.datetime
    """
    stop = None
    start = start_time

    if start:
        if file_path.exists() and satellite == 'LANDSAT_7':
            # From the old onreceipt codebase.
            duration_seconds = round(file_path.stat().st_size * 8.0 / 75000000.0)
        elif file_path.exists() and satellite == 'LANDSAT_5':
            # From the old onreceipt codebase.
            duration_seconds = round(file_path.stat().st_size * 8.0 / 84900000.0)
        else:
            # From the old jobmanager codebase:
            # duration = 10 * 60

            # If we don't know, it's better to leave it as None
            duration_seconds = None

        if duration_seconds is not None:
            stop = start + datetime.timedelta(seconds=duration_seconds)
        _log.debug("Calculated stop time %s", stop)

    return stop


