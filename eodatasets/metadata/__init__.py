# coding=utf-8
from __future__ import absolute_import

import json
import logging

from pathlib import Path
import eodatasets.type as ptype


_LOG = logging.getLogger(__name__)

# From the gaip codebase. Lookup table for sensor information.
with Path(__file__).parent.joinpath('sensors.json').open() as fo:
    SENSORS = json.load(fo)

with Path(__file__).parent.joinpath('groundstations.json').open() as f:
    _GROUNDSTATION_LIST = json.load(f)

# Build groundstation alias lookup table.
_GROUNDSTATION_ALIASES = {}
for _station in _GROUNDSTATION_LIST:
    gsi_ = _station['code']
    _GROUNDSTATION_ALIASES[gsi_] = gsi_
    _GROUNDSTATION_ALIASES.update({alias: gsi_ for alias in _station['aliases']})


def normalise_gsi(gsi):
    """
    Normalise the given GSI.

    Many old datasets and systems use common aliases instead of the actual gsi. We try to translate.
    :type gsi: str
    :rtype: str

    >>> normalise_gsi('ALSP')
    'ASA'
    >>> normalise_gsi('ASA')
    'ASA'
    >>> normalise_gsi('Alice')
    'ASA'
    >>> normalise_gsi('TERSS')
    'HOA'
    """
    return str(_GROUNDSTATION_ALIASES.get(gsi.upper()))


def get_groundstation(gsi):
    """

    :param gsi:
    :rtype: ptype.GroundstationMetadata
    >>> get_groundstation('ASA')
    GroundstationMetadata(code='ASA', label='Alice Springs', eods_domain_code='002')
    >>> # Aliases should work too
    >>> get_groundstation('ALICE')
    GroundstationMetadata(code='ASA', label='Alice Springs', eods_domain_code='002')
    >>> get_groundstation('UNKNOWN_GSI')
    """
    gsi = normalise_gsi(gsi)
    stations = [g for g in _GROUNDSTATION_LIST if g['code'].upper() == gsi]
    if not stations:
        _LOG.warn('Station GSI not known: %r', gsi)
        return None
    station = stations[0]
    return ptype.GroundstationMetadata(
        code=str(station['code']),
        label=str(station['label']),
        eods_domain_code=str(station['eods_domain_code'])
    )


def _expand_band_information(satellite, sensor, band_metadata):
    """
    Use the gaip reference table to add per-band metadata if available.
    :param satellite: satellite as reported by LPGS (eg. LANDSAT_8)
    :param sensor: sensor as reported by LPGS (eg. OLI_TIRS)
    :type band_metadata: ptype.BandMetadata
    :rtype: ptype.BandMetadata

    >>> _expand_band_information('LANDSAT_8', 'OLI_TIRS', ptype.BandMetadata(number='4'))
    BandMetadata(type_='reflective', label='Visible Red', number='4', cell_size=25.0)
    """

    bands = SENSORS[satellite]['sensors'][sensor]['bands']

    band = bands.get(band_metadata.number)
    if band:
        band_metadata.label = str(band['desc'])
        band_metadata.cell_size = band['resolution']
        band_metadata.type_ = str(band['type_desc']).lower()

    return band_metadata


def expand_common_metadata(d):
    """
    :type d: ptype.DatasetMetadata
    :rtype: ptype.DatasetMetadata
    """
    if d.image and d.image.bands:
        for band_metadata in d.image.bands.values():
            _expand_band_information(d.platform.code, d.instrument.name, band_metadata)

    if d.acquisition and d.acquisition.groundstation and d.acquisition.groundstation.code:
        gstation = d.acquisition.groundstation
        # Ensure we're using a standard GSI code.
        gstation.code = normalise_gsi(gstation.code)
        # Set any missing fields for this groundstation.
        full_groundstation = get_groundstation(gstation.code)
        gstation.steal_fields_from(full_groundstation)

    return d
