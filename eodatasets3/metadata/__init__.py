# coding=utf-8
from __future__ import absolute_import

import json
import logging
from pathlib import Path

_LOG = logging.getLogger(__name__)

# From the gaip codebase. Lookup table for sensor information.
with Path(__file__).parent.joinpath("sensors.json").open() as fo:
    SENSORS = json.load(fo)

with Path(__file__).parent.joinpath("groundstations.json").open() as f:
    _GROUNDSTATION_LIST = json.load(f)

# Build groundstation alias lookup table.
_GROUNDSTATION_ALIASES = {}
for _station in _GROUNDSTATION_LIST:
    gsi_ = _station["code"]
    _GROUNDSTATION_ALIASES[gsi_] = gsi_
    _GROUNDSTATION_ALIASES.update({alias: gsi_ for alias in _station["aliases"]})


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


def is_groundstation_alias(alias):
    """
    Is this a known groundstation alias?
    :type alias: str
    :rtype: bool

    >>> is_groundstation_alias('ALICE')
    True
    >>> is_groundstation_alias('alice')
    True
    >>> is_groundstation_alias('ASA')
    True
    >>> is_groundstation_alias('NOT_AN_ALIAS')
    False
    """
    return alias.upper() in _GROUNDSTATION_ALIASES
