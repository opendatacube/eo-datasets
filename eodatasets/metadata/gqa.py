# -*- coding: utf-8 -*-
"""
Extract metadata from GQA 'results' csv files.
"""

import csv
import datetime
import logging
import re

from .util import parse_type

_LOG = logging.getLogger(__name__)


def choose_and_populate_gqa(dataset, files):
    """
    Find any gqa results in the list of files and populate the dataset with them.

    :type dataset: eodatasets.type.DatasetMetadata
    :type files: tuple[pathlib.Path]
    :rtype dataset: eodatasets.type.DatasetMetadata
    """
    gqa_file = _choose_gqa(files)
    if gqa_file:
        dataset = populate_from_gqa(dataset, gqa_file)
    return dataset


def _choose_gqa(additional_files):
    """
    Choose the latest GQA in a list of files (or none if there aren't any).
    :type additional_files: tuple[pathlib.Path]
    :rtype: pathlib.Path or None

    >>> from pathlib import Path
    >>> files = (
    ...     Path('additional/20141201_19991029_B6_gqa_results.csv'),
    ...     Path('additional/20141201_20000321_B6_gqa_results.csv')
    ... )
    >>> str(_choose_gqa(files))
    'additional/20141201_20000321_B6_gqa_results.csv'
    >>> str(_choose_gqa(files[:1]))
    'additional/20141201_19991029_B6_gqa_results.csv'
    >>> _choose_gqa(())
    """
    gqa_files = [f for f in additional_files if f.name.endswith('gqa_results.csv')]
    if not gqa_files:
        return None

    newest_first = list(sorted(gqa_files, reverse=True))
    return newest_first[0]


def populate_from_gqa(md, gqa_file):
    """
    :type md: eodatasets.type.DatasetMetadata
    :type gqa_file: pathlib.Path
    :rtype eodatasets.type.DatasetMetadata
    """
    # Example: 20141201_20010425_B6_gqa_results.csv
    fields = re.match(
        (
            r"(?P<acq_day>[0-9]{8})"
            r"_(?P<ref_day>[0-9]{8})"
            r"_B(?P<band>[0-9a-zA-Z]+)"
            r"_gqa_results"
            "$"
        ), gqa_file.stem).groupdict()

    # Parse from filename: date, reference date, band
    md.gqa = {
        'acq_day': _parse_day('acq_day', fields),
        'ref_day': _parse_day('ref_day', fields),
        'band': fields['band']
    }

    # Read values.
    with gqa_file.open('r') as f:
        rows = csv.reader(f)
        headers = next(rows)
        values = next(rows)
    md.gqa.update({_clean_key(k): parse_type(v) for k, v in zip(headers, values)})

    return md


def _clean_key(k):
    """
    >>> _clean_key('Iterative Mean Residual X')
    'iterative_mean_residual_x'
    """
    return k.lower().strip().replace(' ', '_')


def _parse_day(f, fields):
    return datetime.datetime.strptime(fields[f], '%Y%m%d').date()
