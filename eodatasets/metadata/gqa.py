# -*- coding: utf-8 -*-
"""
"""

import logging
import csv

import re

import datetime

_LOG = logging.getLogger(__name__)


def populate_from_gqa(md, gqa_file):
    """
    :type md: eodatasets.type.DatasetMetadata
    :type gqa_file: pathlib.Path
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
    md.gqa.update({_clean_key(k): v for k, v in zip(headers, values)})


def _clean_key(k):
    """
    >>> _clean_key('Iterative Mean Residual X')
    'iterative_mean_residual_x'
    """
    return k.lower().strip().replace(' ', '_')


def _parse_day(f, fields):
    return datetime.datetime.strptime(fields[f], '%Y%m%d').date()
