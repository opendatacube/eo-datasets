# -*- coding: utf-8 -*-
"""
"""

import logging

import datetime

from eodatasets.type import DatasetMetadata
from pathlib import Path

from eodatasets.metadata import gqa

_LOG = logging.getLogger(__name__)

_GQA_PATH = Path(__file__).absolute().parent.joinpath('20141201_20010425_B6_gqa_results.csv')


def test_gqa():
    md = DatasetMetadata()
    gqa.populate_from_gqa(md, _GQA_PATH)
    print(repr(md.gqa))
    assert md.gqa == {
        'abs_iterative_mean_residual_x': 1.3,
        'abs_iterative_mean_residual_y': 1.2,
        'acq_day': datetime.date(2014, 12, 1),
        # Bands are always strings. They can have odd names ("6_vcid_1").
        'band': '6',
        'blue': 120,
        'cep90': 212.0,
        'final_gcp_count': 1493,
        'green': 340,
        'iterative_mean_residual_x': -0.4,
        'iterative_mean_residual_y': 0.5,
        'iterative_stddev_residual_x': 2.5,
        'iterative_stddev_residual_y': 2.5,
        'mean_residual_x': -0.4,
        'mean_residual_y': 0.5,
        'red': 321,
        'ref_day': datetime.date(2001, 4, 25),
        'residual_x': 1.9,
        'residual_y': 1.8,
        'sceneid': 'LS8_OLITIRS_OTH_P51_GALPGS01-032_099_072_20141201',
        'stddev_residual_x': 3.6,
        'stddev_residual_y': 3.6,
        'teal': 735,
        'yellow': 98,
    }
