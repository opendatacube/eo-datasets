# -*- coding: utf-8 -*-
"""
"""

import datetime
import logging

from pathlib import Path

from eodatasets.metadata import gqa
from eodatasets.type import DatasetMetadata

_LOG = logging.getLogger(__name__)

_GQA_PATH = Path(__file__).absolute().parent.joinpath('20130818_20000119_B5_gqa_results.yaml')


def test_gqa():
    md = DatasetMetadata()
    gqa.populate_from_gqa(md, _GQA_PATH)
    print(repr(md.gqa))
    assert md.gqa == {
        'stddev_residual': {'y': 4.524645392606803, 'x': 4.23604905517344},
        'abs_iterative_mean_residual': {'y': 4.081039145021645, 'x': 8.281189264069264},
        'acq_day': datetime.date(2013, 8, 18),
        'residual': {'y': 4.070382, 'x': 8.266335},
        'colors': {
            'teal': 0.0,
            'blue': 10.0,
            'yellow': 25.0,
            'green': 0.0,
            'red': 274.0
        },
        'ref_day': datetime.date(2000, 1, 19),
        'ref_source': 'GLS_v1',
        'iterative_mean_residual': {'y': 0.9739366558441558, 'x': 8.281189264069264},
        'mean_residual': {'y': 0.9739366558441558, 'x': 8.281189264069264},
        'iterative_stddev_residual': {'y': 4.524645392606803, 'x': 4.23604905517344},
        'cep90': 440.00546272953613,
        # Bands are always strings. They can have odd names ("5_vcid_1")
        'band': '5',
        'final_gcp_count': 308
    }
