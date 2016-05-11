# -*- coding: utf-8 -*-
"""
"""

import datetime
import logging

from pathlib import Path

from eodatasets import type as ptype
from eodatasets.metadata import gqa

_LOG = logging.getLogger(__name__)

_GQA_PATH = Path(__file__).absolute().parent.joinpath('gqa_results.yaml')


def test_gqa():
    md = ptype.DatasetMetadata(lineage=ptype.LineageMetadata(machine=ptype.MachineMetadata()))
    gqa.populate_from_gqa(md, _GQA_PATH)
    print(repr(md.gqa))
    assert md.gqa == {
        "cep90": 0.41,
        "colors": {
            "blue": 600,
            "green": 30164,
            "red": 336,
            "teal": 1340,
            "yellow": 399
        },
        "final_gcp_count": 32582,
        "ref_date": datetime.date(2000, 9, 4),
        "ref_source": "GLS_v2",
        "ref_source_path": "/g/data/v10/eoancillarydata/GCP/GQA_v2/wrs2/091/081/LE70910812000248ASA00_B5.TIF",
        "residual": {
            "abs": {
                "x": 0.2,
                "y": 0.23
            },
            "abs_iterative_mean": {
                "x": 0.15,
                "y": 0.17
            },
            "iterative_mean": {
                "x": 0.02,
                "y": 0
            },
            "iterative_stddev": {
                "x": 0.32,
                "y": 0.52
            },
            "mean": {
                "x": 0.01,
                "y": -0.03
            },
            "stddev": {
                "x": 1.27,
                "y": 3.94
            }
        }
    }

    assert 'gqa' in md.lineage.machine.software_versions
    assert md.lineage.machine.software_versions['gqa'] == {
        'repo_url': "https://github.com/GeoscienceAustralia/gqa.git",
        'version': "0.4+20.gb0d00dc"
    }
