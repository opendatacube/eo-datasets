# coding=utf-8
"""
Package an LS7 RCC dataset.
"""
from __future__ import absolute_import
from subprocess import check_call
import datetime

from pathlib import Path
import yaml

import eodatasets.scripts.genpackage
from tests import temp_dir, assert_file_structure, assert_same, integration_test
from tests.integration import get_script_path, load_checksum_filenames

script_path = get_script_path(eodatasets.scripts.genpackage)

#: :type: Path
source_folder = Path(__file__).parent.joinpath('input', 'ls8-mdf')
assert source_folder.exists()

source_dataset = source_folder.joinpath(
    'data',
    'LS8_OLI-TIRS_STD-MDF_P00_LC81140740812015123LGN00_114_074-081_'
    '20150503T031224Z20150503T031438_1'
)
assert source_dataset.exists()


@integration_test
def test_metadata():
    output_path = temp_dir()

    check_call(
        [
            'python',
            str(script_path),
            '--hard-link',
            'raw',
            str(source_dataset),
            str(output_path)
        ]
    )

    assert_file_structure(output_path, {
        'LS8_OLITIRS_STD-MDF_P00_LC81140740812015123LGN00_114_074-081_'
        '20150503T031224Z20150503T031438': {
            'product': {
                '270.000.2015123031324364.LGS': '',
                '271.000.2015123031330204.LGS': '',
                '271.001.2015123031352904.LGS': '',
                '271.002.2015123031415490.LGS': '',
                '271.003.2015123031438105.LGS': '',
                'LC81140740812015123LGN00_IDF.xml': '',
                'LC81140740812015123LGN00_MD5.txt': '',
            },
            'ga-metadata.yaml': '',
            'package.sha1': ''
        }
    })
    output_path = output_path.joinpath(
        'LS8_OLITIRS_STD-MDF_P00_LC81140740812015123LGN00_114_074-081_'
        '20150503T031224Z20150503T031438')

    # TODO: Check metadata fields are sensible.
    output_metadata_path = output_path.joinpath('ga-metadata.yaml')
    assert output_metadata_path.exists()
    md = yaml.load(output_metadata_path.open('r'))

    # ID is different every time: check not none, and clear it.
    assert md['id'] is not None
    md['id'] = None

    assert_same(md, {
        'id': None,
        'ga_label': 'LS8_OLITIRS_STD-MDF_P00_LC81140740812015123LGN00_114_074-081_'
                    '20150503T031224Z20150503T031438',
        # Default creation date is the same as the input folder ctime.
        'creation_dt': datetime.datetime.utcfromtimestamp(source_dataset.stat().st_ctime),
        'size_bytes': 4485,
        'product_type': 'raw',
        'usgs_dataset_id': 'LC81140740812015123LGN00',
        'format': {'name': 'MDF'},
        'ga_level': 'P00',
        'checksum_path': 'package.sha1',
        'platform': {
            'code': 'LANDSAT_8'
        },
        'instrument': {
            'name': 'OLI_TIRS'
        },
        'acquisition': {
            'los': datetime.datetime(2015, 5, 3, 3, 14, 38, 105000),
            'aos': datetime.datetime(2015, 5, 3, 3, 12, 24, 364000),
            'groundstation': {
                'code': 'LGN',
                'label': 'Landsat Ground Network',
                'eods_domain_code': '032'}
        },
        'image': {
            'satellite_ref_point_start': {'x': 114, 'y': 74},
            'satellite_ref_point_end': {'x': 114, 'y': 81},
            'bands': {},
        },
        'lineage': {
            'source_datasets': {},
            'machine': {}
        }
    })

    # Check all files are listed in checksum file.
    output_checksum_path = output_path.joinpath('package.sha1')
    assert output_checksum_path.exists()
    checksummed_filenames = load_checksum_filenames(output_checksum_path)
    assert checksummed_filenames == [
        'ga-metadata.yaml',
        'product/270.000.2015123031324364.LGS',
        'product/271.000.2015123031330204.LGS',
        'product/271.001.2015123031352904.LGS',
        'product/271.002.2015123031415490.LGS',
        'product/271.003.2015123031438105.LGS',
        'product/LC81140740812015123LGN00_IDF.xml',
        'product/LC81140740812015123LGN00_MD5.txt',
    ]
