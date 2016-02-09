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
from tests.integration import get_script_path, load_checksum_filenames, hardlink_arg

script_path = get_script_path(eodatasets.scripts.genpackage)

#: :type: Path
source_folder = Path(__file__).parent.joinpath('input', 'ls7-rcc')
assert source_folder.exists()

source_dataset = source_folder.joinpath(
    'data',
    'LS7_ETM_STD-RCC_P00_LANDSAT-7.65771.ALSP_0_0_20110827T021036Z20110827T021707_1'
)
assert source_dataset.exists()


@integration_test
def test_metadata():
    output_path = temp_dir()

    check_call(
        [
            'python',
            str(script_path),
            hardlink_arg(output_path, source_dataset),
            'raw',
            str(source_dataset),
            str(output_path)
        ]
    )

    # EODS LS7 dataset id:
    # 'LS7_ETM_STD-RCC_P00_LANDSAT-7.65771.ALSP_0_0_20110827T021036Z20110827T021707'
    # ... slightly different to NCI?

    assert_file_structure(output_path, {
        'LS7_ETM_STD-RCC_P00_L7EB2011239021036ASA111_0_0_20110827T021036Z20110827T021707': {
            'product': {
                'ephem.log': '',
                'acs.log': '',
                'L7EB2011239021036ASA111Q.data': '',
                'passinfo': '',
                'L7EB2011239021036ASA111I.data': '',
                'ref.log': '',
                'demod.log': ''
            },
            'ga-metadata.yaml': '',
            'package.sha1': ''
        }
    })
    output_path = output_path.joinpath(
        'LS7_ETM_STD-RCC_P00_L7EB2011239021036ASA111_0_0_20110827T021036Z20110827T021707')

    # TODO: Check metadata fields are sensible.
    output_metadata_path = output_path.joinpath('ga-metadata.yaml')
    assert output_metadata_path.exists()
    md = yaml.load(output_metadata_path.open('r'))

    # ID is different every time: check not none, and clear it.
    assert md['id'] is not None
    md['id'] = None

    assert_same(md, {
        'id': None,
        'size_bytes': 164368,
        'platform': {'code': 'LANDSAT_7'},
        'instrument': {
            'operation_mode': 'BUMPER',
            'name': 'ETM'
        },
        'ga_level': 'P00',
        'usgs': {
            'interval_id': 'L7EB2011239021036ASA111'
        },
        'product_type': 'satellite_telemetry_data',
        'format': {'name': 'RCC'},
        # Default creation date is the same as the input folder ctime.
        'creation_dt': datetime.datetime.utcfromtimestamp(source_dataset.stat().st_ctime),
        'ga_label': 'LS7_ETM_STD-RCC_P00_L7EB2011239021036ASA111_0_0_'
                    '20110827T021036Z20110827T021707',
        'acquisition': {
            'aos': datetime.datetime(2011, 8, 27, 2, 10, 36),
            'groundstation': {
                'code': 'ASA',
                'label': 'Alice Springs',
                'eods_domain_code': '002'
            },
            'los': datetime.datetime(2011, 8, 27, 2, 17, 7),
            'platform_orbit': 65771
        },
        'image': {'bands': {}},
        'lineage': {
            'source_datasets': {},
            'machine': {}
        },
        'checksum_path': 'package.sha1'
    })

    # Check all files are listed in checksum file.
    output_checksum_path = output_path.joinpath('package.sha1')
    assert output_checksum_path.exists()
    checksummed_filenames = load_checksum_filenames(output_checksum_path)
    assert checksummed_filenames == [
        'ga-metadata.yaml',
        'product/L7EB2011239021036ASA111I.data',
        'product/L7EB2011239021036ASA111Q.data',
        'product/acs.log',
        'product/demod.log',
        'product/ephem.log',
        'product/passinfo',
        'product/ref.log',
    ]
