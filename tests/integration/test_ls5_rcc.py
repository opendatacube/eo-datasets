# coding=utf-8
"""
Package an LS5 RCC dataset.
"""
from __future__ import absolute_import
import datetime

from click.testing import CliRunner
from pathlib import Path
import yaml

import eodatasets.scripts.genpackage
from tests import temp_dir, assert_file_structure, assert_same, integration_test
from tests.integration import get_script_path, load_checksum_filenames

script_path = get_script_path(eodatasets.scripts.genpackage)

#: :type: Path
source_folder = Path(__file__).parent.joinpath('input', 'ls5-rcc')
assert source_folder.exists()

source_dataset = source_folder.joinpath(
    'data',
    'LS5_TM_STD-RCC_P00_LANDSAT-5.146212.ALSP_0_0_20110828T002022Z20110828T002858_1'
)
assert source_dataset.exists()


@integration_test
def test_metadata():
    output_path = temp_dir()

    runner = CliRunner()
    runner.invoke(
        eodatasets.scripts.genpackage.run,
        [
            '--hard-link',
            'raw',
            str(source_dataset), str(output_path)
        ],
        catch_exceptions=False
    )

    assert_file_structure(output_path, {
        'LS5_TM_STD-RCC_P00_L5TB2011240002022ASA123_0_0_20110828T002022Z20110828T002858': {
            'product': {
                'L5TB2011240002022ASA123I00.data': '',
                'acs.log': '',
                'demod.log': '',
                'ephem.log': '',
                'passinfo': '',
                'ref.log': '',
            },
            'ga-metadata.yaml': '',
            'package.sha1': ''
        }
    })
    output_path = output_path.joinpath(
        'LS5_TM_STD-RCC_P00_L5TB2011240002022ASA123_0_0_20110828T002022Z20110828T002858')

    # TODO: Check metadata fields are sensible.
    output_metadata_path = output_path.joinpath('ga-metadata.yaml')
    assert output_metadata_path.exists()
    md = yaml.load(output_metadata_path.open('r'))

    # ID is different every time: check not none, and clear it.
    assert md['id'] is not None
    md['id'] = None

    assert_same(
        md,
        {
            'id': None,
            'ga_level': 'P00',
            # Default creation date is the same as the input folder ctime.
            'creation_dt': datetime.datetime.utcfromtimestamp(source_dataset.stat().st_ctime),
            'platform': {'code': 'LANDSAT_5'},
            'format': {'version': 0, 'name': 'RCC'},
            'size_bytes': 226667,
            'product_type': 'satellite_telemetry_data',
            'usgs': {
                'interval_id': 'L5TB2011240002022ASA123'
            },
            'instrument': {
                'name': 'TM',
                'operation_mode': 'BUMPER'
            },
            'acquisition': {
                'aos': datetime.datetime(2011, 8, 28, 0, 20, 22),
                'los': datetime.datetime(2011, 8, 28, 0, 28, 58),
                'platform_orbit': 146212,
                'groundstation': {
                    'eods_domain_code': '002',
                    'label': 'Alice Springs',
                    'code': 'ASA'
                },
            },
            'ga_label': 'LS5_TM_STD-RCC_P00_L5TB2011240002022ASA123_0_0_'
                        '20110828T002022Z20110828T002858',
            'checksum_path': 'package.sha1',
            'lineage': {
                'machine': {},
                'source_datasets': {}
            },
            'image': {
                'bands': {}
            }
        }
    )

    # Check all files are listed in checksum file.
    output_checksum_path = output_path.joinpath('package.sha1')
    assert output_checksum_path.exists()
    checksummed_filenames = load_checksum_filenames(output_checksum_path)
    assert checksummed_filenames == [
        'ga-metadata.yaml',
        'product/L5TB2011240002022ASA123I00.data',
        'product/acs.log',
        'product/demod.log',
        'product/ephem.log',
        'product/passinfo',
        'product/ref.log',
    ]
