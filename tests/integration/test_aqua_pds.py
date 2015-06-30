# coding=utf-8
"""
Package a raw AQUA PDS dataset.
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
source_folder = Path(__file__).parent.joinpath('input', 'aqua-pds')
assert source_folder.exists()

source_dataset = source_folder.joinpath(
    'data',
    'AQUA.65208.S1A1C1D1R1'
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

    # EODS LS7 dataset id:
    # 'LS7_ETM_STD-RCC_P00_LANDSAT-7.65771.ALSP_0_0_20110827T021036Z20110827T021707'
    # ... slightly different to NCI?

    assert_file_structure(output_path, {
        'AQUA_MODIS_STD-PDS_P00_65208.S1A1C1D1R1_0_0_20140807T031628Z20140807T031630': {
            'product': {
                'P1540064AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540064AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540141AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540141AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540157AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540157AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540261AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540261AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540262AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540262AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540290AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540290AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540342AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540342AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540402AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540402AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540404AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540404AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540405AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540405AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540406AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540406AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540407AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540407AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540414AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540414AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540415AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540415AAAAAAAAAAAAAA14219032341001.PDS': '',
                'P1540957AAAAAAAAAAAAAA14219032341000.PDS': '',
                'P1540957AAAAAAAAAAAAAA14219032341001.PDS': '',
            },
            'ga-metadata.yaml': '',
            'package.sha1': ''
        }
    })
    output_path = output_path.joinpath(
        'AQUA_MODIS_STD-PDS_P00_65208.S1A1C1D1R1_0_0_20140807T031628Z20140807T031630')

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
            'lineage': {'machine': {}, 'source_datasets': {}},
            'product_type': 'raw',
            'format': {'name': 'PDS'},
            'image': {'bands': {}, 'day_percentage_estimate': 100.0},
            # Default creation date is the same as the input folder ctime.
            'creation_dt': datetime.datetime.utcfromtimestamp(source_dataset.stat().st_ctime),
            'rms_string': 'S1A1C1D1R1',
            'instrument': {'name': 'MODIS'},
            'ga_label': 'AQUA_MODIS_STD-PDS_P00_65208.S1A1C1D1R1_0_0_20140807T031628Z20140807T031630',
            'platform': {'code': 'AQUA'},
            'size_bytes': 2144280,
            'checksum_path': 'package.sha1',
            'id': None,
            'acquisition': {
                'los': datetime.datetime(2014, 8, 7, 3, 16, 30, 228023),
                'platform_orbit': 65208,
                'aos': datetime.datetime(2014, 8, 7, 3, 16, 28, 750910)
            }
        }
    )

    # Check all files are listed in checksum file.
    output_checksum_path = output_path.joinpath('package.sha1')
    assert output_checksum_path.exists()
    checksummed_filenames = load_checksum_filenames(output_checksum_path)
    assert checksummed_filenames == [
        'ga-metadata.yaml',
        'product/P1540064AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540064AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540141AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540141AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540157AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540157AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540261AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540261AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540262AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540262AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540290AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540290AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540342AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540342AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540402AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540402AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540404AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540404AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540405AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540405AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540406AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540406AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540407AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540407AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540414AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540414AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540415AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540415AAAAAAAAAAAAAA14219032341001.PDS',
        'product/P1540957AAAAAAAAAAAAAA14219032341000.PDS',
        'product/P1540957AAAAAAAAAAAAAA14219032341001.PDS',
    ]
