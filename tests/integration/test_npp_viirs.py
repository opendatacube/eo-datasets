# coding=utf-8
"""
Package a raw AQUA PDS dataset.
"""
from __future__ import absolute_import

import datetime

import yaml
from click.testing import CliRunner
from pathlib import Path

import eodatasets.scripts.genpackage
from tests import temp_dir, assert_file_structure, assert_same, integration_test
from tests.integration import get_script_path, load_checksum_filenames, hardlink_arg

script_path = get_script_path(eodatasets.scripts.genpackage)

#: :type: Path
source_folder = Path(__file__).parent.joinpath('input', 'npp-viirs')
assert source_folder.exists()

source_dataset = source_folder.joinpath(
    'data',
    'NPP_VIIRS_STD-RDR_P00_NPP.VIIRS.18966.ALICE_0_0_20150626T053709Z20150626T054942_1'
)
assert source_dataset.exists()


@integration_test
def test_metadata():
    output_path = temp_dir()
    runner = CliRunner()
    runner.invoke(
        eodatasets.scripts.genpackage.run,
        [
            hardlink_arg(output_path, source_dataset),
            'raw',
            str(source_dataset), str(output_path)
        ],
        catch_exceptions=False
    )

    assert_file_structure(output_path, {
        'NPP_VIIRS_STD-HDF5_P00_18966.ASA_0_0_20150626T053709Z20150626T055046': {
            'product': {
                'RNSCA-RVIRS_npp_d20150626_t0537097_e0549423_b18966_'
                'c20150626055046759000_nfts_drl.h5': '',
            },
            'ga-metadata.yaml': '',
            'package.sha1': ''
        }
    })
    output_path = output_path.joinpath(
        'NPP_VIIRS_STD-HDF5_P00_18966.ASA_0_0_20150626T053709Z20150626T055046')

    # TODO: Check metadata fields are sensible.
    output_metadata_path = output_path.joinpath('ga-metadata.yaml')
    assert output_metadata_path.exists()
    md = yaml.load(output_metadata_path.open('r'))

    # ID is different every time: check not none, and clear it.
    assert md['id'] is not None
    md['id'] = None

    import sys

    sys.stderr.write('\n\n\n\n%r\n\n\n' % md)

    assert_same(
        md,
        {'ga_label': 'NPP_VIIRS_STD-HDF5_P00_18966.ASA_0_0_20150626T053709Z20150626T055046',
         'image': {'bands': {}},
         'size_bytes': 0,
         'creation_dt': datetime.datetime.utcfromtimestamp(source_dataset.stat().st_ctime),
         'id': None,
         'platform': {'code': 'NPP'},
         'instrument': {'name': 'VIIRS'},
         'ga_level': 'P00',
         'format': {'name': 'HDF5'},
         'checksum_path': 'package.sha1',
         'product_type': 'satellite_telemetry_data',
         'acquisition': {
             'groundstation': {
                 'eods_domain_code': '002',
                 'label': 'Alice Springs',
                 'code': 'ASA'
             },
             'platform_orbit': 18966,
             'los': datetime.datetime(2015, 6, 26, 5, 50, 46),
             'aos': datetime.datetime(2015, 6, 26, 5, 37, 9)
         },
         'lineage': {
             'machine': {},
             'source_datasets': {}
         },
         }
    )

    # Check all files are listed in checksum file.
    output_checksum_path = output_path.joinpath('package.sha1')
    assert output_checksum_path.exists()
    checksummed_filenames = load_checksum_filenames(output_checksum_path)
    assert checksummed_filenames == [
        'ga-metadata.yaml',
        'product/RNSCA-RVIRS_npp_d20150626_t0537097_e0549423_b18966_'
        'c20150626055046759000_nfts_drl.h5',
    ]
