# coding=utf-8
"""
Package an LS7 Ortho dataset.
"""
from __future__ import absolute_import

import datetime
from pathlib import Path

import yaml

from tests import temp_dir, assert_file_structure, assert_same, integration_test, run_packaging_cli, as_file_list
from tests.integration import load_checksum_filenames, hardlink_arg, prepare_datasets_for_comparison, FakeAncilFile, \
    prepare_work_order

#: :type: Path
source_folder = Path(__file__).parent.joinpath('input', 'ls7-ortho')
assert source_folder.exists()

source_dataset = source_folder.joinpath('data', 'LE7_20110214_092_082')
assert source_dataset.exists()

parent_dataset = source_folder.joinpath('parent')
assert parent_dataset.exists()

additional_files = source_folder.joinpath('data', 'additional')
assert additional_files.exists()

wo_template = source_folder.joinpath('work_order.xml')
assert wo_template.exists()


@integration_test
def test_package():
    work_path = temp_dir()
    output_path = work_path.joinpath('out')
    output_path.mkdir(parents=True)
    ancil_base = work_path.joinpath('ancil')
    # We have to override the ancillary directory lookup as they won't exist on test systems.
    ancil_files = (
        FakeAncilFile(ancil_base, 'cpf', 'LE07CPF_20110101_20110331_01.02'),
        FakeAncilFile(ancil_base, 'ephemeris', 'L72013231ASADEF.S00'),
    )
    work_order = prepare_work_order(ancil_files, wo_template)

    # Run!
    args = [
        hardlink_arg(output_path, source_dataset),
        'level1',
        '--newly-processed',
        '--parent', str(parent_dataset),
        '--add-file', str(work_order)
    ]
    for additional_file in additional_files.iterdir():
        args.extend(['--add-file', str(additional_file)])
    args.extend([
        str(source_dataset),
        str(output_path)
    ])
    run_packaging_cli(args)

    output_dataset = output_path.joinpath('LS7_ETM_SYS_P31_GALPGS01-002_092_082_20110214')

    assert_file_structure(output_path, {
        'LS7_ETM_SYS_P31_GALPGS01-002_092_082_20110214': {
            'browse.jpg': '',
            'browse.fr.jpg': '',
            'product': {
                'LE07_L1GS_092082_20110214_20170221_01_T2_ANG.txt': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B1.TIF': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B2.TIF': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B3.TIF': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B4.TIF': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B5.TIF': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B6_VCID_1.TIF': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B6_VCID_2.TIF': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B7.TIF': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B8.TIF': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_BQA.TIF': '',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B1.IMD': 'optional',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B2.IMD': 'optional',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B3.IMD': 'optional',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B4.IMD': 'optional',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B5.IMD': 'optional',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B6_VCID_1.IMD': 'optional',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B6_VCID_2.IMD': 'optional',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B7.IMD': 'optional',
                'LE07_L1GS_092082_20110214_20170221_01_T2_B8.IMD': 'optional',
                'LE07_L1GS_092082_20110214_20170221_01_T2_BQA.IMD': 'optional',
                'LE07_L1GS_092082_20110214_20170221_01_T2_MTL.txt': '',
                'LE7_20110214_092_082_L1GS.xml': '',
            },
            'additional': {
                'work_order.xml': '',
                '20130818_20000119_B5_gqa_results.yaml': '',
                'lpgs_out.xml': '',
            },
            'ga-metadata.yaml': '',
            'package.sha1': ''
        }
    })

    # TODO: Check metadata fields are sensible.
    output_metadata_path = output_dataset.joinpath('ga-metadata.yaml')
    assert output_metadata_path.exists()
    md = yaml.load(output_metadata_path.open('r'))

    prepare_datasets_for_comparison(
        EXPECTED_METADATA,
        md,
        ancil_files,
        output_dataset.joinpath('product')
    )

    assert_same(md, EXPECTED_METADATA)

    # TODO: Asset all files are checksummed.
    output_checksum_path = output_dataset.joinpath('package.sha1')
    assert output_checksum_path.exists()
    checksummed_filenames = load_checksum_filenames(output_checksum_path)

    expected_filenames = sorted(f for f in as_file_list(output_dataset) if f != 'package.sha1')
    assert set(checksummed_filenames) == set(expected_filenames)
    assert checksummed_filenames == expected_filenames


EXPECTED_METADATA = {
    'acquisition': {
        'groundstation': {
            'code': 'ASA',
            'eods_domain_code': '002',
            'label': 'Alice Springs'
        }
    },
    'browse': {
        'full': {
            'blue_band': '1',
            'cell_size': 6106.25,
            'file_type': 'image/jpg',
            'green_band': '4',
            'path': 'browse.fr.jpg',
            'red_band': '7',
            'shape': {'x': 40, 'y': 35}},
        'medium': {
            'blue_band': '1',
            'cell_size': 238.525390625,
            'file_type': 'image/jpg',
            'green_band': '4',
            'path': 'browse.jpg',
            'red_band': '7',
            'shape': {'x': 1024, 'y': 896}
        }
    },
    'checksum_path': 'package.sha1',
    'creation_dt': datetime.datetime(2017, 2, 21, 1, 58, 51),
    'extent': {
        'center_dt': datetime.datetime(2011, 2, 14, 23, 55, 17, 686537),
        'coord': {
            'll': {'lat': -32.698456, 'lon': 146.241866},
            'lr': {'lat': -32.687146, 'lon': 148.847048},
            'ul': {'lat': -30.77261, 'lon': 146.257392},
            'ur': {'lat': -30.762116, 'lon': 148.809244}
        },
        'from_dt': datetime.datetime(2011, 2, 14, 23, 55, 4),
        'to_dt': datetime.datetime(2011, 2, 14, 23, 55, 30)
    },
    'format': {'name': 'GeoTIFF'},
    'ga_label': 'LS7_ETM_SYS_P31_GALPGS01-002_092_082_20110214',
    'gqa': {
        'cep90': 0.41,
        'colors': {'blue': 600.0,
                   'green': 30164.0,
                   'red': 336.0,
                   'teal': 1340.0,
                   'yellow': 399.0},
        'final_gcp_count': 32582,
        'ref_date': datetime.date(2000, 9, 4),
        'ref_source': 'GLS_v2',
        'ref_source_path': '/g/data/v10/eoancillarydata/GCP/GQA_v2/wrs2/091/081/LE70910812000248ASA00_B5.TIF',
        'residual': {
            'abs': {'x': 0.2, 'y': 0.23},
            'abs_iterative_mean': {'x': 0.15, 'y': 0.17},
            'iterative_mean': {'x': 0.02, 'y': 0.0},
            'iterative_stddev': {'x': 0.32, 'y': 0.52},
            'mean': {'x': 0.01, 'y': -0.03},
            'stddev': {'x': 1.27, 'y': 3.94}}},
    'grid_spatial': {
        'projection': {
            'datum': 'GDA94',
            'ellipsoid': 'GRS80',
            'geo_ref_points': {
                'll': {'x': 428937.5,
                       'y': 6381887.5},
                'lr': {'x': 673162.5,
                       'y': 6381887.5},
                'ul': {'x': 428937.5,
                       'y': 6595362.5},
                'ur': {'x': 673162.5,
                       'y': 6595362.5}
            },
            'map_projection': 'UTM',
            'orientation': 'NORTH_UP',
            'resampling_option': 'CUBIC_CONVOLUTION',
            'zone': -55
        }
    },
    'id': None,
    'image': {
        'bands': {
            '1': {'cell_size': 25.0,
                  'label': 'Visible Blue',
                  'number': '1',
                  'path': 'product/LE07_L1GS_092082_20110214_20170221_01_T2_B1.TIF',
                  'type': 'reflective'},
            '2': {'cell_size': 25.0,
                  'label': 'Visible Green',
                  'number': '2',
                  'path': 'product/LE07_L1GS_092082_20110214_20170221_01_T2_B2.TIF',
                  'type': 'reflective'},
            '3': {'cell_size': 25.0,
                  'label': 'Visible Red',
                  'number': '3',
                  'path': 'product/LE07_L1GS_092082_20110214_20170221_01_T2_B3.TIF',
                  'type': 'reflective'},
            '4': {'cell_size': 25.0,
                  'label': 'Near Infrared',
                  'number': '4',
                  'path': 'product/LE07_L1GS_092082_20110214_20170221_01_T2_B4.TIF',
                  'type': 'reflective'},
            '5': {'cell_size': 25.0,
                  'label': 'Middle Infrared 1',
                  'number': '5',
                  'path': 'product/LE07_L1GS_092082_20110214_20170221_01_T2_B5.TIF',
                  'type': 'reflective'},
            '6_vcid_1': {'cell_size': 50.0,
                         'label': 'Thermal Infrared [Low Gain]',
                         'number': '6_vcid_1',
                         'path': 'product/LE07_L1GS_092082_20110214_20170221_01_T2_B6_VCID_1.TIF',
                         'type': 'thermal'},
            '6_vcid_2': {'cell_size': 50.0,
                         'label': 'Thermal Infrared [High Gain]',
                         'number': '6_vcid_2',
                         'path': 'product/LE07_L1GS_092082_20110214_20170221_01_T2_B6_VCID_2.TIF',
                         'type': 'thermal'},
            '7': {'cell_size': 25.0,
                  'label': 'Middle Infrared 2',
                  'number': '7',
                  'path': 'product/LE07_L1GS_092082_20110214_20170221_01_T2_B7.TIF',
                  'type': 'reflective'},
            '8': {'cell_size': 12.5,
                  'label': 'Panchromatic',
                  'number': '8',
                  'path': 'product/LE07_L1GS_092082_20110214_20170221_01_T2_B8.TIF',
                  'type': 'panchromatic'},
            'qa': {'number': 'qa',
                   'path': 'product/LE07_L1GS_092082_20110214_20170221_01_T2_BQA.TIF'}
        },
        'cloud_cover_percentage': 100.0,
        'satellite_ref_point_start': {'x': 92, 'y': 82},
        'sun_azimuth': 69.58819406,
        'sun_earth_distance': 0.9876111,
        'sun_elevation': 51.05312766},
    'instrument': {'name': 'ETM', 'operation_mode': 'BUMPER'},
    'lineage': {
        'algorithm': {
            'name': 'LPGS',
            'parameters': {},
            'version': '12.8.2'
        },
        'ancillary': {
            'cpf': {
                'name': 'LE07CPF_20110101_20110331_01.02',
            },
            'ephemeris': {
                'name': 'L72013231ASADEF.S00',
                'properties': {'type': 'DEFINITIVE'},
            }
        },
        'ancillary_quality': 'DEFINITIVE',
        'machine': {
            'hostname': 'kveikur',
            'runtime_id': '02e99ec4-f7f8-11e6-a434-185e0f80a5c0',
            'software_versions': {
                'eodatasets': {
                    'repo_url': 'https://github.com/GeoscienceAustralia/eo-datasets.git',
                    'version': '0.9+13.gd1b5206.dirty'},
                'gqa': {
                    'repo_url': 'https://github.com/GeoscienceAustralia/gqa.git',
                    'version': '0.4+20.gb0d00dc'},
                'pinkmatter': '4.1.4009'
            },
        },
        'source_datasets': {
            'satellite_telemetry_data': {
                'acquisition': {
                    'aos': datetime.datetime(2013, 8, 18, 23, 29, 23),
                    'groundstation': {
                        'code': 'ASA',
                        'eods_domain_code': '002',
                        'label': 'Alice '
                                 'Springs'
                    },
                    'los': datetime.datetime(2013, 8, 18, 23, 34, 16)
                },
                'checksum_path': 'package.sha1',
                'creation_dt': datetime.datetime(2015, 9, 18, 18, 18, 21, 878054),
                'format': {'name': 'RCC',
                           'version': 0},
                'ga_label': 'LS7_ETM_STD-RCC_P00_L7EB2013230232923ASA213_0_0_20130818T232923Z20130818T233416',
                'ga_level': 'P00',
                'id': 'b2a8f768-5e31-11e5-b592-ac162d791418',
                'image': {'bands': {}},
                'instrument': {'name': 'ETM',
                               'operation_mode': 'BUMPER'},
                'lineage': {'machine': {},
                            'source_datasets': {}},
                'platform': {'code': 'LANDSAT_7'},
                'product_type': 'satellite_telemetry_data',
                'size_bytes': 5488246784,
                'usgs': {'interval_id': 'L7EB2013230232923ASA213'}}}},
    'platform': {'code': 'LANDSAT_7'},
    'product_level': 'L1G',
    'product_type': 'level1',
    'size_bytes': 186404,
    'usgs': {'scene_id': 'LE70920822011045ASA00'}
}
