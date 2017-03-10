# coding=utf-8
"""
Package an ls5 Ortho dataset.
"""
from __future__ import absolute_import

import datetime
from pathlib import Path

import yaml

from tests import temp_dir, assert_file_structure, assert_same, integration_test, run_packaging_cli, as_file_list
from tests.integration import load_checksum_filenames, hardlink_arg, prepare_datasets_for_comparison, FakeAncilFile, \
    prepare_work_order

#: :type: Path
source_folder = Path(__file__).parent.joinpath('input', 'ls5-ortho')
assert source_folder.exists()

source_dataset = source_folder.joinpath('data', 'LT5_20060703_108_078')
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
        FakeAncilFile(ancil_base, 'cpf', 'LT05CPF_20060701_20060815_01.03'),
        FakeAncilFile(ancil_base, 'ephemeris', 'L52006185DEFEPH.S00'),
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

    package_name = 'LS5_TM_OTH_P51_GALPGS01-002_108_078_20060703'
    output_dataset = output_path.joinpath(package_name)

    assert_file_structure(output_path, {
        package_name: {
            'browse.jpg': '',
            'browse.fr.jpg': '',
            'product': {
                'LT05_L1TP_108078_20060703_20170309_01_T1_ANG.txt': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B1.TIF': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B2.TIF': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B3.TIF': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B4.TIF': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B5.TIF': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B6.TIF': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B7.TIF': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_BQA.TIF': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_DEM.TIF': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B1.IMD': 'optional',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B2.IMD': 'optional',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B3.IMD': 'optional',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B4.IMD': 'optional',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B5.IMD': 'optional',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B6.IMD': 'optional',
                'LT05_L1TP_108078_20060703_20170309_01_T1_B7.IMD': 'optional',
                'LT05_L1TP_108078_20060703_20170309_01_T1_BQA.IMD': 'optional',
                'LT05_L1TP_108078_20060703_20170309_01_T1_DEM.IMD': 'optional',
                'LT05_L1TP_108078_20060703_20170309_01_T1_GCP.txt': '',
                'LT05_L1TP_108078_20060703_20170309_01_T1_MTL.txt': '',
                'LT5_20060703_108_078_L1TP.xml': '',
            },
            'additional': {
                'work_order.xml': '',
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

    # pprint(md, indent=4)
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
            'cell_size': 11923.75,
            'file_type': 'image/jpg',
            'green_band': '4',
            'path': 'browse.fr.jpg',
            'red_band': '7',
            'shape': {'x': 20, 'y': 20}
        },
        'medium': {
            'blue_band': '1',
            'cell_size': 232.8857421875,
            'file_type': 'image/jpg',
            'green_band': '4',
            'path': 'browse.jpg',
            'red_band': '7',
            'shape': {'x': 1024, 'y': 1024}
        }
    },
    'checksum_path': 'package.sha1',
    'creation_dt': datetime.datetime(2017, 3, 9, 22, 33, 7),
    'extent': {
        'center_dt': datetime.datetime(2006, 7, 3, 1, 32, 0, 191094),
        'coord': {
            'll': {'lat': -26.956882, 'lon': 123.087025},
            'lr': {'lat': -26.934954, 'lon': 125.488525},
            'ul': {'lat': -25.042103, 'lon': 123.085626},
            'ur': {'lat': -25.021952, 'lon': 125.448553}
        },
        'from_dt': datetime.datetime(2006, 7, 3, 1, 31, 46),
        'to_dt': datetime.datetime(2006, 7, 3, 1, 32, 13)
    },
    'format': {'name': 'GeoTIFF'},
    'ga_label': 'LS5_TM_OTH_P51_GALPGS01-002_108_078_20060703',
    'grid_spatial': {
        'projection': {
            'datum': 'GDA94',
            'ellipsoid': 'GRS80',
            'geo_ref_points': {
                'll': {'x': 508637.5,
                       'y': 7018337.5},
                'lr': {'x': 747087.5,
                       'y': 7018337.5},
                'ul': {'x': 508637.5,
                       'y': 7230387.5},
                'ur': {'x': 747087.5,
                       'y': 7230387.5}
            },
            'map_projection': 'UTM',
            'orientation': 'NORTH_UP',
            'resampling_option': 'CUBIC_CONVOLUTION',
            'zone': -51
        }
    },
    'id': None,
    'image': {
        'bands': {
            '1': {'cell_size': 25.0,
                  'label': 'Visible Blue',
                  'number': '1',
                  'path': 'product/LT05_L1TP_108078_20060703_20170309_01_T1_B1.TIF',
                  'type': 'reflective'},
            '2': {'cell_size': 25.0,
                  'label': 'Visible Green',
                  'number': '2',
                  'path': 'product/LT05_L1TP_108078_20060703_20170309_01_T1_B2.TIF',
                  'type': 'reflective'},
            '3': {'cell_size': 25.0,
                  'label': 'Visible Red',
                  'number': '3',
                  'path': 'product/LT05_L1TP_108078_20060703_20170309_01_T1_B3.TIF',
                  'type': 'reflective'},
            '4': {'cell_size': 25.0,
                  'label': 'Near Infrared',
                  'number': '4',
                  'path': 'product/LT05_L1TP_108078_20060703_20170309_01_T1_B4.TIF',
                  'type': 'reflective'},
            '5': {'cell_size': 25.0,
                  'label': 'Middle Infrared 1',
                  'number': '5',
                  'path': 'product/LT05_L1TP_108078_20060703_20170309_01_T1_B5.TIF',
                  'type': 'reflective'},
            '6': {'cell_size': 100.0,
                  'label': 'Thermal Infrared',
                  'number': '6',
                  'path': 'product/LT05_L1TP_108078_20060703_20170309_01_T1_B6.TIF',
                  'type': 'thermal'},
            '7': {'cell_size': 25.0,
                  'label': 'Middle Infrared 2',
                  'number': '7',
                  'path': 'product/LT05_L1TP_108078_20060703_20170309_01_T1_B7.TIF',
                  'type': 'reflective'},
            'qa': {'number': 'qa',
                   'path': 'product/LT05_L1TP_108078_20060703_20170309_01_T1_BQA.TIF'}
        },
        'cloud_cover_percentage': 0.0,
        'geometric_rmse_model': 2.816,
        'geometric_rmse_model_x': 2.138,
        'geometric_rmse_model_y': 1.832,
        'ground_control_points_model': 222,
        'satellite_ref_point_start': {'x': 108, 'y': 78},
        'sun_azimuth': 36.70872365,
        'sun_earth_distance': 1.0166951,
        'sun_elevation': 31.11802924
    },
    'instrument': {'name': 'TM', 'operation_mode': 'BUMPER'},
    'lineage': {
        'algorithm': {
            'name': 'LPGS',
            'parameters': {},
            'version': '12.8.2'
        },
        'ancillary': {
            'cpf': {'name': 'LT05CPF_20060701_20060815_01.03'},
            'ephemeris': {
                'name': 'L52006185DEFEPH.S00',
                'properties': {'type': 'DEFINITIVE'},
            }
        },
        'ancillary_quality': 'DEFINITIVE',
        'machine': {
            'hostname': 'kveikur',
            'runtime_id': 'cd5c4452-0547-11e7-be44-185e0f80a5c0',
            'software_versions': {
                'eodatasets': {
                    'repo_url': 'https://github.com/GeoscienceAustralia/eo-datasets.git',
                    'version': '0.9+18.gb841ad8.dirty'
                },
                'pinkmatter': '4.1.4261'
            }
        },
        'source_datasets': {
            'satellite_telemetry_data': {
                'acquisition': {
                    'aos': datetime.datetime(2006, 7, 3, 1, 25, 2),
                    'groundstation': {
                        'code': 'ASA',
                        'eods_domain_code': '002',
                        'label': 'Alice Springs'
                    },
                    'los': datetime.datetime(2006, 7, 3, 1, 34, 55),
                    'platform_orbit': 118806
                },
                'checksum_path': 'package.sha1',
                'creation_dt': datetime.datetime(2015, 10, 8, 18, 34, 48, 813884),
                'format': {'name': 'RCC'},
                'ga_label': 'LS5_TM_STD-RCC_P00_L5TB2006184012502ASA111_0_0_20060703T012502Z20060703T013455',
                'ga_level': 'P00',
                'id': '417d833c-6deb-11e5-8e98-ac162d791418',
                'image': {'bands': {}},
                'instrument': {'name': 'TM',
                               'operation_mode': 'BUMPER'},
                'lineage': {'machine': {},
                            'source_datasets': {}},
                'platform': {'code': 'LANDSAT_5'},
                'product_type': 'satellite_telemetry_data',
                'size_bytes': 6273995850,
                'usgs': {'interval_id': 'L5TB2006184012502ASA111'}
            }
        }
    },
    'platform': {'code': 'LANDSAT_5'},
    'product_level': 'L1T',
    'product_type': 'level1',
    'size_bytes': 150281,
    'usgs': {
        'scene_id': 'LT51080782006184ASA00'
    }
}

test_package()
