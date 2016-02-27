# coding=utf-8
"""
Package an LS7 Ortho dataset.
"""
from __future__ import absolute_import

import datetime

import yaml
from pathlib import Path

from tests import temp_dir, assert_file_structure, assert_same, integration_test, run_packaging_cli
from tests.integration import load_checksum_filenames, hardlink_arg, prepare_datasets_for_comparison, FakeAncilFile, \
    prepare_work_order

#: :type: Path
source_folder = Path(__file__).parent.joinpath('input', 'ls7-ortho')
assert source_folder.exists()

source_dataset = source_folder.joinpath('data', 'LE7_20130818_088_075')
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
        FakeAncilFile(ancil_base, 'cpf', 'L8CPF20140101_20140331.05'),
        FakeAncilFile(ancil_base, 'ephemeris', 'L72013231ASADEF.S00'),
    )
    work_order = prepare_work_order(ancil_files, wo_template)

    # Run!
    args = [
        hardlink_arg(output_path, source_dataset),
        'ortho',
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

    output_dataset = output_path.joinpath('LS7_ETM_SYS_P31_GALPGS01-002_088_075_20130818')

    assert_file_structure(output_path, {
        'LS7_ETM_SYS_P31_GALPGS01-002_088_075_20130818': {
            'browse.jpg': '',
            'browse.fr.jpg': '',
            'product': {
                'LE70880752013230ASA00_B1.IMD': '',
                'LE70880752013230ASA00_B1.TIF': '',
                'LE70880752013230ASA00_B2.IMD': '',
                'LE70880752013230ASA00_B2.TIF': '',
                'LE70880752013230ASA00_B3.IMD': '',
                'LE70880752013230ASA00_B3.TIF': '',
                'LE70880752013230ASA00_B4.IMD': '',
                'LE70880752013230ASA00_B4.TIF': '',
                'LE70880752013230ASA00_B5.IMD': '',
                'LE70880752013230ASA00_B5.TIF': '',
                'LE70880752013230ASA00_B6_VCID_1.IMD': '',
                'LE70880752013230ASA00_B6_VCID_1.TIF': '',
                'LE70880752013230ASA00_B6_VCID_2.IMD': '',
                'LE70880752013230ASA00_B6_VCID_2.TIF': '',
                'LE70880752013230ASA00_B7.IMD': '',
                'LE70880752013230ASA00_B7.TIF': '',
                'LE70880752013230ASA00_B8.IMD': '',
                'LE70880752013230ASA00_B8.TIF': '',
                'LE70880752013230ASA00_MTL.txt': '',
                'LE7_20130818_088_075_L1G.xml': '',
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

    expected_filenames = [
        'additional/20130818_20000119_B5_gqa_results.yaml',
        'additional/lpgs_out.xml',
        'additional/work_order.xml',
        'browse.fr.jpg',
        'browse.jpg',
        'ga-metadata.yaml',
        'product/LE70880752013230ASA00_B1.IMD',
        'product/LE70880752013230ASA00_B1.TIF',
        'product/LE70880752013230ASA00_B2.IMD',
        'product/LE70880752013230ASA00_B2.TIF',
        'product/LE70880752013230ASA00_B3.IMD',
        'product/LE70880752013230ASA00_B3.TIF',
        'product/LE70880752013230ASA00_B4.IMD',
        'product/LE70880752013230ASA00_B4.TIF',
        'product/LE70880752013230ASA00_B5.IMD',
        'product/LE70880752013230ASA00_B5.TIF',
        'product/LE70880752013230ASA00_B6_VCID_1.IMD',
        'product/LE70880752013230ASA00_B6_VCID_1.TIF',
        'product/LE70880752013230ASA00_B6_VCID_2.IMD',
        'product/LE70880752013230ASA00_B6_VCID_2.TIF',
        'product/LE70880752013230ASA00_B7.IMD',
        'product/LE70880752013230ASA00_B7.TIF',
        'product/LE70880752013230ASA00_B8.IMD',
        'product/LE70880752013230ASA00_B8.TIF',
        'product/LE70880752013230ASA00_MTL.txt',
        'product/LE7_20130818_088_075_L1G.xml',
    ]
    assert set(checksummed_filenames) == set(expected_filenames)
    assert checksummed_filenames == expected_filenames


EXPECTED_METADATA = {
    'checksum_path': 'package.sha1',
    'extent': {
        'center_dt': datetime.datetime(2013, 8, 18, 23, 29, 40, 486803), 'coord': {
            'ul': {
                'lat': -20.701634, 'lon': 155.103565}, 'll': {
                'lat': -22.576483, 'lon': 155.052864}, 'ur': {
                'lat': -20.738476, 'lon': 157.415391}, 'lr': {
                'lat': -22.617006, 'lon': 157.394718}}}, 'acquisition': {
        'groundstation': {
            'code': 'ASA', 'label': 'Alice Springs', 'eods_domain_code': '002'
        }
    }, 'platform': {'code': 'LANDSAT_7'},
    'ga_label': 'LS7_ETM_SYS_P31_GALPGS01-002_088_075_20130818',
    'browse': {
        'medium': {
            'shape': {
                'y': 1024, 'x': 1024}, 'path': 'browse.jpg', 'green_band': '4', 'file_type': 'image/jpg',
            'blue_band': '1', 'red_band': '7', 'cell_size': 235.3759765625}, 'full': {
            'shape': {
                'y': 20, 'x': 20}, 'path': 'browse.fr.jpg', 'green_band': '4', 'file_type': 'image/jpg',
            'blue_band': '1', 'red_band': '7', 'cell_size': 12051.25
        }
    },
    'id': None,
    'creation_dt': datetime.datetime(2016, 2, 26, 1, 39, 4),
    'product_level': 'L1G',
    'product_type': 'ortho',
    'gqa': {
        'colors': {
            'teal': 0.0, 'blue': 10.0, 'red': 274.0, 'green': 0.0, 'yellow': 25.0}, 'band': '5',
        'acq_day': datetime.date(2013, 8, 18),
        'abs_iterative_mean_residual': {
            'y': 4.081039145021645,
            'x': 8.281189264069264},
        'ref_source': 'GLS_v1', 'cep90': 440.00546272953613,
        'final_gcp_count': 308,
        'stddev_residual': {
            'y': 4.524645392606803, 'x': 4.23604905517344},
        'mean_residual': {
            'y': 0.9739366558441558, 'x': 8.281189264069264},
        'iterative_stddev_residual': {
            'y': 4.524645392606803, 'x': 4.23604905517344},
        'ref_day': datetime.date(2000, 1, 19),
        'residual': {
            'y': 4.070382, 'x': 8.266335},
        'iterative_mean_residual': {
            'y': 0.9739366558441558, 'x': 8.281189264069264}
    },
    'size_bytes': 21280,
    'grid_spatial': {
        'projection': {
            'map_projection': 'UTM',
            'geo_ref_points': {
                'ul': {
                    'y': 7705987.5, 'x': 94012.5}, 'll': {
                    'y': 7497987.5, 'x': 94012.5}, 'ur': {
                    'y': 7705987.5, 'x': 335012.5}, 'lr': {
                    'y': 7497987.5, 'x': 335012.5}
            },
            'zone': -57,
            'orientation': 'NORTH_UP',
            'datum': 'GDA94',
            'ellipsoid': 'GRS80',
            'resampling_option': 'CUBIC_CONVOLUTION'
        }
    },
    'instrument': {
        'name': 'ETM', 'operation_mode': 'BUMPER'},
    'usgs': {
        'scene_id': 'LE70880752013230ASA00'
    },
    'image': {
        'sun_azimuth': 45.5704609, 'sun_elevation': 43.19768592,
        'bands': {
            '7': {
                'path': 'product/LE70880752013230ASA00_B7.TIF',
                'label': 'Middle Infrared 2',
                'number': '7',
                'type': 'reflective', 'cell_size': 25.0},
            '4': {
                'path': 'product/LE70880752013230ASA00_B4.TIF',
                'label': 'Near Infrared',
                'number': '4',
                'type': 'reflective', 'cell_size': 25.0},
            '6_vcid_1': {
                'path': 'product/LE70880752013230ASA00_B6_VCID_1.TIF',
                'label': 'Thermal Infrared [Low Gain]',
                'number': '6_vcid_1', 'type': 'thermal',
                'cell_size': 50.0},
            '1': {
                'path': 'product/LE70880752013230ASA00_B1.TIF',
                'label': 'Visible Blue',
                'number': '1',
                'type': 'reflective', 'cell_size': 25.0},
            '2': {
                'path': 'product/LE70880752013230ASA00_B2.TIF', 'label': 'Visible Green', 'number': '2',
                'type': 'reflective', 'cell_size': 25.0},
            '3': {
                'path': 'product/LE70880752013230ASA00_B3.TIF', 'label': 'Visible Red', 'number': '3',
                'type': 'reflective', 'cell_size': 25.0},
            '8': {
                'path': 'product/LE70880752013230ASA00_B8.TIF', 'label': 'Panchromatic', 'number': '8',
                'type': 'panchromatic', 'cell_size': 12.5},
            '6_vcid_2': {
                'path': 'product/LE70880752013230ASA00_B6_VCID_2.TIF', 'label': 'Thermal Infrared [High Gain]',
                'number': '6_vcid_2', 'type': 'thermal', 'cell_size': 50.0},
            '5': {
                'path': 'product/LE70880752013230ASA00_B5.TIF', 'label': 'Middle Infrared 1', 'number': '5',
                'type': 'reflective', 'cell_size': 25.0
            }
        },
        'sun_earth_distance': 1.012089,
        'satellite_ref_point_start': {
            'y': 75, 'x': 88},
        'cloud_cover_percentage': 0.0},
    'format': {
        'name': 'GEOTIFF'
    },
    'lineage': {
        'machine': {
            'runtime_id': 'fa9f288a-dcf3-11e5-a095-0023dfa0db82',
            'software_versions': {
                'pinkmatter': '4.1.4009'
            },
            'hostname': 'witzo.local'
        },
        'ancillary': {
            'cpf': {
                'name': 'L8CPF20140101_20140331.05',
            },
            'ephemeris': {
                'name': 'L72013231ASADEF.S00'
            }
        },
        'algorithm': {
            'name': 'LPGS',
            'parameters': {},
            'version': '12.6.1'
        },
        'ancillary_quality': 'DEFINITIVE',
        'source_datasets': {
            'satellite_telemetry_data': {
                'lineage': {
                    'source_datasets': {},
                    'machine': {}
                },
                'creation_dt': datetime.datetime(2015, 9, 18, 18, 18, 21, 878054),
                'product_type': 'satellite_telemetry_data',
                'checksum_path': 'package.sha1',
                'acquisition': {
                    'los': datetime.datetime(2013, 8, 18, 23, 34, 16),
                    'aos': datetime.datetime(2013, 8, 18, 23, 29, 23),
                    'groundstation': {
                        'code': 'ASA', 'label': 'Alice Springs', 'eods_domain_code': '002'
                    }
                }, 'ga_level': 'P00',
                'platform': {
                    'code': 'LANDSAT_7'},
                'ga_label': 'LS7_ETM_STD-RCC_P00_L7EB2013230232923ASA213_0_0_20130818T232923Z20130818T233416',
                'instrument': {
                    'name': 'ETM', 'operation_mode': 'BUMPER'},
                'usgs': {
                    'interval_id': 'L7EB2013230232923ASA213'},
                'id': 'b2a8f768-5e31-11e5-b592-ac162d791418',
                'image': {
                    'bands': {
                    }}, 'size_bytes': 5488246784, 'format': {
                    'name': 'RCC', 'version': 0
                }
            }
        },
    }
}
