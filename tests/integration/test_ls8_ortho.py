# coding=utf-8
"""
Package an LS8 NBAR dataset.
"""
from __future__ import absolute_import

import datetime

import yaml
from pathlib import Path
from tests import temp_dir, assert_file_structure, assert_same, integration_test, run_packaging_cli

from tests.integration import load_checksum_filenames, hardlink_arg, prepare_datasets_for_comparison, FakeAncilFile, \
    prepare_work_order

#: :type: Path

source_folder = Path(__file__).parent.joinpath('input', 'ls8-ortho')
assert source_folder.exists()

source_dataset = source_folder.joinpath('data', '112_079_079')
assert source_dataset.exists()

parent_dataset = source_folder.joinpath('parent')
assert parent_dataset.exists()

additional_files = source_folder.joinpath('data', 'additional')
assert additional_files.exists()

work_order_template_path = source_folder.joinpath('work_order.xml')


@integration_test
def test_package():
    work_path = temp_dir()
    output_path = work_path.joinpath('out')
    output_path.mkdir(parents=True)
    ancil_path = work_path.joinpath('ancil')

    # We have to override the ancillary directory lookup as they won't exist on test systems.
    ancil_files = (
        FakeAncilFile(ancil_path, 'cpf', 'L8CPF20140101_20140331.05'),
        FakeAncilFile(ancil_path, 'bpf_oli', 'LO8BPF20140127130115_20140127144056.01'),
        FakeAncilFile(ancil_path, 'bpf_tirs', 'LT8BPF20140116023714_20140116032836.02'),
        FakeAncilFile(ancil_path, 'tirs_ssm_position', '20160529.l8_tirs_estimated_ssm_position.txt'),
        FakeAncilFile(ancil_path, 'rlut', 'L8RLUT20130211_20431231v09.h5', folder_offset=('2013',)),
    )
    work_order_path = prepare_work_order(ancil_files, work_order_template_path)

    # Run!
    args = [
        hardlink_arg(output_path, source_dataset),
        'level1',
        '--newly-processed',
        '--parent', str(parent_dataset),
        '--add-file', str(work_order_path)
    ]
    for additional_file in additional_files.iterdir():
        args.extend(['--add-file', str(additional_file)])
    args.extend([
        str(source_dataset),
        str(output_path)
    ])
    run_packaging_cli(args)

    output_dataset = output_path.joinpath('LS8_OLITIRS_OTH_P51_GALPGS01-002_112_079_20140126')

    assert_file_structure(output_path, {
        'LS8_OLITIRS_OTH_P51_GALPGS01-002_112_079_20140126': {
            'browse.jpg': '',
            'browse.fr.jpg': '',
            'product': {
                'LC81120792014026ASA00_B1.TIF': '',
                'LC81120792014026ASA00_B2.TIF': '',
                'LC81120792014026ASA00_B3.TIF': '',
                'LC81120792014026ASA00_B4.TIF': '',
                'LC81120792014026ASA00_B5.TIF': '',
                'LC81120792014026ASA00_B6.TIF': '',
                'LC81120792014026ASA00_B7.TIF': '',
                'LC81120792014026ASA00_B8.TIF': '',
                'LC81120792014026ASA00_B9.TIF': '',
                'LC81120792014026ASA00_B10.TIF': '',
                'LC81120792014026ASA00_B11.TIF': '',
                'LC81120792014026ASA00_BQA.TIF': '',
                'LC81120792014026ASA00_GCP.txt': '',
                'LC81120792014026ASA00_MTL.txt': '',
                'LO8_20140126_112_079_L1T.xml': '',
            },
            'additional': {
                'work_order.xml': '',
                'lpgs_out.xml': '',
                '20141201_20010425_B6_gqa_results.yaml': ''
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
        'additional/20141201_20010425_B6_gqa_results.yaml',
        'additional/lpgs_out.xml',
        'additional/work_order.xml',
        'browse.fr.jpg',
        'browse.jpg',
        'ga-metadata.yaml',
        'product/LC81120792014026ASA00_B1.TIF',
        'product/LC81120792014026ASA00_B10.TIF',
        'product/LC81120792014026ASA00_B11.TIF',
        'product/LC81120792014026ASA00_B2.TIF',
        'product/LC81120792014026ASA00_B3.TIF',
        'product/LC81120792014026ASA00_B4.TIF',
        'product/LC81120792014026ASA00_B5.TIF',
        'product/LC81120792014026ASA00_B6.TIF',
        'product/LC81120792014026ASA00_B7.TIF',
        'product/LC81120792014026ASA00_B8.TIF',
        'product/LC81120792014026ASA00_B9.TIF',
        'product/LC81120792014026ASA00_BQA.TIF',
        'product/LC81120792014026ASA00_GCP.txt',
        'product/LC81120792014026ASA00_MTL.txt',
        'product/LO8_20140126_112_079_L1T.xml'
    ]
    assert set(checksummed_filenames) == set(expected_filenames)
    assert checksummed_filenames == expected_filenames


EXPECTED_METADATA = {
    'id': None,
    'product_type': 'level1',
    'ga_label': 'LS8_OLITIRS_OTH_P51_GALPGS01-002_112_079_20140126',
    'checksum_path': 'package.sha1',
    'size_bytes': 258695,
    'instrument': {'name': 'OLI_TIRS'},
    'usgs': {
        'scene_id': 'LC81120792014026ASA00'
    },
    'format': {'name': 'GeoTIFF'},
    # Creation date comes from the MTL.
    'creation_dt': datetime.datetime(2015, 4, 7, 0, 58, 8),
    'platform': {'code': 'LANDSAT_8'},
    'product_level': 'L1T',
    'extent':
        {
            'coord':
                {
                    'll': {'lat': -28.49412, 'lon': 116.58121},
                    'ul': {'lat': -26.37259, 'lon': 116.58914},
                    'ur': {'lat': -26.36025, 'lon': 118.92432},
                    'lr': {'lat': -28.48062, 'lon': 118.96145}
                },
            'center_dt': datetime.datetime(2014, 1, 26, 2, 5, 23, 126373)
        },
    'image':
        {
            'cloud_cover_percentage': 0.62,
            'bands':
                {

                    '8': {
                        'number': '8',
                        'type': 'panchromatic',
                        'cell_size': 12.5,
                        'label': 'Panchromatic',
                        'path': 'product/LC81120792014026ASA00_B8.TIF'
                    },

                    '2': {
                        'number': '2',
                        'type': 'reflective',
                        'cell_size': 25.0,
                        'label': 'Visible Blue',
                        'path': 'product/LC81120792014026ASA00_B2.TIF'
                    },

                    '7': {
                        'number': '7',
                        'type': 'reflective',
                        'cell_size': 25.0,
                        'label': 'Short-wave Infrared 2',
                        'path': 'product/LC81120792014026ASA00_B7.TIF'
                    },
                    'qa': {
                        'number': 'qa',
                        'type': 'quality',
                        'cell_size': 25.0,
                        'label': 'Quality',
                        'path': 'product/LC81120792014026ASA00_BQA.TIF'
                    },
                    '3': {
                        'number': '3',
                        'type': 'reflective',
                        'cell_size': 25.0,
                        'label': 'Visible Green',
                        'path': 'product/LC81120792014026ASA00_B3.TIF'
                    },
                    '9': {
                        'number': '9',
                        'type': 'atmosphere',
                        'cell_size': 25.0,
                        'label': 'Cirrus',
                        'path': 'product/LC81120792014026ASA00_B9.TIF'
                    },

                    '6': {
                        'number': '6',
                        'type': 'reflective',
                        'cell_size': 25.0,
                        'label': 'Short-wave Infrared 1',
                        'path': 'product/LC81120792014026ASA00_B6.TIF'
                    },

                    '11': {
                        'number': '11',
                        'type': 'thermal',
                        'cell_size': 25.0,
                        'label': 'Thermal Infrared 2',
                        'path': 'product/LC81120792014026ASA00_B11.TIF'
                    },

                    '5': {
                        'number': '5',
                        'type': 'reflective',
                        'cell_size': 25.0,
                        'label': 'Near Infrared',
                        'path': 'product/LC81120792014026ASA00_B5.TIF'
                    },

                    '4': {
                        'number': '4',
                        'type': 'reflective',
                        'cell_size': 25.0,
                        'label': 'Visible Red',
                        'path': 'product/LC81120792014026ASA00_B4.TIF'
                    },

                    '10': {
                        'number': '10',
                        'type': 'thermal',
                        'cell_size': 25.0,
                        'label': 'Thermal Infrared 1',
                        'path': 'product/LC81120792014026ASA00_B10.TIF'
                    },
                    '1': {
                        'number': '1',
                        'type': 'reflective',
                        'cell_size': 25.0,
                        'label': 'Coastal Aerosol',
                        'path': 'product/LC81120792014026ASA00_B1.TIF'
                    }
                },
            'sun_azimuth': 82.05926755,
            'geometric_rmse_model_y': 3.975,
            'satellite_ref_point_start': {'x': 112, 'y': 79},
            'geometric_rmse_model': 5.458,
            'sun_elevation': 57.68594289,
            'sun_earth_distance': 0.9845943,
            'ground_control_points_model': 380,
            'geometric_rmse_model_x': 3.74
        },
    'grid_spatial': {
        'projection': {
            'zone': -50,
            'ellipsoid': 'GRS80',
            'map_projection': 'UTM',
            'orientation': 'NORTH_UP',
            'datum': 'GDA94',
            'resampling_option': 'CUBIC_CONVOLUTION',
            'geo_ref_points': {
                'll': {'x': 459012.5, 'y': 6847987.5},
                'ul': {'x': 459012.5, 'y': 7082987.5},
                'ur': {'x': 692012.5, 'y': 7082987.5},
                'lr': {'x': 692012.5, 'y': 6847987.5}
            }
        }
    },
    'gqa': {
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
    },
    'acquisition':
        {
            'groundstation': {
                'code': 'ASA',
                'label': 'Alice Springs',
                'eods_domain_code': '002'
            }
        },
    'browse': {
        'full': {
            'cell_size': 25.0,
            'file_type': 'image/jpg',
            'path': 'browse.fr.jpg',
            'red_band': '7',
            'green_band': '5',
            'shape': {'x': 10, 'y': 10},
            'blue_band': '2'
        },
        'medium': {
            'cell_size': 0.244140625,
            'file_type': 'image/jpg',
            'path': 'browse.jpg',
            'red_band': '7',
            'green_band': '5',
            'shape': {'x': 1024, 'y': 1024},
            'blue_band': '2'
        }
    },
    'lineage':
        {
            'machine': {
                'software_versions': {
                    'pinkmatter': '4.0.3616',
                    'gqa': {
                        'repo_url': 'https://github.com/GeoscienceAustralia/gqa.git',
                        'version': '0.4+20.gb0d00dc'
                    }
                }
            },
            'ancillary': {
                'cpf': {
                    'name': 'L8CPF20140101_20140331.05'
                },
                'bpf_oli': {
                    'name': 'LO8BPF20140127130115_20140127144056.01'
                },
                'bpf_tirs': {
                    'name': 'LT8BPF20140116023714_20140116032836.02'
                },
                'rlut': {
                    'name': 'L8RLUT20130211_20431231v09.h5'
                },
                'tirs_ssm_position': {
                    'name': '20160529.l8_tirs_estimated_ssm_position.txt'
                }
            },

            'algorithm': {
                'parameters': {},
                'name': 'LPGS',
                'version': '2.4.0'
            },
            'source_datasets':
                {

                    'satellite_telemetry_data':
                        {
                            'product_type': 'satellite_telemetry_data',
                            'instrument': {'name': 'OLI_TIRS'},
                            'ga_label': ('LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00'
                                         '_116_074_20150330T022553Z20150330T022657'),
                            'image': {
                                'satellite_ref_point_end': {'x': 116, 'y': 74},
                                'satellite_ref_point_start': {'x': 116, 'y': 74}
                            },
                            'checksum_path': 'package.sha1',
                            'ga_level': 'P00',
                            'id': '4ec8fe97-e8b9-11e4-87ff-1040f381a756',
                            'usgs': {
                                'interval_id': 'LC81160740742015089ASA00'
                            },
                            'size_bytes': 637660782,
                            'creation_dt': datetime.datetime(2015, 4, 22, 6, 32, 4),
                            'acquisition':
                                {

                                    'groundstation': {'code': 'ASA'},
                                    'aos': datetime.datetime(2015, 3, 30, 2, 25, 53, 346000),
                                    'los': datetime.datetime(2015, 3, 30, 2, 26, 57, 325000),
                                    'platform_orbit': 11308
                                },
                            'lineage':
                                {
                                    'machine':
                                        {
                                            'hostname': 'niggle.local',
                                            'type_id': 'jobmanager',
                                            'version': '2.4.0',
                                            'uname': 'Darwin niggle.local 14.3.0 Darwin Kernel Version 14.3.0: '
                                                     'Mon Mar 23 11:59:05 PDT 2015; '
                                                     'root:xnu-2782.20.48~5/RELEASE_X86_64 x86_64',
                                            'runtime_id': '4bc6225c-e8b9-11e4-8b66-1040f381a756',

                                        },
                                    'source_datasets': {}
                                },
                            'platform': {'code': 'LANDSAT_8'},
                            'format': {'name': 'MD'}
                        }
                }
        }
}
