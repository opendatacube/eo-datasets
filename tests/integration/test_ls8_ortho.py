# coding=utf-8
"""
Package an LS8 NBAR dataset.
"""
from __future__ import absolute_import

import datetime
import shutil
import socket

import yaml
from pathlib import Path

from eodatasets.metadata.ortho import _get_lpgs_out, _get_work_order
from eodatasets.package import _RUNTIME_ID
from tests import temp_dir, assert_file_structure, assert_same, integration_test, run_packaging_cli
from tests.integration import load_checksum_filenames, hardlink_arg, directory_size, add_default_software_versions

#: :type: Path
source_folder = Path(__file__).parent.joinpath('input', 'ls8-ortho')
assert source_folder.exists()

source_dataset = source_folder.joinpath('data', '112_079_079')
assert source_dataset.exists()

parent_dataset = source_folder.joinpath('parent')
assert parent_dataset.exists()

gqa_file = source_folder.joinpath('20141201_20010425_B6_gqa_results.csv')
assert gqa_file.exists()


@integration_test
def test_package():
    work_path = temp_dir()
    output_path = work_path.joinpath('out')
    output_path.mkdir(parents=True)
    ancil_path = work_path.joinpath('ancil')
    input_product_path = work_path.joinpath('product')

    # We have to override the ancillary directory lookup as they won't exist on test systems.
    ANCIL_FILES = {
        'cpf': (
            ancil_path.joinpath('cpf'),
            ('L8CPF20140101_20140331.05',),
            'da39a3ee5e6b4b0d3255bfef95601890afd80709'
        ),
        'bpf_oli': (
            ancil_path.joinpath('bpf-oli'),
            ('LO8BPF20140127130115_20140127144056.01',),
            'da39a3ee5e6b4b0d3255bfef95601890afd80709'
        ),
        'bpf_tirs': (
            ancil_path.joinpath('bpf-tirs'),
            ('LT8BPF20140116023714_20140116032836.02',),
            'da39a3ee5e6b4b0d3255bfef95601890afd80709'
        ),
        'rlut': (
            ancil_path.joinpath('rlut'),
            # It should search subdirectories too.
            ('2013', 'L8RLUT20130211_20431231v09.h5'),
            'da39a3ee5e6b4b0d3255bfef95601890afd80709'
        )
    }
    for name, (dir, file_offset, _) in ANCIL_FILES.items():
        # Create directories
        dir.joinpath(*file_offset[:-1]).mkdir(parents=True)
        # Create blank ancil file.
        dir.joinpath(*file_offset).open('w').close()

    # Write all our input data to a temp directory (so that we can use a custom work order)
    shutil.copytree(str(source_dataset), str(input_product_path))
    shutil.copy(
        str(_get_lpgs_out(source_dataset)),
        str(work_path.joinpath('lpgs_out.xml'))
    )
    output_work_order = work_path.joinpath('work_order.xml')

    # Write a work order with ancillary locations replaced.
    with _get_work_order(source_dataset).open('rb') as wo:
        wo_text = wo.read().decode('utf-8').format(
            **{k + '_path': v[0] for k, v in ANCIL_FILES.items()}
        )
        with output_work_order.open('w') as out_wo:
            out_wo.write(wo_text)

    # Run!
    run_packaging_cli([
        hardlink_arg(output_path, source_dataset),
        'ortho', '--newly-processed',
        '--parent', str(parent_dataset),
        '--add-file', str(output_work_order),
        '--add-file', str(gqa_file),
        str(input_product_path), str(output_path)
    ])

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
                '20141201_20010425_B6_gqa_results.csv': ''
            },
            'ga-metadata.yaml': '',
            'package.sha1': ''
        }
    })

    # TODO: Check metadata fields are sensible.
    output_metadata_path = output_dataset.joinpath('ga-metadata.yaml')
    assert output_metadata_path.exists()
    md = yaml.load(output_metadata_path.open('r'))

    # ID is different every time: check not none, and clear it.
    assert md['id'] is not None
    md['id'] = None

    EXPECTED_METADATA['size_bytes'] = directory_size(output_dataset / 'product')
    add_default_software_versions(EXPECTED_METADATA)

    # A newly-processed dataset: extra fields
    assert md['lineage']['machine']['uname'] is not None
    del md['lineage']['machine']['uname']
    EXPECTED_METADATA['lineage']['machine']['runtime_id'] = str(_RUNTIME_ID)
    EXPECTED_METADATA['lineage']['machine']['hostname'] = socket.getfqdn()

    # Create the expected ancillary information.
    for ancil_name, ancil_d in EXPECTED_METADATA['lineage']['ancillary'].items():
        assert ancil_name in ANCIL_FILES, 'Unexpected ancil type: ' + ancil_name
        dir_, file_offset, chk = ANCIL_FILES[ancil_name]
        #: :type: pathlib.Path
        ancil_path = dir_.joinpath(*file_offset)
        ancil_d['uri'] = str(ancil_path)
        ancil_d['modification_dt'] = datetime.datetime.fromtimestamp(ancil_path.stat().st_mtime)
        ancil_d['checksum_sha1'] = chk

    assert_same(md, EXPECTED_METADATA)

    # TODO: Asset all files are checksummed.
    output_checksum_path = output_dataset.joinpath('package.sha1')
    assert output_checksum_path.exists()
    checksummed_filenames = load_checksum_filenames(output_checksum_path)

    expected_filenames = [
        'additional/20141201_20010425_B6_gqa_results.csv',
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
    'product_type': 'ortho',
    'ga_label': 'LS8_OLITIRS_OTH_P51_GALPGS01-002_112_079_20140126',
    'checksum_path': 'package.sha1',
    'size_bytes': 258695,
    'instrument': {'name': 'OLI_TIRS'},
    'usgs': {
        'scene_id': 'LC81120792014026ASA00'
    },
    'format': {'name': 'GEOTIFF'},
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
        'abs_iterative_mean_residual_x': 1.3,
        'abs_iterative_mean_residual_y': 1.2,
        'acq_day': datetime.date(2014, 12, 1),
        # Bands are always strings. They can have odd names ("6_vcid_1").
        'band': '6',
        'blue': 120,
        'cep90': 212.0,
        'final_gcp_count': 1493,
        'green': 340,
        'iterative_mean_residual_x': -0.4,
        'iterative_mean_residual_y': 0.5,
        'iterative_stddev_residual_x': 2.5,
        'iterative_stddev_residual_y': 2.5,
        'mean_residual_x': -0.4,
        'mean_residual_y': 0.5,
        'red': 321,
        'ref_day': datetime.date(2001, 4, 25),
        'residual_x': 1.9,
        'residual_y': 1.8,
        'sceneid': 'LS8_OLITIRS_OTH_P51_GALPGS01-032_099_072_20141201',
        'stddev_residual_x': 3.6,
        'stddev_residual_y': 3.6,
        'teal': 735,
        'yellow': 98,
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
                    'pinkmatter': '4.0.3616'
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
