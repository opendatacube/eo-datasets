# coding=utf-8
"""
Package an LS8 PQA dataset.
"""
from __future__ import absolute_import
from subprocess import check_call
import datetime

from pathlib import Path
import yaml

import eodatasets.scripts.genpackage
from tests import temp_dir, assert_file_structure, assert_same, integration_test
from tests.integration import get_script_path, load_checksum_filenames, hardlink_arg

packaging_script_path = get_script_path(eodatasets.scripts.genpackage)

#: :type: Path
source_folder = Path(__file__).parent.joinpath('input', 'ls8-pqa')
assert source_folder.exists()

source_dataset = source_folder.joinpath('data')
assert source_dataset.exists()

parent_dataset = source_folder.joinpath('parent')
assert parent_dataset.exists()


@integration_test
def test_package():
    output_path = temp_dir()

    check_call(
        [
            'python',
            str(packaging_script_path),
            hardlink_arg(output_path, source_dataset),
            'pqa',
            '--parent', str(parent_dataset),
            str(source_dataset), str(output_path)
        ]
    )

    output_dataset = output_path.joinpath('LS8_OLITIRS_PQ_P55_GAPQ01-032_090_081_20140726')

    assert_file_structure(output_path, {
        'LS8_OLITIRS_PQ_P55_GAPQ01-032_090_081_20140726': {
            'browse.jpg': '',
            'browse.fr.jpg': '',
            'product': {
                'LS8_OLITIRS_PQ_P55_GAPQ01-032_090_081_20140726.tif': '',
            },
            'ga-metadata.yaml': '',
            'package.sha1': ''
        }
    })

    # Load metadata file and compare it to expected.
    output_checksum_path = output_dataset.joinpath('ga-metadata.yaml')
    assert output_checksum_path.exists()
    md = yaml.load(output_checksum_path.open('r'))

    # ID is different every time: check not none, and clear it.
    assert md['id'] is not None
    md['id'] = None

    # Check metadata is as expected.
    assert_same(
        md,
        {
            'format': {'name': 'GeoTIFF'},
            'ga_label': 'LS8_OLITIRS_PQ_P55_GAPQ01-032_090_081_20140726',
            'checksum_path': 'package.sha1',
            'id': None,
            'size_bytes': 2831,
            'ga_level': 'P55',
            'platform': {'code': 'LANDSAT_8'},
            'instrument': {'name': 'OLI_TIRS'},
            'product_type': 'pqa',
            # Default creation date is the same as the input folder ctime.
            'creation_dt': datetime.datetime.utcfromtimestamp(source_dataset.stat().st_ctime),
            'extent': {
                'center_dt': datetime.datetime(2014, 7, 26, 23, 49, 0, 343853),
                'coord': {
                    'll': {'lat': -31.33333, 'lon': 149.78434},
                    'lr': {'lat': -31.37116, 'lon': 152.20094},
                    'ul': {'lat': -29.23394, 'lon': 149.85216},
                    'ur': {'lat': -29.26873, 'lon': 152.21782}
                }
            },
            'image': {
                'satellite_ref_point_start': {'x': 90, 'y': 81},
                'bands': {
                    'pqa': {
                        'path': 'product/LS8_OLITIRS_PQ_P55_GAPQ01-032_090_081_20140726.tif',
                        'number': 'pqa'
                    }
                }
            },
            'acquisition': {
                'groundstation': {
                    'label': 'Landsat Ground Network',
                    'eods_domain_code': '032',
                    'code': 'LGN'
                }
            },
            'browse': {
                'medium': {
                    'cell_size': 0.9765625,
                    'file_type': 'image/jpg',
                    'shape': {'x': 1024, 'y': 1024},
                    'path': 'browse.jpg',
                    'green_band': 'pqa',
                    'red_band': 'pqa',
                    'blue_band': 'pqa'
                },
                'full': {
                    'cell_size': 25.0,
                    'file_type': 'image/jpg',
                    'shape': {'x': 40, 'y': 40},
                    'path': 'browse.fr.jpg',

                    'green_band': 'pqa',
                    'red_band': 'pqa',
                    'blue_band': 'pqa'
                }
            },
            'lineage': {
                'source_datasets': {
                    'nbar_brdf': {
                        'ga_level': 'P54',
                        'extent': {
                            'center_dt': datetime.datetime(2014, 7, 26, 23, 49, 0, 343853),
                            'coord': {
                                'll': {'lat': -31.33333, 'lon': 149.78434},
                                'lr': {'lat': -31.37116, 'lon': 152.20094},
                                'ul': {'lat': -29.23394, 'lon': 149.85216},
                                'ur': {'lat': -29.26873, 'lon': 152.21782}
                            }
                        },
                        'platform': {'code': 'LANDSAT_8'},
                        'instrument': {'name': 'OLI_TIRS'},
                        'product_type': 'nbar_brdf',
                        'creation_dt': datetime.datetime(2015, 5, 8, 0, 26, 22),
                        'format': {'name': 'GeoTIFF'},
                        'ga_label': 'LS8_OLITIRS_NBAR_P54_GALPGS01-032_090_081_20140726',
                        'image': {
                            'satellite_ref_point_start': {'x': 90, 'y': 81},
                            'bands': {}
                        },
                        'acquisition': {
                            'groundstation': {
                                'label': 'Landsat Ground Network',
                                'eods_domain_code': '032',
                                'code': 'LGN'
                            }
                        },
                        'checksum_path': 'package.sha1',
                        'id': 'eb858ce1-f87c-11e4-a817-1040f381a756',
                        'size_bytes': 0,
                        'lineage': {
                            'source_datasets': {
                                'ortho': {
                                    'id': '7bff72fc-e96d-11e4-b15e-a0000100fe80',
                                    'size_bytes': 1758484367,
                                    'ga_label': 'LS8_OLITIRS_OTH_P51_GALPGS01-032_090_081_20140726',
                                    'platform': {'code': 'LANDSAT_8'},
                                    'instrument': {'name': 'OLI_TIRS'},
                                    'usgs': {
                                        'scene_id': 'LC80900812014207LGN00'
                                    },
                                    'product_type': 'ortho',
                                    'creation_dt': datetime.datetime(2015, 4, 7, 3, 25, 59),
                                    'checksum_path': 'package.sha1',
                                    'format': {'name': 'GEOTIFF'},
                                    'extent': {
                                        'center_dt': datetime.datetime(2014, 7, 26, 23, 49, 0, 343853),
                                        'coord': {
                                            'll': {'lat': -31.33333, 'lon': 149.78434},
                                            'lr': {'lat': -31.37116, 'lon': 152.20094},
                                            'ul': {'lat': -29.23394, 'lon': 149.85216},
                                            'ur': {'lat': -29.26873, 'lon': 152.21782}
                                        }
                                    },
                                    'acquisition': {'groundstation': {'code': 'LGN'}},
                                    'grid_spatial': {
                                        'projection': {
                                            'map_projection': 'UTM',
                                            'datum': 'GDA94',
                                            'geo_ref_points': {
                                                'll': {'x': 194012.5, 'y': 6528987.5},
                                                'lr': {'x': 424012.5, 'y': 6528987.5},
                                                'ul': {'x': 194012.5, 'y': 6761987.5},
                                                'ur': {'x': 424012.5, 'y': 6761987.5}
                                            },
                                            'resampling_option': 'CUBIC_CONVOLUTION',
                                            'ellipsoid': 'GRS80',
                                            'orientation': 'NORTH_UP',
                                            'zone': -56
                                        }
                                    },
                                    'browse': {
                                        'medium': {
                                            'cell_size': 224.6337890625,
                                            'file_type': 'image/jpg',
                                            'shape': {'x': 1024, 'y': 1038},
                                            'path': 'browse.jpg',
                                            'green_band': '5',
                                            'red_band': '7',
                                            'blue_band': '2'},

                                        'full': {
                                            'cell_size': 25.0,
                                            'file_type': 'image/jpg',
                                            'shape': {'x': 9201, 'y': 9321},
                                            'path': 'browse.fr.jpg',
                                            'green_band': '5',
                                            'red_band': '7',
                                            'blue_band': '2'
                                        }
                                    },
                                    'lineage': {
                                        'algorithm': {
                                            'version': '2.4.0',
                                            'name': 'LPGS',
                                            'parameters': {}
                                        },
                                        'ancillary': {
                                            'bpf_oli': {'name': 'LO8BPF20140712102915_20140712112424.01'},
                                            'bpf_tirs': {'name': 'LT8BPF20140726120951_20140726131856.01'},
                                            'cpf': {'name': 'L8CPF20140701_20140930.03'},
                                            'rlut': {'name': 'L8RLUT20130211_20431231v09.h5'}
                                        },
                                        'machine': {
                                            'version': '2.4.0',
                                            'runtime_id': 'ddce56fe-e960-11e4-b15e-a0000100fe80',
                                            'hostname': 'r2830',
                                            'uname': 'Linux r2830 2.6.32-504.12.2.el6.x86_64 '
                                                     '#1 SMP Wed Mar 11 22:03:14 UTC 2015 x86_64',
                                            'type_id': 'jobmanager'
                                        },
                                        'source_datasets': {
                                            'satellite_telemetry_data': {
                                                'id': '99d53da0-e96a-11e4-b15e-a0000100fe80',
                                                'size_bytes': 6138129367,
                                                'checksum_path': 'package.sha1',
                                                'platform': {'code': 'LANDSAT_8'},
                                                'instrument': {'name': 'OLI_TIRS'},
                                                'product_type': 'satellite_telemetry_data',
                                                'creation_dt': datetime.datetime(2015, 4, 7, 3, 25, 59),
                                                'ga_level': 'P00',
                                                'format': {'name': 'MD'},
                                                'usgs': {
                                                    'interval_id': 'LC80900750902014207LGN00'
                                                },
                                                'ga_label': 'LS8_OLITIRS_STD-MD_P00_LC80900750902014207LGN00_'
                                                            '090_075-090_20140727T005408Z20140727T005712',
                                                'image': {
                                                    'satellite_ref_point_end': {'x': 90, 'y': 90},
                                                    'satellite_ref_point_start': {'x': 90, 'y': 75}},
                                                'acquisition': {
                                                    'groundstation': {'code': 'LGN'},
                                                    'aos': datetime.datetime(2014, 7, 27, 0, 54, 8, 903000),
                                                    'los': datetime.datetime(2014, 7, 27, 0, 57, 12, 457000)},
                                                'lineage': {
                                                    'source_datasets': {},
                                                    'machine': {
                                                        'version': '2.4.0',
                                                        'runtime_id': 'ddce56fe-e960-11e4-b15e-a0000100fe80',
                                                        'hostname': 'r2830',
                                                        'uname': 'Linux r2830 2.6.32-504.12.2.el6.x86_64 '
                                                                 '#1 SMP Wed Mar 11 22:03:14 UTC 2015 x86_64',
                                                        'type_id': 'jobmanager'
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    'product_level': 'L1T',
                                    'image': {
                                        'sun_azimuth': 37.30962098,
                                        'sun_elevation': 31.06756304,
                                        'geometric_rmse_model': 7.087,
                                        'sun_earth_distance': 1.0155974,
                                        'satellite_ref_point_start': {'x': 90, 'y': 81},
                                        'geometric_rmse_model_y': 5.194,
                                        'ground_control_points_model': 401,
                                        'cloud_cover_percentage': 5.16,
                                        'geometric_rmse_model_x': 4.822,
                                        'bands': {
                                            '1': {
                                                'label': 'Coastal Aerosol',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_B1.TIF',
                                                'type': 'reflective',
                                                'number': '1'
                                            },
                                            '6': {
                                                'label': 'Short-wave Infrared 1',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_B6.TIF',
                                                'type': 'reflective',
                                                'number': '6'
                                            },
                                            'quality': {
                                                'label': 'Quality',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_BQA.TIF',
                                                'type': 'quality',
                                                'number': 'quality'
                                            },
                                            '8': {
                                                'label': 'Panchromatic',
                                                'cell_size': 12.5,
                                                'path': 'product/LC80900812014207LGN00_B8.TIF',
                                                'type': 'panchromatic',
                                                'number': '8'
                                            },
                                            '3': {
                                                'label': 'Visible Green',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_B3.TIF',
                                                'type': 'reflective',
                                                'number': '3'
                                            },
                                            '2': {
                                                'label': 'Visible Blue',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_B2.TIF',
                                                'type': 'reflective',
                                                'number': '2'
                                            },
                                            '5': {
                                                'label': 'Near Infrared',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_B5.TIF',
                                                'type': 'reflective',
                                                'number': '5'
                                            },
                                            '4': {
                                                'label': 'Visible Red',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_B4.TIF',
                                                'type': 'reflective',
                                                'number': '4'
                                            },
                                            '7': {
                                                'label': 'Short-wave Infrared 2',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_B7.TIF',
                                                'type': 'reflective',
                                                'number': '7'
                                            },
                                            '10': {
                                                'label': 'Thermal Infrared 1',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_B10.TIF',
                                                'type': 'thermal',
                                                'number': '10'
                                            },
                                            '11': {
                                                'label': 'Thermal Infrared 2',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_B11.TIF',
                                                'type': 'thermal',
                                                'number': '11'
                                            },
                                            '9': {
                                                'label': 'Cirrus',
                                                'cell_size': 25.0,
                                                'path': 'product/LC80900812014207LGN00_B9.TIF',
                                                'type': 'atmosphere',
                                                'number': '9'
                                            }
                                        }
                                    },
                                }
                            },
                            'machine': {}
                        }
                    }
                },
                'machine': {}
            }
        }
    )

    # TODO: Assert correct checksums? They shouldn't change in theory. But they may with gdal versions etc.
    # Check all files are listed in checksum file.
    output_checksum_path = output_dataset.joinpath('package.sha1')
    assert output_checksum_path.exists()
    checksummed_filenames = load_checksum_filenames(output_checksum_path)
    assert checksummed_filenames == [
        'browse.fr.jpg',
        'browse.jpg',
        'ga-metadata.yaml',
        'product/LS8_OLITIRS_PQ_P55_GAPQ01-032_090_081_20140726.tif',
    ]


if __name__ == '__main__':
    import logging

    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    test_package()
