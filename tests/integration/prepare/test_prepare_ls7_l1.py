from datetime import datetime
from pathlib import Path

from .common import run_prepare_cli, check_prepare_outputs

L71GT_TARBALL_PATH: Path = Path(__file__).parent / 'data' / 'LE07_L1GT_104078_20131209_20161119_01_T2.tar.gz'


def test_prepare_l7_l1_usgs_tarball(tmpdir):
    assert L71GT_TARBALL_PATH.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)
    expected_metadata_path = output_path / 'LE07_L1GT_104078_20131209_20161119_01_T2.yaml'

    def path_offset(offset: str):
        return 'tar:' + str(L71GT_TARBALL_PATH.absolute()) + '!' + offset

    expected_doc = {
        'id': '<<Always Different>> This field is ignored below',
        'product_type': 'level1',
        'format': {'name': 'GeoTIFF'},
        'extent': {
            'center_dt': '2013-12-09 01:10:46.6908469Z',
            'coord': {
                'll': {
                    'lat': -26.954354903739272,
                    'lon': 129.22769548746237
                },
                'lr': {
                    'lat': -26.928774471741676,
                    'lon': 131.69588786021484
                },
                'ul': {
                    'lat': -25.03371801797915,
                    'lon': 129.22402339672468
                },
                'ur': {
                    'lat': -25.01021635833341,
                    'lon': 131.65247288941694
                }
            },
        },
        'grid_spatial': {
            'projection': {
                'geo_ref_points': {
                    'll': {
                        'x': 522600.0,
                        'y': -2981400.0
                    },
                    'lr': {
                        'x': 767700.0,
                        'y': -2981400.0
                    },
                    'ul': {
                        'x': 522600.0,
                        'y': -2768700.0
                    },
                    'ur': {
                        'x': 767700.0,
                        'y': -2768700.0
                    }
                },
                'spatial_reference': 'EPSG:32652'
            }
        },
        'image': {
            'bands': {
                'blue': {
                    'layer': 1,
                    'path': path_offset('LE07_L1GT_104078_20131209_20161119_01_T2_B1.TIF')
                },
                'green': {
                    'layer': 1,
                    'path': path_offset('LE07_L1GT_104078_20131209_20161119_01_T2_B2.TIF')
                },
                'nir': {
                    'layer': 1,
                    'path': path_offset('LE07_L1GT_104078_20131209_20161119_01_T2_B4.TIF')
                },
                'quality': {
                    'layer': 1,
                    'path': path_offset('LE07_L1GT_104078_20131209_20161119_01_T2_BQA.TIF')
                },
                'red': {
                    'layer': 1,
                    'path': path_offset('LE07_L1GT_104078_20131209_20161119_01_T2_B3.TIF')
                },
                'swir1': {
                    'layer': 1,
                    'path': path_offset('LE07_L1GT_104078_20131209_20161119_01_T2_B5.TIF')
                },
                'swir2': {
                    'layer': 1,
                    'path': path_offset('LE07_L1GT_104078_20131209_20161119_01_T2_B7.TIF')
                }
            }
        },
        'mtl': {
            'image_attributes': {
                'cloud_cover': 85.0,
                'cloud_cover_land': 85.0,
                'earth_sun_distance': 0.9849428,
                'image_quality': 9,
                'saturation_band_1': 'Y',
                'saturation_band_2': 'Y',
                'saturation_band_3': 'Y',
                'saturation_band_4': 'Y',
                'saturation_band_5': 'Y',
                'saturation_band_6_vcid_1': 'N',
                'saturation_band_6_vcid_2': 'N',
                'saturation_band_7': 'Y',
                'saturation_band_8': 'N',
                'sun_azimuth': 89.78702069,
                'sun_elevation': 62.640177
            },
            'metadata_file_info': {
                'collection_number': 1,
                'data_category': 'NOMINAL',
                'file_date': '2016-11-19T00:48:19Z',
                'landsat_product_id': 'LE07_L1GT_104078_20131209_20161119_01_T2',
                'landsat_scene_id': 'LE71040782013343ASA00',
                'origin': 'Image courtesy of the U.S. Geological Survey',
                'processing_software_version': 'LPGS_12.8.2',
                'request_id': 50161117102904467,
                'station_id': 'ASA'
            },
            'min_max_pixel_value': {
                'quantize_cal_max_band_1': 255,
                'quantize_cal_max_band_2': 255,
                'quantize_cal_max_band_3': 255,
                'quantize_cal_max_band_4': 255,
                'quantize_cal_max_band_5': 255,
                'quantize_cal_max_band_6_vcid_1': 255,
                'quantize_cal_max_band_6_vcid_2': 255,
                'quantize_cal_max_band_7': 255,
                'quantize_cal_max_band_8': 255,
                'quantize_cal_min_band_1': 1,
                'quantize_cal_min_band_2': 1,
                'quantize_cal_min_band_3': 1,
                'quantize_cal_min_band_4': 1,
                'quantize_cal_min_band_5': 1,
                'quantize_cal_min_band_6_vcid_1': 1,
                'quantize_cal_min_band_6_vcid_2': 1,
                'quantize_cal_min_band_7': 1,
                'quantize_cal_min_band_8': 1
            },
            'min_max_radiance': {
                'radiance_maximum_band_1': 191.6,
                'radiance_maximum_band_2': 196.5,
                'radiance_maximum_band_3': 152.9,
                'radiance_maximum_band_4': 241.1,
                'radiance_maximum_band_5': 31.06,
                'radiance_maximum_band_6_vcid_1': 17.04,
                'radiance_maximum_band_6_vcid_2': 12.65,
                'radiance_maximum_band_7': 10.8,
                'radiance_maximum_band_8': 243.1,
                'radiance_minimum_band_1': -6.2,
                'radiance_minimum_band_2': -6.4,
                'radiance_minimum_band_3': -5.0,
                'radiance_minimum_band_4': -5.1,
                'radiance_minimum_band_5': -1.0,
                'radiance_minimum_band_6_vcid_1': 0.0,
                'radiance_minimum_band_6_vcid_2': 3.2,
                'radiance_minimum_band_7': -0.35,
                'radiance_minimum_band_8': -4.7
            },
            'min_max_reflectance': {
                'reflectance_maximum_band_1': 0.286807,
                'reflectance_maximum_band_2': 0.322668,
                'reflectance_maximum_band_3': 0.305569,
                'reflectance_maximum_band_4': 0.686088,
                'reflectance_maximum_band_5': 0.427173,
                'reflectance_maximum_band_7': 0.404562,
                'reflectance_maximum_band_8': 0.56171,
                'reflectance_minimum_band_1': -0.009281,
                'reflectance_minimum_band_2': -0.010509,
                'reflectance_minimum_band_3': -0.009992,
                'reflectance_minimum_band_4': -0.014513,
                'reflectance_minimum_band_5': -0.013753,
                'reflectance_minimum_band_7': -0.013111,
                'reflectance_minimum_band_8': -0.01086
            },
            'product_metadata': {
                'angle_coefficient_file_name': 'LE07_L1GT_104078_20131209_20161119_01_T2_ANG.txt',
                'collection_category': 'T2',
                'corner_ll_lat_product': -26.95435,
                'corner_ll_lon_product': 129.2277,
                'corner_ll_projection_x_product': 522600.0,
                'corner_ll_projection_y_product': -2981400.0,
                'corner_lr_lat_product': -26.92877,
                'corner_lr_lon_product': 131.69589,
                'corner_lr_projection_x_product': 767700.0,
                'corner_lr_projection_y_product': -2981400.0,
                'corner_ul_lat_product': -25.03372,
                'corner_ul_lon_product': 129.22402,
                'corner_ul_projection_x_product': 522600.0,
                'corner_ul_projection_y_product': -2768700.0,
                'corner_ur_lat_product': -25.01022,
                'corner_ur_lon_product': 131.65247,
                'corner_ur_projection_x_product': 767700.0,
                'corner_ur_projection_y_product': -2768700.0,
                'cpf_name': 'LE07CPF_20131001_20131231_01.02',
                'data_type': 'L1GT',
                'date_acquired': '2013-12-09',
                'elevation_source': 'GLS2000',
                'ephemeris_type': 'DEFINITIVE',
                'file_name_band_1': 'LE07_L1GT_104078_20131209_20161119_01_T2_B1.TIF',
                'file_name_band_2': 'LE07_L1GT_104078_20131209_20161119_01_T2_B2.TIF',
                'file_name_band_3': 'LE07_L1GT_104078_20131209_20161119_01_T2_B3.TIF',
                'file_name_band_4': 'LE07_L1GT_104078_20131209_20161119_01_T2_B4.TIF',
                'file_name_band_5': 'LE07_L1GT_104078_20131209_20161119_01_T2_B5.TIF',
                'file_name_band_6_vcid_1': 'LE07_L1GT_104078_20131209_20161119_01_T2_B6_VCID_1.TIF',
                'file_name_band_6_vcid_2': 'LE07_L1GT_104078_20131209_20161119_01_T2_B6_VCID_2.TIF',
                'file_name_band_7': 'LE07_L1GT_104078_20131209_20161119_01_T2_B7.TIF',
                'file_name_band_8': 'LE07_L1GT_104078_20131209_20161119_01_T2_B8.TIF',
                'file_name_band_quality': 'LE07_L1GT_104078_20131209_20161119_01_T2_BQA.TIF',
                'metadata_file_name': 'LE07_L1GT_104078_20131209_20161119_01_T2_MTL.txt',
                'output_format': 'GEOTIFF',
                'panchromatic_lines': 14181,
                'panchromatic_samples': 16341,
                'reflective_lines': 7091,
                'reflective_samples': 8171,
                'scene_center_time': '01:10:46.6908469Z',
                'sensor_id': 'ETM',
                'sensor_mode': 'BUMPER',
                'spacecraft_id': 'LANDSAT_7',
                'thermal_lines': 7091,
                'thermal_samples': 8171,
                'wrs_path': 104,
                'wrs_row': 78
            },
            'product_parameters': {
                'correction_bias_band_1': 'INTERNAL_CALIBRATION',
                'correction_bias_band_2': 'INTERNAL_CALIBRATION',
                'correction_bias_band_3': 'INTERNAL_CALIBRATION',
                'correction_bias_band_4': 'INTERNAL_CALIBRATION',
                'correction_bias_band_5': 'INTERNAL_CALIBRATION',
                'correction_bias_band_6_vcid_1': 'INTERNAL_CALIBRATION',
                'correction_bias_band_6_vcid_2': 'INTERNAL_CALIBRATION',
                'correction_bias_band_7': 'INTERNAL_CALIBRATION',
                'correction_bias_band_8': 'INTERNAL_CALIBRATION',
                'correction_gain_band_1': 'CPF',
                'correction_gain_band_2': 'CPF',
                'correction_gain_band_3': 'CPF',
                'correction_gain_band_4': 'CPF',
                'correction_gain_band_5': 'CPF',
                'correction_gain_band_6_vcid_1': 'CPF',
                'correction_gain_band_6_vcid_2': 'CPF',
                'correction_gain_band_7': 'CPF',
                'correction_gain_band_8': 'CPF',
                'gain_band_1': 'H',
                'gain_band_2': 'H',
                'gain_band_3': 'H',
                'gain_band_4': 'L',
                'gain_band_5': 'H',
                'gain_band_6_vcid_1': 'L',
                'gain_band_6_vcid_2': 'H',
                'gain_band_7': 'H',
                'gain_band_8': 'L',
                'gain_change_band_1': 'HH',
                'gain_change_band_2': 'HH',
                'gain_change_band_3': 'HH',
                'gain_change_band_4': 'LL',
                'gain_change_band_5': 'HH',
                'gain_change_band_6_vcid_1': 'LL',
                'gain_change_band_6_vcid_2': 'HH',
                'gain_change_band_7': 'HH',
                'gain_change_band_8': 'LL',
                'gain_change_scan_band_1': 0,
                'gain_change_scan_band_2': 0,
                'gain_change_scan_band_3': 0,
                'gain_change_scan_band_4': 0,
                'gain_change_scan_band_5': 0,
                'gain_change_scan_band_6_vcid_1': 0,
                'gain_change_scan_band_6_vcid_2': 0,
                'gain_change_scan_band_7': 0,
                'gain_change_scan_band_8': 0
            },
            'projection_parameters': {
                'datum': 'WGS84',
                'ellipsoid': 'WGS84',
                'grid_cell_size_panchromatic': 15.0,
                'grid_cell_size_reflective': 30.0,
                'grid_cell_size_thermal': 30.0,
                'map_projection': 'UTM',
                'orientation': 'NORTH_UP',
                'resampling_option': 'CUBIC_CONVOLUTION',
                'scan_gap_interpolation': 2.0,
                'utm_zone': 52
            },
            'radiometric_rescaling': {
                'radiance_add_band_1': -6.97874,
                'radiance_add_band_2': -7.19882,
                'radiance_add_band_3': -5.62165,
                'radiance_add_band_4': -6.06929,
                'radiance_add_band_5': -1.12622,
                'radiance_add_band_6_vcid_1': -0.06709,
                'radiance_add_band_6_vcid_2': 3.1628,
                'radiance_add_band_7': -0.3939,
                'radiance_add_band_8': -5.67559,
                'radiance_mult_band_1': 0.77874,
                'radiance_mult_band_2': 0.79882,
                'radiance_mult_band_3': 0.62165,
                'radiance_mult_band_4': 0.96929,
                'radiance_mult_band_5': 0.12622,
                'radiance_mult_band_6_vcid_1': 0.067087,
                'radiance_mult_band_6_vcid_2': 0.037205,
                'radiance_mult_band_7': 0.043898,
                'radiance_mult_band_8': 0.97559,
                'reflectance_add_band_1': -0.010447,
                'reflectance_add_band_2': -0.011821,
                'reflectance_add_band_3': -0.011235,
                'reflectance_add_band_4': -0.017271,
                'reflectance_add_band_5': -0.015489,
                'reflectance_add_band_7': -0.014755,
                'reflectance_add_band_8': -0.013114,
                'reflectance_mult_band_1': 0.0011657,
                'reflectance_mult_band_2': 0.0013117,
                'reflectance_mult_band_3': 0.0012424,
                'reflectance_mult_band_4': 0.0027583,
                'reflectance_mult_band_5': 0.0017359,
                'reflectance_mult_band_7': 0.0016444,
                'reflectance_mult_band_8': 0.0022542},
            'thermal_constants': {
                'k1_constant_band_6_vcid_1': 666.09,
                'k1_constant_band_6_vcid_2': 666.09,
                'k2_constant_band_6_vcid_1': 1282.71,
                'k2_constant_band_6_vcid_2': 1282.71
            }
        },
        'lineage': {
            'source_datasets': {}
        }
    }

    check_prepare_outputs(
        input_dataset=L71GT_TARBALL_PATH,
        expected_doc=expected_doc,
        output_path=output_path,
        expected_metadata_path=expected_metadata_path
    )


def test_skips_old_datasets(tmpdir):
    """Prepare should skip datasets older than the given date"""

    output_path = Path(tmpdir)
    expected_metadata_path = output_path / 'LE07_L1GT_104078_20131209_20161119_01_T2.yaml'

    run_prepare_cli(
        '--output', str(output_path),
        # Can't be newer than right now.
        '--newer-than', datetime.now().isoformat(),
        str(L71GT_TARBALL_PATH),
    )
    assert not expected_metadata_path.exists(), "Dataset should have been skipped due to age"

    # It should work with an old date.
    run_prepare_cli(
        '--output', str(output_path),
        # Some old date, from before the test data was created.
        '--newer-than', "2014-05-04",
        str(L71GT_TARBALL_PATH),
    )
    assert expected_metadata_path.exists(), "Dataset should have been packaged when using an ancient date cutoff"
