from pathlib import Path

from .common import check_prepare_outputs

L1_TARBALL_PATH: Path = Path(__file__).parent / 'data' / 'LT05_L1TP_090085_19970406_20161231_01_T1.tar.gz'


def test_prepare_l7_l1_usgs_tarball(tmpdir):
    assert L1_TARBALL_PATH.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)
    expected_metadata_path = output_path / 'LT05_L1TP_090085_19970406_20161231_01_T1.yaml'

    def path_offset(offset: str):
        return 'tar:' + str(L1_TARBALL_PATH.absolute()) + '!' + offset

    expected_doc = {
        'id': '<<Always Different>> This field is ignored below',
        'product_type': 'level1',
        'extent': {
            'center_dt': '1997-04-06 23:17:43.1020000Z',
            'coord': {
                'll': {
                    'lat': -37.0505865085101,
                    'lon': 148.05040048068
                },
                'lr': {
                    'lat': -36.99521175837006,
                    'lon': 150.77305358506374
                },
                'ul': {
                    'lat': -35.07373601408718,
                    'lon': 148.02444424678097
                },
                'ur': {
                    'lat': -35.02220984451927,
                    'lon': 150.68005778871884}
            },
        },
        'format': {
            'name': 'GeoTIFF'
        },
        'grid_spatial': {
            'projection': {
                'geo_ref_points': {
                    'll': {
                        'x': 593400.0,
                        'y': -4101000.0
                    },
                    'lr': {
                        'x': 835800.0,
                        'y': -4101000.0
                    },
                    'ul': {
                        'x': 593400.0,
                        'y': -3881700.0
                    },
                    'ur': {
                        'x': 835800.0,
                        'y': -3881700.0
                    },
                },
                'spatial_reference': 'EPSG:32655'
            },
        },
        'image': {
            'bands': {
                'blue': {
                    'layer': 1,
                    'path': path_offset('LT05_L1TP_090085_19970406_20161231_01_T1_B1.TIF')
                },
                'green': {
                    'layer': 1,
                    'path': path_offset('LT05_L1TP_090085_19970406_20161231_01_T1_B2.TIF')
                },
                'nir': {
                    'layer': 1,
                    'path': path_offset('LT05_L1TP_090085_19970406_20161231_01_T1_B4.TIF')
                },
                'quality': {
                    'layer': 1,
                    'path': path_offset('LT05_L1TP_090085_19970406_20161231_01_T1_BQA.TIF')
                },
                'red': {
                    'layer': 1,
                    'path': path_offset('LT05_L1TP_090085_19970406_20161231_01_T1_B3.TIF')
                },
                'swir1': {
                    'layer': 1,
                    'path': path_offset('LT05_L1TP_090085_19970406_20161231_01_T1_B5.TIF')
                },
                'swir2': {
                    'layer': 1,
                    'path': path_offset('LT05_L1TP_090085_19970406_20161231_01_T1_B7.TIF')}
            },
        },
        'mtl': {
            'image_attributes': {
                'cloud_cover': 27.0,
                'cloud_cover_land': 29.0,
                'earth_sun_distance': 1.0009715,
                'geometric_rmse_model': 4.286,
                'geometric_rmse_model_x': 3.036,
                'geometric_rmse_model_y': 3.025,
                'geometric_rmse_verify': 0.163,
                'geometric_rmse_verify_quad_ll': 0.179,
                'geometric_rmse_verify_quad_lr': 0.17,
                'geometric_rmse_verify_quad_ul': 0.164,
                'geometric_rmse_verify_quad_ur': 0.151,
                'ground_control_points_model': 161,
                'ground_control_points_verify': 1679,
                'ground_control_points_version': 4,
                'image_quality': 9,
                'saturation_band_1': 'Y',
                'saturation_band_2': 'Y',
                'saturation_band_3': 'Y',
                'saturation_band_4': 'Y',
                'saturation_band_5': 'Y',
                'saturation_band_6': 'N',
                'saturation_band_7': 'Y',
                'sun_azimuth': 51.25454223,
                'sun_elevation': 31.98763219
            },
            'metadata_file_info': {
                'collection_number': 1,
                'data_category': 'NOMINAL',
                'file_date': '2016-12-31T15:54:58Z',
                'landsat_product_id': 'LT05_L1TP_090085_19970406_20161231_01_T1',
                'landsat_scene_id': 'LT50900851997096ASA00',
                'origin': 'Image courtesy of the U.S. Geological Survey',
                'processing_software_version': 'LPGS_12.8.2',
                'request_id': 50161230705418380,
                'station_id': 'ASA'
            },
            'min_max_pixel_value': {
                'quantize_cal_max_band_1': 255,
                'quantize_cal_max_band_2': 255,
                'quantize_cal_max_band_3': 255,
                'quantize_cal_max_band_4': 255,
                'quantize_cal_max_band_5': 255,
                'quantize_cal_max_band_6': 255,
                'quantize_cal_max_band_7': 255,
                'quantize_cal_min_band_1': 1,
                'quantize_cal_min_band_2': 1,
                'quantize_cal_min_band_3': 1,
                'quantize_cal_min_band_4': 1,
                'quantize_cal_min_band_5': 1,
                'quantize_cal_min_band_6': 1,
                'quantize_cal_min_band_7': 1
            },
            'min_max_radiance': {
                'radiance_maximum_band_1': 193.0,
                'radiance_maximum_band_2': 365.0,
                'radiance_maximum_band_3': 264.0,
                'radiance_maximum_band_4': 221.0,
                'radiance_maximum_band_5': 30.2,
                'radiance_maximum_band_6': 15.303,
                'radiance_maximum_band_7': 16.5,
                'radiance_minimum_band_1': -1.52,
                'radiance_minimum_band_2': -2.84,
                'radiance_minimum_band_3': -1.17,
                'radiance_minimum_band_4': -1.51,
                'radiance_minimum_band_5': -0.37,
                'radiance_minimum_band_6': 1.238,
                'radiance_minimum_band_7': -0.15
            },
            'min_max_reflectance': {
                'reflectance_maximum_band_1': 0.312503,
                'reflectance_maximum_band_2': 0.653161,
                'reflectance_maximum_band_3': 0.557713,
                'reflectance_maximum_band_4': 0.673419,
                'reflectance_maximum_band_5': 0.453533,
                'reflectance_maximum_band_7': 0.63153,
                'reflectance_minimum_band_1': -0.002461,
                'reflectance_minimum_band_2': -0.005082,
                'reflectance_minimum_band_3': -0.002472,
                'reflectance_minimum_band_4': -0.004601,
                'reflectance_minimum_band_5': -0.005557,
                'reflectance_minimum_band_7': -0.005741
            },
            'product_metadata': {
                'angle_coefficient_file_name': 'LT05_L1TP_090085_19970406_20161231_01_T1_ANG.txt',
                'browse_verify_file_name': 'LT05_L1TP_090085_19970406_20161231_01_T1_VER.jpg',
                'collection_category': 'T1',
                'corner_ll_lat_product': -37.05059,
                'corner_ll_lon_product': 148.0504,
                'corner_ll_projection_x_product': 593400.0,
                'corner_ll_projection_y_product': -4101000.0,
                'corner_lr_lat_product': -36.99521,
                'corner_lr_lon_product': 150.77305,
                'corner_lr_projection_x_product': 835800.0,
                'corner_lr_projection_y_product': -4101000.0,
                'corner_ul_lat_product': -35.07374,
                'corner_ul_lon_product': 148.02444,
                'corner_ul_projection_x_product': 593400.0,
                'corner_ul_projection_y_product': -3881700.0,
                'corner_ur_lat_product': -35.02221,
                'corner_ur_lon_product': 150.68006,
                'corner_ur_projection_x_product': 835800.0,
                'corner_ur_projection_y_product': -3881700.0,
                'cpf_name': 'LT05CPF_19970401_19970630_01.03',
                'data_type': 'L1TP',
                'data_type_l0rp': 'TMR_L0RP',
                'date_acquired': '1997-04-06',
                'elevation_source': 'GLS2000',
                'ephemeris_type': 'PREDICTIVE',
                'file_name_band_1': 'LT05_L1TP_090085_19970406_20161231_01_T1_B1.TIF',
                'file_name_band_2': 'LT05_L1TP_090085_19970406_20161231_01_T1_B2.TIF',
                'file_name_band_3': 'LT05_L1TP_090085_19970406_20161231_01_T1_B3.TIF',
                'file_name_band_4': 'LT05_L1TP_090085_19970406_20161231_01_T1_B4.TIF',
                'file_name_band_5': 'LT05_L1TP_090085_19970406_20161231_01_T1_B5.TIF',
                'file_name_band_6': 'LT05_L1TP_090085_19970406_20161231_01_T1_B6.TIF',
                'file_name_band_7': 'LT05_L1TP_090085_19970406_20161231_01_T1_B7.TIF',
                'file_name_band_quality': 'LT05_L1TP_090085_19970406_20161231_01_T1_BQA.TIF',
                'ground_control_point_file_name': 'LT05_L1TP_090085_19970406_20161231_01_T1_GCP.txt',
                'metadata_file_name': 'LT05_L1TP_090085_19970406_20161231_01_T1_MTL.txt',
                'output_format': 'GEOTIFF',
                'reflective_lines': 7311,
                'reflective_samples': 8081,
                'report_verify_file_name': 'LT05_L1TP_090085_19970406_20161231_01_T1_VER.txt',
                'scene_center_time': '23:17:43.1020000Z',
                'sensor_id': 'TM',
                'sensor_mode': 'SAM',
                'spacecraft_id': 'LANDSAT_5',
                'thermal_lines': 7311,
                'thermal_samples': 8081,
                'wrs_path': 90,
                'wrs_row': 85
            },
            'product_parameters': {
                'correction_bias_band_1': 'CPF',
                'correction_bias_band_2': 'CPF',
                'correction_bias_band_3': 'CPF',
                'correction_bias_band_4': 'CPF',
                'correction_bias_band_5': 'CPF',
                'correction_bias_band_6': 'CPF',
                'correction_bias_band_7': 'CPF',
                'correction_gain_band_1': 'CPF',
                'correction_gain_band_2': 'CPF',
                'correction_gain_band_3': 'CPF',
                'correction_gain_band_4': 'CPF',
                'correction_gain_band_5': 'CPF',
                'correction_gain_band_6': 'INTERNAL_CALIBRATION',
                'correction_gain_band_7': 'CPF'
            },
            'projection_parameters': {
                'datum': 'WGS84',
                'ellipsoid': 'WGS84',
                'grid_cell_size_reflective': 30.0,
                'grid_cell_size_thermal': 30.0,
                'map_projection': 'UTM',
                'map_projection_l0ra': 'NA',
                'orientation': 'NORTH_UP',
                'resampling_option': 'CUBIC_CONVOLUTION',
                'utm_zone': 55
            },
            'radiometric_rescaling': {
                'radiance_add_band_1': -2.28583,
                'radiance_add_band_2': -4.28819,
                'radiance_add_band_3': -2.21398,
                'radiance_add_band_4': -2.38602,
                'radiance_add_band_5': -0.49035,
                'radiance_add_band_6': 1.18243,
                'radiance_add_band_7': -0.21555,
                'radiance_mult_band_1': 0.76583,
                'radiance_mult_band_2': 1.4482,
                'radiance_mult_band_3': 1.044,
                'radiance_mult_band_4': 0.87602,
                'radiance_mult_band_5': 0.12035,
                'radiance_mult_band_6': 0.055375,
                'radiance_mult_band_7': 0.065551,
                'reflectance_add_band_1': -0.003701,
                'reflectance_add_band_2': -0.007674,
                'reflectance_add_band_3': -0.004677,
                'reflectance_add_band_4': -0.007271,
                'reflectance_add_band_5': -0.007364,
                'reflectance_add_band_7': -0.00825,
                'reflectance_mult_band_1': 0.00124,
                'reflectance_mult_band_2': 0.0025915,
                'reflectance_mult_band_3': 0.0022055,
                'reflectance_mult_band_4': 0.0026694,
                'reflectance_mult_band_5': 0.0018074,
                'reflectance_mult_band_7': 0.0025089
            },
            'thermal_constants': {
                'k1_constant_band_6': 607.76,
                'k2_constant_band_6': 1260.56
            },
        },
        'lineage': {
            'source_datasets': {
            }
        }
    }

    check_prepare_outputs(
        input_dataset=L1_TARBALL_PATH,
        expected_doc=expected_doc,
        output_path=output_path,
        expected_metadata_path=expected_metadata_path
    )
