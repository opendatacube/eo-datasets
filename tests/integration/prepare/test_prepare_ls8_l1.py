from pathlib import Path

from .common import check_prepare_outputs

L1_TARBALL_PATH: Path = Path(__file__).parent / 'data' / 'LC08_L1TP_090084_20160121_20170405_01_T1.tar.gz'


def test_prepare_l7_l1_usgs_tarball(tmpdir):
    assert L1_TARBALL_PATH.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)
    expected_metadata_path = output_path / 'LC08_L1TP_090084_20160121_20170405_01_T1.yaml'

    def path_offset(offset: str):
        return 'tar:' + str(L1_TARBALL_PATH.absolute()) + '!' + offset

    expected_doc = {
        'id': '<<Always Different>> This field is ignored below',
        'product_type': 'level1',
        'extent': {
            'center_dt': '2016-01-21 23:50:23.0544350Z',
            'coord': {
                'll': {
                    'lat': -35.71162642627402,
                    'lon': 148.5697851318128
                },
                'lr': {
                    'lat': -35.648930411428125,
                    'lon': 151.1887791534246
                },
                'ul': {
                    'lat': -33.561627680748764,
                    'lon': 148.52977968016268
                },
                'ur': {
                    'lat': -33.503736851013535,
                    'lon': 151.0823438821332}
            },
        },
        'format': {
            'name': 'GeoTIFF'
        },
        'grid_spatial': {
            'projection': {
                'geo_ref_points': {
                    'll': {
                        'x': 642000.0,
                        'y': -3953100.0
                    },
                    'lr': {
                        'x': 879300.0,
                        'y': -3953100.0
                    },
                    'ul': {
                        'x': 642000.0,
                        'y': -3714600.0
                    },
                    'ur': {
                        'x': 879300.0,
                        'y': -3714600.0
                    },
                },
                'spatial_reference': 'EPSG:32655'
            },
        },
        'image': {
            'bands': {
                'blue': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B2.TIF')
                },
                'cirrus': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B9.TIF')
                },
                'coastal_aerosol': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B1.TIF')
                },
                'green': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B3.TIF')
                },
                'lwir1': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B10.TIF')
                },
                'lwir2': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B11.TIF')
                },
                'nir': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B5.TIF')
                },
                'panchromatic': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B8.TIF')
                },
                'quality': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_BQA.TIF')
                },
                'red': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B4.TIF')
                },
                'swir1': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B6.TIF')
                },
                'swir2': {
                    'layer': 1,
                    'path': path_offset('LC08_L1TP_090084_20160121_20170405_01_T1_B7.TIF')}
            },
        },
        'mtl': {
            'image_attributes': {
                'cloud_cover': 93.22,
                'cloud_cover_land': 93.19,
                'earth_sun_distance': 0.984075,
                'geometric_rmse_model': 7.412,
                'geometric_rmse_model_x': 4.593,
                'geometric_rmse_model_y': 5.817,
                'ground_control_points_model': 66,
                'ground_control_points_version': 4,
                'image_quality_oli': 9,
                'image_quality_tirs': 9,
                'roll_angle': -0.001,
                'saturation_band_1': 'N',
                'saturation_band_2': 'N',
                'saturation_band_3': 'N',
                'saturation_band_4': 'N',
                'saturation_band_5': 'N',
                'saturation_band_6': 'N',
                'saturation_band_7': 'Y',
                'saturation_band_8': 'N',
                'saturation_band_9': 'N',
                'sun_azimuth': 74.0074438,
                'sun_elevation': 55.486483,
                'tirs_ssm_model': 'FINAL',
                'tirs_ssm_position_status': 'ESTIMATED',
                'tirs_stray_light_correction_source': 'TIRS',
                'truncation_oli': 'UPPER'
            },
            'metadata_file_info': {
                'collection_number': 1,
                'file_date': '2017-04-05T11:17:36Z',
                'landsat_product_id': 'LC08_L1TP_090084_20160121_20170405_01_T1',
                'landsat_scene_id': 'LC80900842016021LGN02',
                'origin': 'Image courtesy of the U.S. Geological Survey',
                'processing_software_version': 'LPGS_2.7.0',
                'request_id': 50170405598800011,
                'station_id': 'LGN'
            },
            'min_max_pixel_value': {
                'quantize_cal_max_band_1': 65535,
                'quantize_cal_max_band_10': 65535,
                'quantize_cal_max_band_11': 65535,
                'quantize_cal_max_band_2': 65535,
                'quantize_cal_max_band_3': 65535,
                'quantize_cal_max_band_4': 65535,
                'quantize_cal_max_band_5': 65535,
                'quantize_cal_max_band_6': 65535,
                'quantize_cal_max_band_7': 65535,
                'quantize_cal_max_band_8': 65535,
                'quantize_cal_max_band_9': 65535,
                'quantize_cal_min_band_1': 1,
                'quantize_cal_min_band_10': 1,
                'quantize_cal_min_band_11': 1,
                'quantize_cal_min_band_2': 1,
                'quantize_cal_min_band_3': 1,
                'quantize_cal_min_band_4': 1,
                'quantize_cal_min_band_5': 1,
                'quantize_cal_min_band_6': 1,
                'quantize_cal_min_band_7': 1,
                'quantize_cal_min_band_8': 1,
                'quantize_cal_min_band_9': 1
            },
            'min_max_radiance': {
                'radiance_maximum_band_1': 784.86145,
                'radiance_maximum_band_10': 22.0018,
                'radiance_maximum_band_11': 22.0018,
                'radiance_maximum_band_2': 803.70764,
                'radiance_maximum_band_3': 740.60974,
                'radiance_maximum_band_4': 624.52386,
                'radiance_maximum_band_5': 382.17746,
                'radiance_maximum_band_6': 95.04406,
                'radiance_maximum_band_7': 32.03493,
                'radiance_maximum_band_8': 706.78912,
                'radiance_maximum_band_9': 149.36362,
                'radiance_minimum_band_1': -64.81411,
                'radiance_minimum_band_10': 0.10033,
                'radiance_minimum_band_11': 0.10033,
                'radiance_minimum_band_2': -66.37044,
                'radiance_minimum_band_3': -61.15979,
                'radiance_minimum_band_4': -51.57338,
                'radiance_minimum_band_5': -31.56034,
                'radiance_minimum_band_6': -7.84877,
                'radiance_minimum_band_7': -2.64546,
                'radiance_minimum_band_8': -58.36687,
                'radiance_minimum_band_9': -12.3345
            },
            'min_max_reflectance': {
                'reflectance_maximum_band_1': 1.2107,
                'reflectance_maximum_band_2': 1.2107,
                'reflectance_maximum_band_3': 1.2107,
                'reflectance_maximum_band_4': 1.2107,
                'reflectance_maximum_band_5': 1.2107,
                'reflectance_maximum_band_6': 1.2107,
                'reflectance_maximum_band_7': 1.2107,
                'reflectance_maximum_band_8': 1.2107,
                'reflectance_maximum_band_9': 1.2107,
                'reflectance_minimum_band_1': -0.09998,
                'reflectance_minimum_band_2': -0.09998,
                'reflectance_minimum_band_3': -0.09998,
                'reflectance_minimum_band_4': -0.09998,
                'reflectance_minimum_band_5': -0.09998,
                'reflectance_minimum_band_6': -0.09998,
                'reflectance_minimum_band_7': -0.09998,
                'reflectance_minimum_band_8': -0.09998,
                'reflectance_minimum_band_9': -0.09998
            },
            'product_metadata': {
                'angle_coefficient_file_name': 'LC08_L1TP_090084_20160121_20170405_01_T1_ANG.txt',
                'bpf_name_oli': 'LO8BPF20160121232151_20160122000630.01',
                'bpf_name_tirs': 'LT8BPF20160110081635_20160124145303.01',
                'collection_category': 'T1',
                'corner_ll_lat_product': -35.71163,
                'corner_ll_lon_product': 148.56979,
                'corner_ll_projection_x_product': 642000.0,
                'corner_ll_projection_y_product': -3953100.0,
                'corner_lr_lat_product': -35.64893,
                'corner_lr_lon_product': 151.18878,
                'corner_lr_projection_x_product': 879300.0,
                'corner_lr_projection_y_product': -3953100.0,
                'corner_ul_lat_product': -33.56163,
                'corner_ul_lon_product': 148.52978,
                'corner_ul_projection_x_product': 642000.0,
                'corner_ul_projection_y_product': -3714600.0,
                'corner_ur_lat_product': -33.50374,
                'corner_ur_lon_product': 151.08234,
                'corner_ur_projection_x_product': 879300.0,
                'corner_ur_projection_y_product': -3714600.0,
                'cpf_name': 'LC08CPF_20160101_20160331_01.01',
                'data_type': 'L1TP',
                'date_acquired': '2016-01-21',
                'elevation_source': 'GLS2000',
                'file_name_band_1': 'LC08_L1TP_090084_20160121_20170405_01_T1_B1.TIF',
                'file_name_band_10': 'LC08_L1TP_090084_20160121_20170405_01_T1_B10.TIF',
                'file_name_band_11': 'LC08_L1TP_090084_20160121_20170405_01_T1_B11.TIF',
                'file_name_band_2': 'LC08_L1TP_090084_20160121_20170405_01_T1_B2.TIF',
                'file_name_band_3': 'LC08_L1TP_090084_20160121_20170405_01_T1_B3.TIF',
                'file_name_band_4': 'LC08_L1TP_090084_20160121_20170405_01_T1_B4.TIF',
                'file_name_band_5': 'LC08_L1TP_090084_20160121_20170405_01_T1_B5.TIF',
                'file_name_band_6': 'LC08_L1TP_090084_20160121_20170405_01_T1_B6.TIF',
                'file_name_band_7': 'LC08_L1TP_090084_20160121_20170405_01_T1_B7.TIF',
                'file_name_band_8': 'LC08_L1TP_090084_20160121_20170405_01_T1_B8.TIF',
                'file_name_band_9': 'LC08_L1TP_090084_20160121_20170405_01_T1_B9.TIF',
                'file_name_band_quality': 'LC08_L1TP_090084_20160121_20170405_01_T1_BQA.TIF',
                'metadata_file_name': 'LC08_L1TP_090084_20160121_20170405_01_T1_MTL.txt',
                'nadir_offnadir': 'NADIR',
                'output_format': 'GEOTIFF',
                'panchromatic_lines': 15901,
                'panchromatic_samples': 15821,
                'reflective_lines': 7951,
                'reflective_samples': 7911,
                'rlut_file_name': 'LC08RLUT_20150303_20431231_01_12.h5',
                'scene_center_time': '23:50:23.0544350Z',
                'sensor_id': 'OLI_TIRS',
                'spacecraft_id': 'LANDSAT_8',
                'target_wrs_path': 90,
                'target_wrs_row': 84,
                'thermal_lines': 7951,
                'thermal_samples': 7911,
                'wrs_path': 90,
                'wrs_row': 84
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
                'utm_zone': 55
            },
            'radiometric_rescaling': {
                'radiance_add_band_1': -64.82708,
                'radiance_add_band_10': 0.1,
                'radiance_add_band_11': 0.1,
                'radiance_add_band_2': -66.38372,
                'radiance_add_band_3': -61.17203,
                'radiance_add_band_4': -51.5837,
                'radiance_add_band_5': -31.56665,
                'radiance_add_band_6': -7.85034,
                'radiance_add_band_7': -2.64598,
                'radiance_add_band_8': -58.37855,
                'radiance_add_band_9': -12.33696,
                'radiance_mult_band_1': 0.012965,
                'radiance_mult_band_10': 0.0003342,
                'radiance_mult_band_11': 0.0003342,
                'radiance_mult_band_2': 0.013277,
                'radiance_mult_band_3': 0.012234,
                'radiance_mult_band_4': 0.010317,
                'radiance_mult_band_5': 0.0063133,
                'radiance_mult_band_6': 0.0015701,
                'radiance_mult_band_7': 0.0005292,
                'radiance_mult_band_8': 0.011676,
                'radiance_mult_band_9': 0.0024674,
                'reflectance_add_band_1': -0.1,
                'reflectance_add_band_2': -0.1,
                'reflectance_add_band_3': -0.1,
                'reflectance_add_band_4': -0.1,
                'reflectance_add_band_5': -0.1,
                'reflectance_add_band_6': -0.1,
                'reflectance_add_band_7': -0.1,
                'reflectance_add_band_8': -0.1,
                'reflectance_add_band_9': -0.1,
                'reflectance_mult_band_1': 2e-05,
                'reflectance_mult_band_2': 2e-05,
                'reflectance_mult_band_3': 2e-05,
                'reflectance_mult_band_4': 2e-05,
                'reflectance_mult_band_5': 2e-05,
                'reflectance_mult_band_6': 2e-05,
                'reflectance_mult_band_7': 2e-05,
                'reflectance_mult_band_8': 2e-05,
                'reflectance_mult_band_9': 2e-05
            },
            'tirs_thermal_constants': {
                'k1_constant_band_10': 774.8853,
                'k1_constant_band_11': 480.8883,
                'k2_constant_band_10': 1321.0789,
                'k2_constant_band_11': 1201.1442
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
