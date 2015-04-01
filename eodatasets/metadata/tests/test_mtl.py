import unittest

from pathlib import PosixPath

from eodatasets.metadata import mtl
from eodatasets.type import *


_PARSED_MTL = {
    'IMAGE_ATTRIBUTES': {
        'earth_sun_distance': 0.998137,
        'geometric_rmse_model': 4.61,
        'cloud_cover': 0.01,
        'image_quality_tirs': 9,
        'sun_azimuth': 59.57807899,
        'sun_elevation': 57.89670734,
        'roll_angle': -0.001,
        'ground_control_points_model': 420,
        'geometric_rmse_model_x': 2.968,
        'geometric_rmse_model_y': 3.527,
        'image_quality_oli': 9
    },
    'TIRS_THERMAL_CONSTANTS': {
        'k2_constant_band_10': 1321.08,
        'k2_constant_band_11': 1201.14,
        'k1_constant_band_10': 774.89,
        'k1_constant_band_11': 480.89
    },
    'RADIOMETRIC_RESCALING': {
        'radiance_add_band_11': 0.1,
        'radiance_add_band_10': 0.1,
        'reflectance_mult_band_9': 2e-05,
        'reflectance_mult_band_8': 2e-05,
        'reflectance_mult_band_1': 2e-05,
        'reflectance_mult_band_3': 2e-05,
        'reflectance_mult_band_2': 2e-05,
        'reflectance_mult_band_5': 2e-05,
        'reflectance_mult_band_4': 2e-05,
        'reflectance_mult_band_7': 2e-05,
        'reflectance_mult_band_6': 2e-05,
        'radiance_add_band_9': -11.9918,
        'radiance_add_band_8': -56.74524,
        'radiance_add_band_1': -63.01334,
        'radiance_add_band_3': -59.46056,
        'radiance_add_band_2': -64.52643,
        'radiance_add_band_5': -30.68348,
        'radiance_add_band_4': -50.14049,
        'radiance_add_band_7': -2.57196,
        'radiance_add_band_6': -7.6307,
        'radiance_mult_band_7': 0.00051439,
        'radiance_mult_band_6': 0.0015261,
        'radiance_mult_band_5': 0.0061367,
        'radiance_mult_band_4': 0.010028,
        'radiance_mult_band_3': 0.011892,
        'radiance_mult_band_2': 0.012905,
        'radiance_mult_band_1': 0.012603,
        'radiance_mult_band_9': 0.0023984,
        'radiance_mult_band_8': 0.011349,
        'radiance_mult_band_11': 0.0003342,
        'radiance_mult_band_10': 0.0003342,
        'reflectance_add_band_9': -0.1,
        'reflectance_add_band_8': -0.1,
        'reflectance_add_band_7': -0.1,
        'reflectance_add_band_6': -0.1,
        'reflectance_add_band_5': -0.1,
        'reflectance_add_band_4': -0.1,
        'reflectance_add_band_3': -0.1,
        'reflectance_add_band_2': -0.1,
        'reflectance_add_band_1': -0.1
    },
    'PRODUCT_METADATA': {
        'sensor_id': 'OLI_TIRS',
        'thermal_lines': 8881,
        'elevation_source': 'GLS2000',
        'corner_ll_projection_y_product': 7013987.5,
        'cpf_name': 'L8CPF20141001_20141231.01',
        'reflective_lines': 8881,
        'wrs_path': 101,
        'corner_ul_lon_product': 133.97954,
        'bpf_name_tirs': 'LT8BPF20141012002432_20141012011154.02',
        'corner_ll_lat_product': -26.99236,
        'panchromatic_lines': 17761,
        'scene_center_time': datetime.time(0, 56, 6, 5785),
        'metadata_file_name': 'LC81010782014285LGN00_MTL.txt',
        'spacecraft_id': 'LANDSAT_8',
        'file_name_band_quality': 'LC81010782014285LGN00_BQA.TIF',
        'panchromatic_samples': 18241,
        'corner_ul_lat_product': -24.98805,
        'corner_ur_lat_product': -24.9864,
        'corner_ll_lon_product': 133.96208,
        'corner_lr_projection_y_product': 7013987.5,
        'date_acquired': datetime.date(2014, 10, 12),
        'target_wrs_row': 78,
        'data_type': 'L1T',
        'corner_lr_lat_product': -26.99055,
        'output_format': 'GEOTIFF',
        'file_name_band_10': 'LC81010782014285LGN00_B10.TIF',
        'corner_ll_projection_x_product': 397012.5,
        'wrs_row': 78,
        'corner_ul_projection_y_product': 7235987.5,
        'bpf_name_oli': 'LO8BPF20141012002825_20141012011100.01',
        'file_name_band_7': 'LC81010782014285LGN00_B7.TIF',
        'corner_ur_projection_y_product': 7235987.5,
        'corner_ul_projection_x_product': 397012.5,
        'thermal_samples': 9121,
        'file_name_band_3': 'LC81010782014285LGN00_B3.TIF',
        'file_name_band_2': 'LC81010782014285LGN00_B2.TIF',
        'file_name_band_1': 'LC81010782014285LGN00_B1.TIF',
        'nadir_offnadir': 'NADIR',
        'file_name_band_6': 'LC81010782014285LGN00_B6.TIF',
        'file_name_band_5': 'LC81010782014285LGN00_B5.TIF',
        'file_name_band_4': 'LC81010782014285LGN00_B4.TIF',
        'reflective_samples': 9121,
        'corner_ur_projection_x_product': 625012.5,
        'file_name_band_8': 'LC81010782014285LGN00_B8.TIF',
        'corner_lr_projection_x_product': 625012.5,
        'corner_ur_lon_product': 136.23866,
        'file_name_band_11': 'LC81010782014285LGN00_B11.TIF',
        'corner_lr_lon_product': 136.25985,
        'target_wrs_path': 101,
        'file_name_band_9': 'LC81010782014285LGN00_B9.TIF',
        'rlut_file_name': 'L8RLUT20130211_20431231v09.h5'
    },
    'PROJECTION_PARAMETERS': {
        'utm_zone': -53,
        'grid_cell_size_reflective': 25.0,
        'map_projection': 'UTM',
        'orientation': 'NORTH_UP',
        'ellipsoid': 'GRS80',
        'grid_cell_size_thermal': 25.0,
        'datum': 'GDA94',
        'grid_cell_size_panchromatic': 12.5,
        'resampling_option': 'CUBIC_CONVOLUTION'
    },
    'METADATA_FILE_INFO': {
        'origin': 'Image courtesy of the U.S. Geological Survey',
        'landsat_scene_id': 'LC81010782014285LGN00',
        'processing_software_version': 'LPGS_2.3.0',
        'file_date': datetime.datetime(2014, 11, 12, 15, 8, 35),
        'station_id': 'LGN',
        'request_id': '101_078_078'
    },
    'MIN_MAX_PIXEL_VALUE': {
        'quantize_cal_min_band_11': 1,
        'quantize_cal_min_band_10': 1,
        'quantize_cal_max_band_11': 65535,
        'quantize_cal_max_band_10': 65535,
        'quantize_cal_max_band_5': 65535,
        'quantize_cal_max_band_4': 65535,
        'quantize_cal_max_band_7': 65535,
        'quantize_cal_max_band_6': 65535,
        'quantize_cal_max_band_1': 65535,
        'quantize_cal_max_band_3': 65535,
        'quantize_cal_max_band_2': 65535,
        'quantize_cal_max_band_9': 65535,
        'quantize_cal_max_band_8': 65535,
        'quantize_cal_min_band_9': 1,
        'quantize_cal_min_band_8': 1,
        'quantize_cal_min_band_7': 1,
        'quantize_cal_min_band_6': 1,
        'quantize_cal_min_band_5': 1,
        'quantize_cal_min_band_4': 1,
        'quantize_cal_min_band_3': 1,
        'quantize_cal_min_band_2': 1,
        'quantize_cal_min_band_1': 1
    },
    'MIN_MAX_RADIANCE': {
        'radiance_minimum_band_6': -7.62918,
        'radiance_minimum_band_7': -2.57144,
        'radiance_minimum_band_4': -50.13046,
        'radiance_minimum_band_5': -30.67734,
        'radiance_minimum_band_2': -64.51353,
        'radiance_minimum_band_3': -59.44866,
        'radiance_minimum_band_1': -63.00074,
        'radiance_maximum_band_1': 762.90253,
        'radiance_maximum_band_2': 781.2215,
        'radiance_maximum_band_3': 719.88898,
        'radiance_maximum_band_4': 607.0509,
        'radiance_maximum_band_5': 371.48489,
        'radiance_maximum_band_6': 92.38491,
        'radiance_maximum_band_7': 31.13866,
        'radiance_minimum_band_10': 0.10033,
        'radiance_minimum_band_11': 0.10033,
        'radiance_minimum_band_8': -56.73389,
        'radiance_maximum_band_8': 687.01459,
        'radiance_minimum_band_9': -11.9894,
        'radiance_maximum_band_10': 22.0018,
        'radiance_maximum_band_11': 22.0018,
        'radiance_maximum_band_9': 145.18472
    },
    'MIN_MAX_REFLECTANCE': {
        'reflectance_maximum_band_8': 1.2107,
        'reflectance_minimum_band_8': -0.09998,
        'reflectance_maximum_band_9': 1.2107,
        'reflectance_minimum_band_9': -0.09998,
        'reflectance_minimum_band_4': -0.09998,
        'reflectance_minimum_band_5': -0.09998,
        'reflectance_minimum_band_6': -0.09998,
        'reflectance_minimum_band_7': -0.09998,
        'reflectance_minimum_band_1': -0.09998,
        'reflectance_minimum_band_2': -0.09998,
        'reflectance_minimum_band_3': -0.09998,
        'reflectance_maximum_band_6': 1.2107,
        'reflectance_maximum_band_7': 1.2107,
        'reflectance_maximum_band_4': 1.2107,
        'reflectance_maximum_band_5': 1.2107,
        'reflectance_maximum_band_2': 1.2107,
        'reflectance_maximum_band_3': 1.2107,
        'reflectance_maximum_band_1': 1.2107
    }
}

_EXPECTED_OUT = DatasetMetadata(
    id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'),
    usgs_dataset_id='LC81010782014285LGN00',
    product_type='L1T',
    creation_dt=datetime.datetime(2014, 11, 12, 15, 8, 35),
    platform=PlatformMetadata(
        code='LANDSAT_8'
    ),
    instrument=InstrumentMetadata(
        name='OLI_TIRS'
    ),
    format_=FormatMetadata(
        name='GEOTIFF'
    ),
    acquisition=AcquisitionMetadata(
        groundstation=GroundstationMetadata(
            code='LGN'
        )
    ),
    extent=ExtentMetadata(
        coord=Polygon(
            ul=Coord(
                lat=-24.98805,
                lon=133.97954
            ),
            ur=Coord(
                lat=-24.9864,
                lon=136.23866
            ),
            ll=Coord(
                lat=-26.99236,
                lon=133.96208
            ),
            lr=Coord(
                lat=-26.99055,
                lon=136.25985
            )
        ),
        center_dt=datetime.datetime(2014, 10, 12, 0, 56, 6, 5785)
    ),
    grid_spatial=GridSpatialMetadata(
        projection=ProjectionMetadata(
            geo_ref_points=Polygon(
                ul=Point(
                    x=397012.5,
                    y=7235987.5
                ),
                ur=Point(
                    x=625012.5,
                    y=7235987.5
                ),
                ll=Point(
                    x=397012.5,
                    y=7013987.5
                ),
                lr=Point(
                    x=625012.5,
                    y=7013987.5
                )
            ),
            datum='GDA94',
            ellipsoid='GRS80',
            map_projection='UTM',
            orientation='NORTH_UP',
            resampling_option='CUBIC_CONVOLUTION',
            zone=-53
        )
    ),
    image=ImageMetadata(
        satellite_ref_point_start=Point(x=101, y=78),
        cloud_cover_percentage=0.01,
        sun_azimuth=59.57807899,
        sun_elevation=57.89670734,
        sun_earth_distance=0.998137,
        ground_control_points_model=420,
        geometric_rmse_model=4.61,
        geometric_rmse_model_x=2.968,
        geometric_rmse_model_y=3.527,
        bands={
            '11': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B11.TIF'),
                number='11',
            ),
            '10': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B10.TIF'),
                number='10',
            ),
            '1': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B1.TIF'),
                number='1',
            ),
            '3': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B3.TIF'),
                number='3',
            ),
            '2': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B2.TIF'),
                number='2',
            ),
            '5': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B5.TIF'),
                number='5',
            ),
            '4': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B4.TIF'),
                number='4',
            ),
            '7': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B7.TIF'),
                number='7',
            ),
            '6': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B6.TIF'),
                number='6',
            ),
            '9': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B9.TIF'),
                number='9',
            ),
            '8': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B8.TIF'),
                number='8',
            ),
            'quality': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_BQA.TIF'),
                number='quality',
            )}
    ),
    lineage=LineageMetadata(
        algorithm=AlgorithmMetadata(
            name='LPGS',
            version='2.3.0',
            parameters={}
        ),
        ancillary={
            'rlut': AncillaryMetadata(
                name='L8RLUT20130211_20431231v09.h5'
            ),
            'bpf_tirs': AncillaryMetadata(
                name='LT8BPF20141012002432_20141012011154.02'
            ),
            'bpf_oli': AncillaryMetadata(
                name='LO8BPF20141012002825_20141012011100.01'
            ),
            'cpf': AncillaryMetadata(
                name='L8CPF20141001_20141231.01'
            )}
    )
)


class TestMtlRead(unittest.TestCase):
    def test_equivalence(self):
        ds = DatasetMetadata(id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'))
        ds = mtl.populate_from_mtl_dict(ds, _PARSED_MTL, folder=Path('/tmp/fake-folder'))
        self.assertEqual(ds, _EXPECTED_OUT)

        # Sanity check: different dataset_id is not equal.
        ds = DatasetMetadata()
        ds = mtl.populate_from_mtl_dict(ds, _PARSED_MTL, folder=Path('/tmp/fake-folder'))
        self.assertNotEqual(ds, _EXPECTED_OUT)