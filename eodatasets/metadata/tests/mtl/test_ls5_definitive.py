import unittest
from pathlib import PosixPath, Path

from eodatasets.type import *

from eodatasets.metadata.tests.mtl import assert_expected_mtl

FILENAME = 'ls5_definitive_mtl.txt'

EXPECTED_OUT = DatasetMetadata(
    id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'),
    usgs_dataset_id='LT51130632005152ASA00',
    product_type='L1T',
    creation_dt=datetime.datetime(2015, 4, 7, 1, 12, 3),
    platform=PlatformMetadata(
        code='LANDSAT_5'
    ),
    instrument=InstrumentMetadata(
        name='TM',
        operation_mode='BUMPER'
    ),
    format_=FormatMetadata(
        name='GEOTIFF'
    ),
    acquisition=AcquisitionMetadata(
        groundstation=GroundstationMetadata(
            code='ASA'
        )
    ),
    extent=ExtentMetadata(
        coord=CoordPolygon(
            ul=Coord(
                lat=-3.38926,
                lon=120.38133
            ),
            ur=Coord(
                lat=-3.39269,
                lon=122.51399
            ),
            ll=Coord(
                lat=-5.26901,
                lon=120.37486
            ),
            lr=Coord(
                lat=-5.27436,
                lon=122.51278
            )
        ),
        center_dt=datetime.datetime(2005, 6, 1, 1, 51, 30, 434044)
    ),
    grid_spatial=GridSpatialMetadata(
        projection=ProjectionMetadata(
            geo_ref_points=PointPolygon(
                ul=Point(
                    x=209012.500,
                    y=9624987.500
                ),
                ur=Point(
                    x=446012.500,
                    y=9624987.500
                ),
                ll=Point(
                    x=209012.500,
                    y=9416987.500
                ),
                lr=Point(
                    x=446012.500,
                    y=9416987.500
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
        satellite_ref_point_start=Point(x=113, y=63),
        cloud_cover_percentage=52.00,
        sun_azimuth=46.93282849,
        sun_elevation=50.44317205,
        # sun_earth_distance=0.998137,
        ground_control_points_version=2,
        ground_control_points_model=47,
        geometric_rmse_model=4.582,
        geometric_rmse_model_x=3.370,
        geometric_rmse_model_y=3.104,
        bands={
            '1': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B1.TIF'),
                number='1',
            ),

            '2': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B2.TIF'),
                number='2',
            ),
            '3': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B3.TIF'),
                number='3',
            ),
            '4': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B4.TIF'),
                number='4',
            ),
            '5': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B5.TIF'),
                number='5',
            ),
            '6': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B6.TIF'),
                number='6',
            ),
            '7': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B7.TIF'),
                number='7',
            )
        }
    ),
    lineage=LineageMetadata(
        algorithm=AlgorithmMetadata(
            name='LPGS',
            version='12.5.0',
            parameters={}
        ),
        ancillary_quality='DEFINITIVE',
        ancillary={
            'cpf': AncillaryMetadata(
                name='L5CPF20050401_20050630.12'
            )}
    )
)


class TestMtlRead(unittest.TestCase):
    def test_ls5_equivalence(self):
        assert_expected_mtl(
            Path(os.path.join(os.path.dirname(__file__), FILENAME)),
            EXPECTED_OUT
        )
