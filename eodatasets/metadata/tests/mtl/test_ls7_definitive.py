# coding=utf-8
from __future__ import absolute_import
import unittest

from pathlib import Path, PosixPath
from eodatasets.type import *
from eodatasets.metadata.tests.mtl import assert_expected_mtl


FILENAME = 'ls7_definitive_mtl.txt'

EXPECTED_OUT = DatasetMetadata(
    id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'),
    usgs_dataset_id='LE71140732005007ASA00',
    product_level='L1G',
    creation_dt=datetime.datetime(2015, 4, 7, 1, 58, 25),
    platform=PlatformMetadata(
        code='LANDSAT_7'
    ),
    instrument=InstrumentMetadata(
        name='ETM',
        operation_mode='SAM'
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
                lat=-17.82157,
                lon=115.58472
            ),
            ur=Coord(
                lat=-17.82497,
                lon=117.82111
            ),
            ll=Coord(
                lat=-19.72798,
                lon=115.56872
            ),
            lr=Coord(
                lat=-19.73177,
                lon=117.83040
            )
        ),
        # TODO: python dt is one digit less precise than mtl (02:03:36.9270519Z). Does this matter?
        center_dt=datetime.datetime(2005, 1, 7, 2, 3, 36, 927051)
    ),
    grid_spatial=GridSpatialMetadata(
        projection=ProjectionMetadata(
            geo_ref_points=PointPolygon(
                ul=Point(
                    x=350012.500,
                    y=8028987.500
                ),
                ur=Point(
                    x=587012.500,
                    y=8028987.500
                ),
                ll=Point(
                    x=350012.500,
                    y=7817987.500
                ),
                lr=Point(
                    x=587012.500,
                    y=7817987.500
                )
            ),
            datum='GDA94',
            ellipsoid='GRS80',
            map_projection='UTM',
            orientation='NORTH_UP',
            resampling_option='CUBIC_CONVOLUTION',
            zone=-50
        )
    ),
    image=ImageMetadata(
        satellite_ref_point_start=Point(x=114, y=73),
        cloud_cover_percentage=0.0,
        sun_azimuth=102.37071009,
        sun_elevation=58.08261077,
        # sun_earth_distance=0.998137,
        # ground_control_points_version=2,
        # ground_control_points_model=47,
        # geometric_rmse_model=4.582,
        # geometric_rmse_model_x=3.370,
        # geometric_rmse_model_y=3.104,
        bands={
            '1': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B1.TIF'),
                number='1',
            ),

            '2': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B2.TIF'),
                number='2',
            ),
            '3': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B3.TIF'),
                number='3',
            ),
            '4': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B4.TIF'),
                number='4',
            ),
            '5': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B5.TIF'),
                number='5',
            ),
            '6_vcid_1': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B6_VCID_1.TIF'),
                number='6_vcid_1',
            ),
            '6_vcid_2': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B6_VCID_2.TIF'),
                number='6_vcid_2',
            ),
            '7': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B7.TIF'),
                number='7',
            ),
            '8': BandMetadata(
                path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B8.TIF'),
                number='8',
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
                name='L7CPF20050101_20050331.09'
            )
        }
    )
)


class TestMtlRead(unittest.TestCase):
    def test_ls7_equivalence(self):
        assert_expected_mtl(
            Path(os.path.join(os.path.dirname(__file__), FILENAME)),
            EXPECTED_OUT
        )