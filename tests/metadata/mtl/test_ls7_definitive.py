# coding=utf-8
from __future__ import absolute_import
import unittest
import uuid
import datetime
import os

from pathlib import Path, PosixPath
import eodatasets.type as ptype
from tests.metadata.mtl import assert_expected_mtl


FILENAME = 'ls7_definitive_mtl.txt'

EXPECTED_OUT = ptype.DatasetMetadata(
    id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'),
    product_level='L1G',
    creation_dt=datetime.datetime(2015, 4, 7, 1, 58, 25),
    platform=ptype.PlatformMetadata(
        code='LANDSAT_7'
    ),
    instrument=ptype.InstrumentMetadata(
        name='ETM',
        operation_mode='SAM'
    ),
    format_=ptype.FormatMetadata(
        name='GeoTIFF'
    ),
    acquisition=ptype.AcquisitionMetadata(
        groundstation=ptype.GroundstationMetadata(
            code='ASA'
        )
    ),
    usgs=ptype.UsgsMetadata(
        scene_id='LE71140732005007ASA00'
    ),
    extent=ptype.ExtentMetadata(
        coord=ptype.CoordPolygon(
            ul=ptype.Coord(
                lat=-17.82157,
                lon=115.58472
            ),
            ur=ptype.Coord(
                lat=-17.82497,
                lon=117.82111
            ),
            ll=ptype.Coord(
                lat=-19.72798,
                lon=115.56872
            ),
            lr=ptype.Coord(
                lat=-19.73177,
                lon=117.83040
            )
        ),
        # TODO: python dt is one digit less precise than mtl (02:03:36.9270519Z). Does this matter?
        center_dt=datetime.datetime(2005, 1, 7, 2, 3, 36, 927051)
    ),
    grid_spatial=ptype.GridSpatialMetadata(
        projection=ptype.ProjectionMetadata(
            geo_ref_points=ptype.PointPolygon(
                ul=ptype.Point(
                    x=350012.500,
                    y=8028987.500
                ),
                ur=ptype.Point(
                    x=587012.500,
                    y=8028987.500
                ),
                ll=ptype.Point(
                    x=350012.500,
                    y=7817987.500
                ),
                lr=ptype.Point(
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
    image=ptype.ImageMetadata(
        satellite_ref_point_start=ptype.Point(x=114, y=73),
        cloud_cover_percentage=0.0,
        sun_azimuth=102.37071009,
        sun_elevation=58.08261077,
        # sun_earth_distance=0.998137,
        # ground_control_points_version=2,
        # ground_control_points_model=47,
        # geometric_rmse_model=4.582,
        # geometric_rmse_model_x=3.370,
        # geometric_rmse_model_y=3.104,
        bands={}
    ),
    lineage=ptype.LineageMetadata(
        algorithm=ptype.AlgorithmMetadata(
            name='LPGS',
            version='12.5.0',
            parameters={}
        ),
        ancillary_quality='DEFINITIVE',
        ancillary={
            'cpf': ptype.AncillaryMetadata(
                name='L7CPF20050101_20050331.09'
            ),
            # We have the properties (quality) of the ancillary but not the file.
            'ephemeris': ptype.AncillaryMetadata(
                properties={'type': 'DEFINITIVE'}
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
