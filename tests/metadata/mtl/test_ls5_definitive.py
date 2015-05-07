# coding=utf-8
from __future__ import absolute_import
import unittest
import uuid
import datetime
import os

from pathlib import Path

import eodatasets.type as ptype
from tests.metadata.mtl import assert_expected_mtl


FILENAME = 'ls5_definitive_mtl.txt'

EXPECTED_OUT = ptype.DatasetMetadata(
    id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'),
    usgs_dataset_id='LT51130632005152ASA00',
    product_level='L1T',
    creation_dt=datetime.datetime(2015, 4, 7, 1, 12, 3),
    platform=ptype.PlatformMetadata(
        code='LANDSAT_5'
    ),
    instrument=ptype.InstrumentMetadata(
        name='TM',
        operation_mode='BUMPER'
    ),
    format_=ptype.FormatMetadata(
        name='GEOTIFF'
    ),
    acquisition=ptype.AcquisitionMetadata(
        groundstation=ptype.GroundstationMetadata(
            code='ASA'
        )
    ),
    extent=ptype.ExtentMetadata(
        coord=ptype.CoordPolygon(
            ul=ptype.Coord(
                lat=-3.38926,
                lon=120.38133
            ),
            ur=ptype.Coord(
                lat=-3.39269,
                lon=122.51399
            ),
            ll=ptype.Coord(
                lat=-5.26901,
                lon=120.37486
            ),
            lr=ptype.Coord(
                lat=-5.27436,
                lon=122.51278
            )
        ),
        center_dt=datetime.datetime(2005, 6, 1, 1, 51, 30, 434044)
    ),
    grid_spatial=ptype.GridSpatialMetadata(
        projection=ptype.ProjectionMetadata(
            geo_ref_points=ptype.PointPolygon(
                ul=ptype.Point(
                    x=209012.500,
                    y=9624987.500
                ),
                ur=ptype.Point(
                    x=446012.500,
                    y=9624987.500
                ),
                ll=ptype.Point(
                    x=209012.500,
                    y=9416987.500
                ),
                lr=ptype.Point(
                    x=446012.500,
                    y=9416987.500
                )
            ),
            datum='GDA94',
            ellipsoid='GRS80',
            map_projection='UTM',
            orientation='NORTH_UP',
            resampling_option='CUBIC_CONVOLUTION',
            zone=-51
        )
    ),
    image=ptype.ImageMetadata(
        satellite_ref_point_start=ptype.Point(x=113, y=63),
        cloud_cover_percentage=52.00,
        sun_azimuth=46.93282849,
        sun_elevation=50.44317205,
        # sun_earth_distance=0.998137,
        # ground_control_points_version=2,
        ground_control_points_model=47,
        geometric_rmse_model=4.582,
        geometric_rmse_model_x=3.104,
        geometric_rmse_model_y=3.370,
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
