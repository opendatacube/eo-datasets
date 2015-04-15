import datetime
import unittest
from pathlib import Path, PosixPath
from eodatasets.type import *

from eodatasets.metadata.tests.mtl import assert_expected_mtl

FILENAME = 'ls8_mtl.txt'

EXPECTED_OUT = DatasetMetadata(
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
        coord=CoordPolygon(
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
            geo_ref_points=PointPolygon(
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
    def test_ls8_equivalence(self):
        assert_expected_mtl(
            Path(os.path.join(os.path.dirname(__file__), FILENAME)),
            EXPECTED_OUT
        )