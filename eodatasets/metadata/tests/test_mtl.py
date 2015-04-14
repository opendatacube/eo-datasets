import unittest

import datetime
from pathlib import Path, PosixPath

from eodatasets.metadata import mtl
from eodatasets.type import *


_EXPECTED_LS8_OUT = DatasetMetadata(
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


_EXPECTED_LT5_OUT = DatasetMetadata(
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
    def test_ls8_equivalence(self):

        mtl_path = Path(os.path.join(os.path.dirname(__file__), 'data', 'LC8_MTL.txt'))

        ds = DatasetMetadata(id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'))
        ds = mtl.populate_from_mtl(ds, mtl_path, base_folder=Path('/tmp/fake-folder'))

        _assert_same(ds, _EXPECTED_LS8_OUT)

        # Sanity check: different dataset_id is not equal.
        ds = DatasetMetadata()
        ds = mtl.populate_from_mtl(ds, mtl_path, base_folder=Path('/tmp/fake-folder'))
        self.assertNotEqual(ds, _EXPECTED_LS8_OUT)

    def test_ls5_equivalence(self):

        mtl_path = Path(os.path.join(os.path.dirname(__file__), 'data', 'LT5_MTL.txt'))

        ds = DatasetMetadata(id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'))
        ds = mtl.populate_from_mtl(ds, mtl_path, base_folder=Path('/tmp/fake-folder'))

        _assert_same(ds, _EXPECTED_LT5_OUT)


def _assert_same(o1, o2, prefix=''):
    """
    Assert the two are equal.

    Compares property values one-by-one recursively to print friendly error messages.

    (ie. the exact property that differs)

    :type o1: object
    :type o2: object
    :raises: AssertionError
    """

    def _compare(k, val1, val2):
        _assert_same(val1, val2, prefix=prefix+'.'+str(k))

    if isinstance(o1, SimpleObject):
        assert o1.__class__ == o2.__class__, "Differing classes %r: %r and %r" \
                                             % (prefix, o1.__class__.__name__, o2.__class__.__name__)

        for k, val in o1.items_ordered(skip_nones=False):
            _compare(k, val, getattr(o2, k))
    elif isinstance(o1, list):
        assert len(o1) == len(o2), "Differing lengths: %s" % prefix

        for i, val in enumerate(o1):
            _compare(i, val, o2[i])
    elif isinstance(o1, dict):
        assert len(o1) == len(o2), "Differing lengths: %s" % prefix

        for k, val in o1.items():
            _compare(k, val, o2[k])
    elif o1 != o2:
        print repr(o1)
        print repr(o2)
        raise AssertionError("Mismatch for property %r:  %r != %r" % (prefix, o1, o2))

if __name__ == '__main__':

    import unittest
    unittest.main()
