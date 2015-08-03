# coding=utf-8
from __future__ import absolute_import
import logging
import unittest
import uuid
import os
import datetime

import dateutil.parser

from pathlib import Path
from eodatasets import type as ptype, serialise
from tests import temp_file, assert_same, TestCase


def _serialise_to_file(file_name, dataset):
    """

    :type file_name: str
    :type dataset: ptype.DatasetMetadata
    :return:
    """
    serialise.write_yaml_metadata(dataset, '%s.yaml' % file_name, os.getcwd())
    serialise.write_property_metadata(dataset, '%s.properties' % file_name, os.getcwd())


def _build_ls8_raw():
    _reset_runtime_id()
    raw = ptype.DatasetMetadata(
        id_=uuid.UUID('1c76a8ca-51ae-11e4-8644-0050568d59ac'),
        creation_dt=dateutil.parser.parse("2014-10-12 04:18:01"),
        size_bytes=5680940 * 1024,
        ga_label='MD_P00_LC81010700832014285LGN00_101_070-083_20141012T032336Z20141012T032910_1',
        product_type='satellite_telemetry_data',
        usgs=ptype.UsgsMetadata(
            interval_id='LC81010782014285LGN00'
        ),
        platform=ptype.PlatformMetadata(code='LANDSAT-8'),
        instrument=ptype.InstrumentMetadata(name='OLI_TIRS'),
        format_=ptype.FormatMetadata(name='MD'),
        acquisition=ptype.AcquisitionMetadata(
            aos=dateutil.parser.parse('2014-10-12T00:52:52'),
            los=dateutil.parser.parse('2014-10-12T00:58:37'),
            groundstation=ptype.GroundstationMetadata(
                code='ASA',
                antenna_coord=ptype.Coord(
                    lat=-23.759,
                    lon=133.8824,
                    height=579.312
                )
            ),
            heading='D',
            platform_orbit=8846
        ),
        extent=None,
        grid_spatial=None,
        browse=None,
        image=ptype.ImageMetadata(
            satellite_ref_point_start=ptype.Point(101, 70),
            satellite_ref_point_end=ptype.Point(101, 83)
        ),
        lineage=ptype.LineageMetadata(
            machine=ptype.MachineMetadata()
        )
    )
    return raw


def _build_ls8_ortho():
    _reset_runtime_id()
    return ptype.DatasetMetadata(
        id_=uuid.UUID('17b92c16-51d3-11e4-909d-005056bb6972'),
        ga_label='LS8_OLITIRS_OTH_P51_GALPGS01-002_101_078_20141012',
        product_type='GAORTHO01',
        creation_dt=dateutil.parser.parse('2014-10-12 05:46:20'),
        size_bytes=2386550 * 1024,
        platform=ptype.PlatformMetadata(code='LANDSAT-8'),
        instrument=ptype.InstrumentMetadata(
            name='OLI_TIRS',
            type_="Multi-Spectral",
            operation_mode='PUSH-BROOM'
        ),
        format_=ptype.FormatMetadata(name='GeoTiff', version=1),
        extent=ptype.ExtentMetadata(
            reference_system='WGS84',
            coord=ptype.CoordPolygon(
                ul=ptype.Coord(lat=-24.97,
                               lon=133.97969),
                ur=ptype.Coord(lat=-24.96826,
                               lon=136.24838),
                lr=ptype.Coord(lat=-26.96338,
                               lon=136.26962),
                ll=ptype.Coord(lat=-26.96528,
                               lon=133.96233)
            ),
            from_dt=dateutil.parser.parse("2014-10-12T00:55:54"),
            center_dt=dateutil.parser.parse("2014-10-12T00:56:06"),
            to_dt=dateutil.parser.parse("2014-10-12T00:56:18"),

        ),
        grid_spatial=ptype.GridSpatialMetadata(
            dimensions=[
                ptype.DimensionMetadata(name='sample', resolution=25.0, size=9161),
                ptype.DimensionMetadata(name='line', resolution=25.0, size=9161)
            ],
            projection=ptype.ProjectionMetadata(
                centre_point=ptype.Point(511512.500000, 7127487.500000),
                geo_ref_points=ptype.PointPolygon(
                    ul=ptype.Point(397012.5, 7237987.5),
                    ur=ptype.Point(626012.5, 7237987.5),
                    ll=ptype.Point(397012.5, 7016987.5),
                    lr=ptype.Point(626012.5, 7016987.5)
                ),
                datum='GDA94',
                ellipsoid='GRS80',
                point_in_pixel='UL',
                map_projection='UTM',
                resampling_option='CUBIC_CONVOLUTION',
                zone=-53
            )
        ),
        browse={
            'medium':
                ptype.BrowseMetadata(
                    path=Path('product/LS8_OLITIRS_OTH_P51_GALPGS01-032_101_078_20141012.jpg'),
                    file_type='image/jpg',
                    cell_size=219.75,
                    red_band=7,
                    green_band=5,
                    blue_band=1
                ),
            'full':
                ptype.BrowseMetadata(
                    path=Path('LS8_OLITIRS_OTH_P51_GALPGS01-032_101_078_20141012_FR.jpg'),
                    file_type='image/jpg',
                    cell_size=25.0,
                    red_band=7,
                    green_band=5,
                    blue_band=1
                )
        },
        image=ptype.ImageMetadata(
            satellite_ref_point_start=ptype.Point(101, 78),
            cloud_cover_percentage=0,
            cloud_cover_details=None,

            sun_elevation=58.00268508,
            sun_azimuth=59.41814014,

            ground_control_points_model=420,
            geometric_rmse_model=4.610,
            geometric_rmse_model_x=3.527,
            geometric_rmse_model_y=2.968,

            # TODO: What are these two?
            viewing_incidence_angle_long_track=0,
            viewing_incidence_angle_x_track=0,

            bands={
                'coastal_aerosol': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B1.TIF'),
                    number=1,
                    type_='reflective',
                    cell_size=25.0,

                ),
                'visible_blue': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B2.TIF'),
                    number=2,
                    type_='reflective',
                    cell_size=25.0,

                ),
                'visible_green': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B3.TIF'),
                    number=3,
                    type_='reflective',
                    cell_size=25.0,

                ),
                'visible_red': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B4.TIF'),
                    number=4,
                    type_='reflective',
                    cell_size=25.0,

                ),
                'near_infrared': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B5.TIF'),
                    number=5,
                    type_='reflective',
                    cell_size=25.0,

                ),
                'short_wave_infrared1': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B6.TIF'),
                    number=6,
                    type_='reflective',
                    cell_size=25.0,

                ),
                'short_wave_infrared2': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B7.TIF'),
                    number=7,
                    type_='reflective',
                    cell_size=25.0,

                ),
                'panchromatic': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B8.TIF'),
                    number=8,
                    type_='panchromatic',
                    cell_size=12.50,
                    shape=ptype.Point(17761, 18241),

                ),
                'cirrus': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B9.TIF'),
                    number=9,
                    type_='atmosphere',

                ),
                'thermal_infrared1': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B10.TIF'),
                    number=10,
                    type_='thermal',
                    cell_size=25.0,
                    shape=ptype.Point(8881, 9121),

                ),
                'thermal_infrared2': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B11.TIF'),
                    number=11,
                    type_='thermal',
                    cell_size=25.0,
                    shape=ptype.Point(8881, 9121),

                ),
                'quality': ptype.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_BQA.TIF'),
                    number='QA',
                    type_='quality',

                )
            }
        ),
        lineage=ptype.LineageMetadata(
            algorithm=ptype.AlgorithmMetadata(
                name='Pinkmatter Landsat Processor',
                version='3.3.3104',
                parameters={
                    'resampling': 'CC',
                    'radiometric_correction': 'CPF',
                    'orientation': 'NUP',
                    'hemisphere': 'S',
                }
            ),
            machine=ptype.MachineMetadata(hostname='rhe-jm-prod08.prod.lan', type_id='jobmanager',
                                          uname='Linux rhe-jm-dev08.dev.lan 2.6.32-279.22.1.el6.x86_64 #1 SMP Sun Oct '
                                                '12 '
                                                '09:21:40 EST 2014 x86_64 x86_64 x86_64 GNU/Linux'),
            ancillary={
                'cpf':
                    ptype.AncillaryMetadata(name='L8CPF20141001_20141231.01',
                                            uri='/eoancillarydata/sensor-specific/LANDSAT8/CalibrationParameterFile'
                                                '/L8CPF20141001_20141231.01'),
                'bpf_tirs':
                    ptype.AncillaryMetadata(name='LT8BPF20141012002432_20141012020301.01',
                                            uri='/eoancillarydata/sensor-specific/LANDSAT8/BiasParameterFile/2014/10'
                                                '/LT8BPF20141012002432_20141012020301.01'),
                'bpf_oli':
                    ptype.AncillaryMetadata(name='LO8BPF20141012002825_20141012011100.01',
                                            uri='/eoancillarydata/sensor-specific/LANDSAT8/BiasParameterFile/2014/10'
                                                '/LT8BPF20141012002432_20141012020301.01'),
                'rlut':
                    ptype.AncillaryMetadata(name='L8RLUT20130211_20431231v09.h5')
            },
            source_datasets={'satellite_telemetry_data': _build_ls8_raw()}
        )
    )


def _reset_runtime_id():
    """
    Regenerate the runtime id to simulate creation on different hosts/days.
    :return:
    """
    ptype._RUNTIME_ID = uuid.uuid1()


def _build_ls7_wofs():
    return ptype.DatasetMetadata(
        ga_label='LS7_ETM_WATER_140_-027_2013-07-24T00-32-27.952897',
        product_type='GAWATER',
        size_bytes=616 * 1024,
        platform=ptype.PlatformMetadata(code='LS7'),
        instrument=ptype.InstrumentMetadata(
            name='ETM',
            type_='Multi-Spectral'
        ),
        format_=ptype.FormatMetadata('GeoTIFF', version=1),
        extent=ptype.ExtentMetadata(
            reference_system='WGS84',
            coord=ptype.CoordPolygon(
                ul=ptype.Coord(140.0000000, -26.0000000),
                ll=ptype.Coord(140.0000000, -27.0000000),
                ur=ptype.Coord(141.0000000, -26.0000000),
                lr=ptype.Coord(141.0000000, -27.0000000)
            ),

            # TODO: Should we store the center coordinate?

            from_dt=dateutil.parser.parse('2013-07-24 00:32:27.952897'),
            to_dt=dateutil.parser.parse('2013-07-24 00:33:15.899670')
        ),
        grid_spatial=ptype.GridSpatialMetadata(
            dimensions=[
                ptype.DimensionMetadata(name='x', resolution=27.1030749476, size=4000),
                ptype.DimensionMetadata(name='y', resolution=27.1030749476, size=4000)
            ],
            # TODO: Should WOfS have tile coordinates here?
            # georectified=ptype.GeoRectifiedSpacialMetadata(
            # geo_ref_points=PointPolygon(
            # ul=ptype.Point(255012.500, 7229987.500),
            # ur=ptype.Point(497012.500, 7229987.500),
            # ll=ptype.Point(255012.500, 7019987.500),
            # lr=ptype.Point(497012.500, 7229987.500)
            # ),
            # checkpoint_availability=0,
            # datum='GDA94',
            #     ellipsoid='GRS80',
            #     point_in_pixel='UL',
            #     projection='UTM',
            #     zone=-54
            # )
        ),
        image=ptype.ImageMetadata(
            satellite_ref_point_start=ptype.Point(98, 78),
            satellite_ref_point_end=ptype.Point(98, 79),
            cloud_cover_percentage=0.76494375,
            cloud_cover_details='122391 count',

            sun_elevation=33.0061002772,
            sun_azimuth=38.2433049177,

            bands={
                'W': ptype.BandMetadata(
                    path=Path('LS7_ETM_WATER_140_-027_2013-07-24T00-32-27.952897.tif'),
                    # TODO: Nodata value?
                )
            }
        ),
        lineage=ptype.LineageMetadata(
            algorithm=ptype.AlgorithmMetadata(name='WOfS', version='1.3', parameters={}),
            machine=ptype.MachineMetadata(),
            source_datasets={
                # TODO: LS7 dataset?
            }
        )
    )


def _build_ls8_nbar():
    _reset_runtime_id()
    nbar = ptype.DatasetMetadata(
        id_=uuid.UUID("249ae098-bd88-11e4-beaa-1040f381a756"),
        size_bytes=622208 * 1024,
        ga_label='LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012',
        product_type='GANBAR01',
        platform=ptype.PlatformMetadata(code='LANDSAT-8'),
        instrument=ptype.InstrumentMetadata(
            name='OLI_TIRS',
            type_="Multi-Spectral",
            operation_mode='PUSH-BROOM'
        ),
        # acquisition=ptype.AcquisitionMetadata(),
        format_=ptype.FormatMetadata(name='GeoTiff', version=1),
        extent=ptype.ExtentMetadata(
            reference_system='WGS84',
            coord=ptype.CoordPolygon(
                ul=ptype.Coord(lat=-24.97,
                               lon=133.97969),
                ur=ptype.Coord(lat=-24.96826,
                               lon=136.24838),
                lr=ptype.Coord(lat=-26.96338,
                               lon=136.26962),
                ll=ptype.Coord(lat=-26.96528,
                               lon=133.96233)
            ),
            from_dt=dateutil.parser.parse("2014-10-12T00:55:54"),
            to_dt=dateutil.parser.parse("2014-10-12T00:56:18"),

        ),
        grid_spatial=ptype.GridSpatialMetadata(
            dimensions=[
                ptype.DimensionMetadata(name='sample', resolution=25.0, size=9161),
                ptype.DimensionMetadata(name='line', resolution=25.0, size=9161)
            ],
            projection=ptype.ProjectionMetadata(
                centre_point=ptype.Point(511512.500000, 7127487.500000),
                geo_ref_points=ptype.PointPolygon(
                    ul=ptype.Point(397012.5, 7237987.5),
                    ur=ptype.Point(626012.5, 7237987.5),
                    ll=ptype.Point(397012.5, 7016987.5),
                    lr=ptype.Point(626012.5, 7016987.5)
                ),
                datum='GDA94',
                ellipsoid='GRS80',
                point_in_pixel='UL',
                map_projection='UTM',
                resampling_option='CUBIC_CONVOLUTION',
                zone=-53
            )
        ),
        browse={
            'medium':
                ptype.BrowseMetadata(
                    path=Path('LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012.tif'),
                    file_type='image/jpg',

                    cell_size=219.75,
                    red_band=7,
                    green_band=5,
                    blue_band=2
                ),
            'full':
                ptype.BrowseMetadata(
                    path=Path('LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_FR.tif'),
                    file_type='image/jpg',

                    cell_size=25.0,
                    red_band=7,
                    green_band=5,
                    blue_band=2
                )
        },
        image=ptype.ImageMetadata(
            satellite_ref_point_start=ptype.Point(101, 78),
            cloud_cover_percentage=0.01,
            cloud_cover_details=None,

            # TODO: What are these two?
            viewing_incidence_angle_long_track=0,
            viewing_incidence_angle_x_track=0,

            bands={
                '1': ptype.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B1.tif'),

                ),
                '2': ptype.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B2.tif'),

                ),
                '3': ptype.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B3.tif'),

                ),
                '4': ptype.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B4.tif'),

                ),
                '5': ptype.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B5.tif'),

                ),
                '6': ptype.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B6.tif'),

                ),
                '7': ptype.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B7.tif'),

                )
            }
        ),
        lineage=ptype.LineageMetadata(
            algorithm=ptype.AlgorithmMetadata(
                name='GANBAR',
                version='3.2.1',
                parameters={
                }
            ),
            machine=ptype.MachineMetadata(),
            source_datasets={
                'ortho': _build_ls8_ortho()
            },
            ancillary={}
        )
    )
    return nbar


class PackageTypeTests(unittest.TestCase):
    def test_equivalence(self):
        ls8_raw = _build_ls8_raw()
        self.assertEqual(ls8_raw, ls8_raw, msg='RAW mismatch')

        self.assertNotEqual(ls8_raw, _build_ls7_wofs(), msg='Different datasets should not be equal')

        ls8_nbar = _build_ls8_nbar()
        self.assertEqual(ls8_nbar, ls8_nbar, msg='NBAR mismatch')

    def test_raw_serialise(self):
        ls8_raw = _build_ls8_raw()

        # Serialise, deserialize, then compare to the original.
        yaml_file = temp_file(suffix='ls8-raw-test.yaml')
        serialise.write_yaml_metadata(ls8_raw, yaml_file)
        serialised_ls8_raw = serialise.read_yaml_metadata(yaml_file)
        self.assertEqual(ls8_raw, serialised_ls8_raw, msg='RAW mismatch')

    def _compare_bands(self, ds1, ds2):
        assert_same(ds1.image.bands, ds2.image.bands)

    def test_nbar_serialise(self):
        ls8_nbar = _build_ls8_nbar()

        # Serialise, deserialize, then compare to the original.
        yaml_file = temp_file(suffix='ls8-nbar-test.yaml')
        serialise.write_yaml_metadata(ls8_nbar, yaml_file)
        serialised_d = serialise.read_yaml_metadata(yaml_file)

        # Compare bands first
        self._compare_bands(ls8_nbar, serialised_d)

        self._compare_bands(
            ls8_nbar.lineage.source_datasets['ortho'],
            serialised_d.lineage.source_datasets['ortho']
        )

        # Clear bands to compare remaining object:
        ls8_nbar.image.bands, ls8_nbar.lineage.source_datasets['ortho'].image.bands = {}, {}
        serialised_d.image.bands, serialised_d.lineage.source_datasets['ortho'].image.bands = {}, {}

        assert_same(ls8_nbar, serialised_d)


class SimpleObjectTests(TestCase):
    def test_properties(self):
        class TestObj(ptype.SimpleObject):
            def __init__(self, a, b, c=42):
                self.a = a
                self.b = b
                self.c = c

        self.assert_values_equal(TestObj.item_defaults(), [('a', None), ('b', None), ('c', 42)])
        self.assert_values_equal(TestObj(1, 2, 3).items_ordered(), [('a', 1), ('b', 2), ('c', 3)])
        self.assert_values_equal(TestObj(1, 2).items_ordered(), [('a', 1), ('b', 2), ('c', 42)])

        self.assertEqual(repr(TestObj(1, 2)), "TestObj(a=1, b=2, c=42)")

        class TestAllDefaults(ptype.SimpleObject):
            def __init__(self, a=1, b=2, c=None):
                """
                :type a: int or None
                :type b: int or None
                :type c: int or None
                """
                self.a = a
                self.b = b
                self.c = c

        self.assert_values_equal(TestAllDefaults.item_defaults(), [('a', 1), ('b', 2), ('c', None)])
        self.assert_values_equal(TestAllDefaults(3, 2, 1).items_ordered(), [('a', 3), ('b', 2), ('c', 1)])

        # None handling: Blank values with blank defaults are not included by items_ordered()
        self.assert_values_equal(TestAllDefaults(2, 1).items_ordered(), [('a', 2), ('b', 1)])
        self.assertEqual(repr(TestAllDefaults()), "TestAllDefaults(a=1, b=2)")

        # Blank value with non-blank default is output.
        self.assert_values_equal(TestAllDefaults(2, None).items_ordered(), [('a', 2), ('b', None)])
        self.assertEqual(repr(TestAllDefaults(a=1, b=None)), "TestAllDefaults(a=1, b=None)")

    def test_from_dict(self):
        class TestObj(ptype.SimpleObject):
            def __init__(self, a, b, c=42):
                self.a = a
                self.b = b
                self.c = c

        self.assertEqual(TestObj.from_dict({'a': 1, 'b': 2, 'c': 3}), TestObj(1, 2, 3))
        self.assertEqual(TestObj.from_dict({'a': 1, 'b': 2}), TestObj(1, 2, 42))

    def test_from_dict_embedded_obj(self):
        class DoorHandle(ptype.SimpleObject):
            def __init__(self, a=42):
                self.a = a

        class Door(ptype.SimpleObject):
            PROPERTY_PARSERS = {
                'handle': DoorHandle.from_dict
            }

            def __init__(self, a=42, handle=None):
                self.a = a
                self.handle = handle

        class House(ptype.SimpleObject):
            PROPERTY_PARSERS = {
                'door': Door.from_dict
            }

            def __init__(self, door, b, c=42):
                self.door = door
                self.b = b
                self.c = c

        # Two levels
        self.assertEqual(
            House.from_dict({'b': 2, 'door': {'a': 1}}),
            House(Door(a=1), b=2)
        )

        # Three levels
        self.assertEqual(
            House.from_dict({'b': 2, 'door': {'handle': {'a': 111}}}),
            House(Door(a=42, handle=DoorHandle(a=111)), b=2)
        )

    def test_steal_properties_from(self):
        class TestObj(ptype.SimpleObject):
            def __init__(self, a, b=None, c=3):
                self.a = a
                self.b = b
                self.c = c

        # B and C are default values, so should be overridden.
        # A is already set, so should remain.
        o1 = TestObj(1)
        o1.steal_fields_from(TestObj(2, 3, 4))
        self.assertEqual(o1, TestObj(1, 3, 4))

        # Should be unmodified when stealing for an all-default value.
        clean = TestObj(1)
        o1.steal_fields_from(clean)
        self.assertEqual(o1, TestObj(1, 3, 4))

        # Unmodified when stealing from itself.
        o1.steal_fields_from(o1)
        self.assertEqual(o1, TestObj(1, 3, 4))

        # C is a default value and should be overridden.
        o1.c = 3
        o1.steal_fields_from(TestObj(9, 9, 9))
        self.assertEqual(o1, TestObj(1, 3, 9))

        # B is a default value and should be overridden.
        o1.b = None
        o1.steal_fields_from(TestObj(9, 9, 3))
        self.assertEqual(o1, TestObj(1, 9, 9))

        # A is compulsory and should be overridden if None.
        o1.a = None
        o1.steal_fields_from(TestObj(9, None, 3))
        self.assertEqual(o1, TestObj(9, 9, 9))

        # Don't override compulsory field (A) with None
        o1.steal_fields_from(TestObj(None, None, 3))
        self.assertEqual(o1, TestObj(9, 9, 9))


class DeserializeTests(unittest.TestCase):
    def test_deserialize_dataset(self):
        ls8_parsed_yaml_dict = {'acquisition': {'groundstation': {'code': 'LGN'}},
                                'browse': {'full': {'blue_band': '1',
                                                    'cell_size': 25.0,

                                                    'file_type': 'image/jpg',
                                                    'green_band': '5',
                                                    'path': 'browse.fr.jpg',
                                                    'red_band': '7'},
                                           'medium': {'blue_band': '1',
                                                      'cell_size': 222.6806640625,

                                                      'file_type': 'image/jpg',
                                                      'green_band': '5',
                                                      'path': 'browse.jpg',
                                                      'red_band': '7'}},
                                'creation_dt': datetime.datetime(2014, 11, 12, 15, 8, 35),
                                'extent': {'center_dt': datetime.datetime(2014, 10, 12, 0, 56, 6, 5785),
                                           'coord': {'ll': {'lat': -26.99236, 'lon': 133.96208},
                                                     'lr': {'lat': -26.99055, 'lon': 136.25985},
                                                     'ul': {'lat': -24.98805, 'lon': 133.97954},
                                                     'ur': {'lat': -24.9864, 'lon': 136.23866}}},
                                'format': {'name': 'GEOTIFF'},
                                'usgs': {
                                    'interval_id': 'LC81010782014285LGN00'
                                },
                                'grid_spatial': {'projection': {'datum': 'GDA94',
                                                                'ellipsoid': 'GRS80',
                                                                'geo_ref_points': {
                                                                    'll': {'x': 397012.5, 'y': 7013987.5},
                                                                    'lr': {'x': 625012.5, 'y': 7013987.5},
                                                                    'ul': {'x': 397012.5, 'y': 7235987.5},
                                                                    'ur': {'x': 625012.5, 'y': 7235987.5}
                                                                },
                                                                'map_projection': 'UTM',
                                                                'orientation': 'NORTH_UP',
                                                                'resampling_option': 'CUBIC_CONVOLUTION',
                                                                'zone': -53}},
                                'id': '70c8ff82-d838-11e4-bd17-1040f381a756',
                                'image': {'bands': {'1': {'cell_size': 25.0,

                                                          'label': 'Coastal Aerosol',
                                                          'number': '1',
                                                          'path': 'package/LC81010782014285LGN00_B1.TIF',
                                                          'type': 'reflective'},
                                                    '10': {'cell_size': 25.0,

                                                           'label': 'Thermal Infrared 1',
                                                           'number': '10',
                                                           'path': 'package/LC81010782014285LGN00_B10.TIF',
                                                           'type': 'thermal'},
                                                    '11': {'cell_size': 25.0,

                                                           'label': 'Thermal Infrared 2',
                                                           'number': '11',
                                                           'path': 'package/LC81010782014285LGN00_B11.TIF',
                                                           'type': 'thermal'},
                                                    '2': {'cell_size': 25.0,

                                                          'label': 'Visible Blue',
                                                          'number': '2',
                                                          'path': 'package/LC81010782014285LGN00_B2.TIF',
                                                          'type': 'reflective'},
                                                    '3': {'cell_size': 25.0,

                                                          'label': 'Visible Green',
                                                          'number': '3',
                                                          'path': 'package/LC81010782014285LGN00_B3.TIF',
                                                          'type': 'reflective'},
                                                    '4': {'cell_size': 25.0,

                                                          'label': 'Visible Red',
                                                          'number': '4',
                                                          'path': 'package/LC81010782014285LGN00_B4.TIF',
                                                          'type': 'reflective'},
                                                    '5': {'cell_size': 25.0,

                                                          'label': 'Near Infrared',
                                                          'number': '5',
                                                          'path': 'package/LC81010782014285LGN00_B5.TIF',
                                                          'type': 'reflective'},
                                                    '6': {'cell_size': 25.0,

                                                          'label': 'Short-wave Infrared 1',
                                                          'number': '6',
                                                          'path': 'package/LC81010782014285LGN00_B6.TIF',
                                                          'type': 'reflective'},
                                                    '7': {'cell_size': 25.0,

                                                          'label': 'Short-wave Infrared 2',
                                                          'number': '7',
                                                          'path': 'package/LC81010782014285LGN00_B7.TIF',
                                                          'type': 'reflective'},
                                                    '8': {'cell_size': 12.5,

                                                          'label': 'Panchromatic',
                                                          'number': '8',
                                                          'path': 'package/LC81010782014285LGN00_B8.TIF',
                                                          'type': 'panchromatic'},
                                                    '9': {'cell_size': 25.0,

                                                          'label': 'Cirrus',
                                                          'number': '9',
                                                          'path': 'package/LC81010782014285LGN00_B9.TIF',
                                                          'type': 'atmosphere'},
                                                    'quality': {'cell_size': 25.0,

                                                                'label': 'Quality',
                                                                'number': 'quality',
                                                                'path': 'package/LC81010782014285LGN00_BQA.TIF',
                                                                'type': 'quality'}},
                                          'cloud_cover_percentage': 0.01,
                                          'geometric_rmse_model': 4.61,
                                          'geometric_rmse_model_x': 2.968,
                                          'geometric_rmse_model_y': 3.527,
                                          'ground_control_points_model': 420,
                                          'satellite_ref_point_start': {'x': 101, 'y': 78},
                                          'sun_azimuth': 59.57807899,
                                          'sun_earth_distance': 0.998137,
                                          'sun_elevation': 57.89670734},
                                'instrument': {'name': 'OLI_TIRS'},
                                'lineage': {'algorithm': {'name': 'LPGS',
                                                          'parameters': {},
                                                          'version': '2.3.0'},
                                            'ancillary': {
                                                'bpf_oli': {
                                                    'name': 'LO8BPF20141012002825_20141012011100.01'
                                                },
                                                'bpf_tirs': {
                                                    'name': 'LT8BPF20141012002432_20141012011154.02'
                                                },
                                                'cpf': {
                                                    'name': 'L8CPF20141001_20141231.01'
                                                },
                                                'rlut': {
                                                    'name': 'L8RLUT20130211_20431231v09.h5'
                                                }},
                                            'machine': {'hostname': 'niggle.local',
                                                        'runtime_id': '4a6e0699-d838-11e4-95ef-1040f381a756',
                                                        'type_id': 'jobmanager',
                                                        'uname': 'Darwin niggle.local 14.1.0 Darwin Kernel Version '
                                                                 '14.1.0: Thu Feb 26 19:26:47 PST 2015; '
                                                                 'root:xnu-2782.10.73~1/RELEASE_X86_64 x86_64',
                                                        'version': '2.4.0'}},
                                'platform': {'code': 'LANDSAT_8'},
                                'product_type': 'L1T',
                                'size_bytes': 1642703993
                                }

        ptype.DatasetMetadata.from_dict(ls8_parsed_yaml_dict)


class TestRewritePaths(TestCase):
    def test_rewrite_paths(self):
        class TestObj(ptype.SimpleObject):
            def __init__(self, a, b=None, c=3):
                self.a = a
                self.b = b
                self.c = c

        o = ptype.rebase_paths(
            Path('/tmp/from'),
            Path('/tmp/to'),
            TestObj(Path('/tmp/from/test.txt'))
        )

        assert o == TestObj(Path('/tmp/to/test.txt'))

    def test_rewrite_paths_nested(self):
        class TestObj(ptype.SimpleObject):
            def __init__(self, a, b=None, c=3):
                self.a = a
                self.b = b
                self.c = c

        o = TestObj(
            Path('/tmp/from/test1.txt'),
            b=TestObj(Path('/tmp/from/test2.txt'))
        )
        o2 = ptype.rebase_paths(
            Path('/tmp/from'),
            Path('/tmp/to'),
            o
        )
        assert o is not o2
        self.assert_same(
            o2,
            TestObj(
                Path('/tmp/to/test1.txt'),
                b=TestObj(Path('/tmp/to/test2.txt'))
            )
        )

    def test_rewrite_mixed_paths(self):
        class TestObj(ptype.SimpleObject):
            def __init__(self, a, b=None, c=3):
                self.a = a
                self.b = b
                self.c = c

        o = ptype.rebase_paths(
            Path('/tmp/from'),
            Path('/tmp/to'),
            TestObj(
                Path('/tmp/other'),
                b=TestObj(Path('relative.txt')),
                c=2
            )
        )
        self.assert_same(o, TestObj(
            Path('/tmp/other'),
            b=TestObj(Path('relative.txt')),
            c=2
        ))

    def test_rewrite_within_lists(self):
        class TestObj(ptype.SimpleObject):
            def __init__(self, a, b=None, c=3):
                self.a = a
                self.b = b
                self.c = c

        o = [TestObj(
            Path('/tmp/from/test1.txt'),
            b={'a': TestObj(Path('/tmp/from/test2.txt'))}
        )]
        o2 = ptype.rebase_paths(
            Path('/tmp/from'),
            Path('/tmp/to'),
            o
        )
        assert o is not o2
        self.assert_same(o2, [TestObj(
            Path('/tmp/to/test1.txt'),
            b={'a': TestObj(Path('/tmp/to/test2.txt'))}
        )])

    def test_map_vales(self):
        class TestObj(ptype.SimpleObject):
            def __init__(self, a, b=None, c=3):
                self.a = a
                self.b = b
                self.c = c

        o = [TestObj(a=(1,), b={'a': TestObj(3)})]
        o2 = ptype.map_values(lambda a: a + 1, o, skip_nones=True)
        # Should return a new object
        assert o is not o2
        # Paths translated.
        self.assert_same(o2, [TestObj(a=(2,), b={'a': TestObj(a=4, c=4)}, c=4)])

    def test_unchanged(self):
        nbar = _build_ls8_nbar()
        new_nbar = ptype.rebase_paths(Path('/not-exist'), Path('/not-exist2'), nbar)

        # Should return a new object
        assert nbar is not new_nbar
        self.assert_same(nbar, new_nbar)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    _serialise_to_file('nbar', _build_ls8_nbar())
    _serialise_to_file('wofs', _build_ls7_wofs())

    import doctest

    doctest.testmod(ptype)
    unittest.main()
