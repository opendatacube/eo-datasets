# coding=utf-8
from __future__ import absolute_import

import datetime
from textwrap import dedent
from uuid import UUID

from pathlib import Path

import eodatasets.type as ptype
from eodatasets import drivers
from tests import write_files, TestCase
from tests.metadata.mtl import test_ls8, test_ls7_definitive, test_ls5_definitive

_LS5_RAW = ptype.DatasetMetadata(
    id_=UUID('c86809b3-e894-11e4-8958-1040f381a756'),
    ga_level='P00',
    product_type='satellite_telemetry_data',
    creation_dt=datetime.datetime(2015, 4, 22, 0, 7, 48),
    size_bytes=5871413760,
    checksum_path=Path('package.sha1'),
    platform=ptype.PlatformMetadata(code='LANDSAT_5'),
    instrument=ptype.InstrumentMetadata(name='TM', operation_mode='BUMPER'),
    format_=ptype.FormatMetadata(name='RCC'),
    acquisition=ptype.AcquisitionMetadata(
        aos=datetime.datetime(2005, 6, 1, 1, 51, 10),
        los=datetime.datetime(2005, 6, 1, 2, 0, 25),
        groundstation=ptype.GroundstationMetadata(code='ASA'),
        platform_orbit=113025
    ),
    usgs=ptype.UsgsMetadata(
        interval_id='L5TB2005152015110ASA111'
    ),
    lineage=ptype.LineageMetadata(
        machine=ptype.MachineMetadata(
            hostname='niggle.local',
            runtime_id=UUID('b2af5545-e894-11e4-b3b0-1040f381a756'),
            type_id='jobmanager',
            version='2.4.0',
            uname='Darwin niggle.local 14.3.0 Darwin Kernel Version 14.3.0: Mon Mar 23 11:59:05 PDT 2015; '
                  'root:xnu-2782.20.48~5/RELEASE_X86_64 x86_64'
        ),
        source_datasets={}
    )
)

_LS7_RAW = ptype.DatasetMetadata(
    id_=UUID('c50c6bd4-e895-11e4-9814-1040f381a756'),
    ga_level='P00',
    product_type='satellite_telemetry_data',
    creation_dt=datetime.datetime(2015, 4, 15, 1, 42, 47),
    size_bytes=7698644992,
    checksum_path=Path('package.sha1'),
    usgs=ptype.UsgsMetadata(
        interval_id='L7ET2005007020028ASA123'
    ),
    platform=ptype.PlatformMetadata(code='LANDSAT_7'),
    instrument=ptype.InstrumentMetadata(name='ETM', operation_mode='SAM'),
    format_=ptype.FormatMetadata(name='RCC'),
    acquisition=ptype.AcquisitionMetadata(
        aos=datetime.datetime(2005, 1, 7, 2, 0, 28),
        los=datetime.datetime(2005, 1, 7, 2, 7, 19),
        groundstation=ptype.GroundstationMetadata(code='ASA'),
        platform_orbit=30486
    ),
    lineage=ptype.LineageMetadata(
        machine=ptype.MachineMetadata(
            hostname='niggle.local',
            runtime_id=UUID('a86f8a4c-e895-11e4-83e1-1040f381a756'),
            type_id='jobmanager',
            version='2.4.0',
            uname='Darwin niggle.local 14.3.0 Darwin Kernel Version 14.3.0: Mon Mar 23 '
                  '11:59:05 PDT 2015; root:xnu-2782.20.48~5/RELEASE_X86_64 x86_64'
        ),
        source_datasets={}
    )
)

_EXPECTED_NBAR = ptype.DatasetMetadata(
    id_=UUID('c50c6bd4-e895-11e4-9814-1040f381a756'),
    ga_level='P54',
    platform=ptype.PlatformMetadata(code='LANDSAT_8'),
    instrument=ptype.InstrumentMetadata(name='OLI_TIRS'),
    format_=ptype.FormatMetadata(name='GeoTIFF'),
    acquisition=ptype.AcquisitionMetadata(groundstation=ptype.GroundstationMetadata(code='LGN')),
    extent=ptype.ExtentMetadata(
        coord=ptype.CoordPolygon(
            ul=ptype.Coord(lat=-24.98805, lon=133.97954),
            ur=ptype.Coord(lat=-24.9864, lon=136.23866),
            ll=ptype.Coord(lat=-26.99236, lon=133.96208),
            lr=ptype.Coord(lat=-26.99055, lon=136.25985)
        ),
        center_dt=datetime.datetime(2014, 10, 12, 0, 56, 6, 5785)
    ),
    image=ptype.ImageMetadata(
        satellite_ref_point_start=ptype.Point(x=101, y=78),
        bands={}
    ),
    lineage=ptype.LineageMetadata(
        source_datasets={'ortho': test_ls8.EXPECTED_OUT},
        algorithm=ptype.AlgorithmMetadata(name='terrain', version='1.0'),
        machine=ptype.MachineMetadata(software_versions={'nbar': '1.0'}),
    ),
    grid_spatial=ptype.GridSpatialMetadata(
        projection=ptype.ProjectionMetadata(
            geo_ref_points=ptype.PointPolygon(
                ul=ptype.Point(
                    x=397012.5,
                    y=7235987.5
                ),
                ur=ptype.Point(
                    x=625012.5,
                    y=7235987.5
                ),
                ll=ptype.Point(
                    x=397012.5,
                    y=7013987.5
                ),
                lr=ptype.Point(
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
)

_EXPECTED_PQA = ptype.DatasetMetadata(
    id_=UUID('c50c6bd4-e895-11e4-9814-1040f381a756'),
    ga_level='P55',
    platform=ptype.PlatformMetadata(code='LANDSAT_8'),
    instrument=ptype.InstrumentMetadata(name='OLI_TIRS'),
    format_=ptype.FormatMetadata(name='GeoTIFF'),
    acquisition=ptype.AcquisitionMetadata(groundstation=ptype.GroundstationMetadata(code='LGN')),
    extent=ptype.ExtentMetadata(
        coord=ptype.CoordPolygon(
            ul=ptype.Coord(lat=-24.98805, lon=133.97954),
            ur=ptype.Coord(lat=-24.9864, lon=136.23866),
            ll=ptype.Coord(lat=-26.99236, lon=133.96208),
            lr=ptype.Coord(lat=-26.99055, lon=136.25985)
        ),
        center_dt=datetime.datetime(2014, 10, 12, 0, 56, 6, 5785)
    ),
    image=ptype.ImageMetadata(
        satellite_ref_point_start=ptype.Point(x=101, y=78),
        bands={}
    ),
    lineage=ptype.LineageMetadata(
        algorithm=ptype.AlgorithmMetadata(name='pqa', version='1.0'),
        source_datasets={'nbar': _EXPECTED_NBAR}
    ),
    grid_spatial=ptype.GridSpatialMetadata(
        projection=ptype.ProjectionMetadata(
            geo_ref_points=ptype.PointPolygon(
                ul=ptype.Point(
                    x=397012.5,
                    y=7235987.5
                ),
                ur=ptype.Point(
                    x=625012.5,
                    y=7235987.5
                ),
                ll=ptype.Point(
                    x=397012.5,
                    y=7013987.5
                ),
                lr=ptype.Point(
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
)


class TestDrivers(TestCase):
    def _get_raw_ls8(self):
        d = write_files({
            'LANDSAT-8.11308': {
                'LC81160740842015089ASA00': {
                    '480.000.2015089022657325.ASA': '',
                    '481.000.2015089022653346.ASA': '',
                    'LC81160740742015089ASA00_IDF.xml': '',
                    'LC81160740742015089ASA00_MD5.txt': '',
                    'file.list': '',
                }
            }
        })
        raw_driver = drivers.RawDriver()
        metadata = raw_driver.fill_metadata(
            ptype.DatasetMetadata(),
            d.joinpath('LANDSAT-8.11308', 'LC81160740842015089ASA00')
        )
        return metadata, raw_driver

    def test_raw_ls8_time_calc(self):
        metadata, raw_driver = self._get_raw_ls8()

        self.assertEqual(metadata.platform.code, 'LANDSAT_8')
        self.assertEqual(metadata.instrument.name, 'OLI_TIRS')

        # TODO: Can we extract the operation mode?
        self.assertEqual(metadata.instrument.operation_mode, None)

        self.assertEqual(metadata.acquisition.platform_orbit, 11308)
        self.assertEqual(metadata.acquisition.groundstation.code, 'ASA')

        # Note that the files are not in expected order: when ordered by their first number (storage location), the
        # higher number is actually an earlier date.
        self.assertEqual(metadata.acquisition.aos, datetime.datetime(2015, 3, 30, 2, 25, 53, 346000))
        self.assertEqual(metadata.acquisition.los, datetime.datetime(2015, 3, 30, 2, 26, 57, 325000))

    def test_raw_ls8_label(self):
        metadata, raw_driver = self._get_raw_ls8()
        self.assertEqual(
            'LS8_OLITIRS_STD-MD_P00_LC81160740842015089ASA00_116_074-084_20150330T022553Z20150330T022657',
            raw_driver.get_ga_label(metadata),
        )

    def test_raw_ls5_label(self):
        self.assertEqual(
            'LS5_TM_STD-RCC_P00_L5TB2005152015110ASA111_0_0_20050601T015110Z20050601T020025',
            drivers.RawDriver().get_ga_label(_LS5_RAW)
        )

    def test_raw_ls7_label(self):
        self.assertEqual(
            'LS7_ETM_STD-RCC_P00_L7ET2005007020028ASA123_0_0_20050107T020028Z20050107T020719',
            drivers.RawDriver().get_ga_label(_LS7_RAW)
        )

    def test_ortho_ls8_label(self):
        self.assertEqual(
            "LS8_OLITIRS_OTH_P51_GALPGS01-032_101_078_20141012",
            drivers.OrthoDriver().get_ga_label(test_ls8.EXPECTED_OUT)
        )

    def test_ortho_ls7_label(self):
        self.assertEqual(
            "LS7_ETM_SYS_P31_GALPGS01-002_114_073_20050107",
            drivers.OrthoDriver().get_ga_label(test_ls7_definitive.EXPECTED_OUT)
        )

    def test_ortho_ls5_label(self):
        self.assertEqual(
            "LS5_TM_OTH_P51_GALPGS01-002_113_063_20050601",
            drivers.OrthoDriver().get_ga_label(test_ls5_definitive.EXPECTED_OUT)
        )

    def test_aqua_pds_label(self):
        ds = ptype.DatasetMetadata(
            id_=UUID('d083fa45-1edd-11e5-8f9e-1040f381a756'),
            product_type='satellite_telemetry_data',
            creation_dt=datetime.datetime(2015, 6, 11, 5, 51, 50),
            platform=ptype.PlatformMetadata(code='AQUA'),
            instrument=ptype.InstrumentMetadata(name='MODIS'),
            format_=ptype.FormatMetadata(name='PDS'),
            rms_string='S1A1C1D1R1',
            acquisition=ptype.AcquisitionMetadata(
                aos=datetime.datetime(2014, 8, 7, 3, 16, 28, 750910),
                los=datetime.datetime(2014, 8, 7, 3, 16, 30, 228023),
                platform_orbit=65208
            ),
            image=ptype.ImageMetadata(day_percentage_estimate=100.0),
            lineage=ptype.LineageMetadata(
                machine=ptype.MachineMetadata(),
                source_datasets={}
            )
        )

        self.assertEqual(
            "AQUA_MODIS_STD-PDS_P00_65208.S1A1C1D1R1_0_0_20140807T031628Z20140807T031630",
            drivers.RawDriver().get_ga_label(ds)
        )

    def test_eods_fill_metadata(self):
        dataset_folder = "LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012"
        bandname = '10'
        bandfile = dataset_folder + '_B' + bandname + '.tif'
        input_folder = write_files({
            dataset_folder: {
                'metadata.xml': """<EODS_DATASET>
                <ACQUISITIONINFORMATION>
                <EVENT>
                <AOS>20141012T03:23:36</AOS>
                <LOS>20141012T03:29:10</LOS>
                </EVENT>
                </ACQUISITIONINFORMATION>
                <EXEXTENT>
                <TEMPORALEXTENTFROM>20141012 00:55:54</TEMPORALEXTENTFROM>
                <TEMPORALEXTENTTO>20141012 00:56:18</TEMPORALEXTENTTO>
                </EXEXTENT>
                </EODS_DATASET>""",
                'scene01': {
                    bandfile: ''
                }
            }
        })
        expected = ptype.DatasetMetadata(
            id_=_EXPECTED_NBAR.id_,
            ga_label=dataset_folder,
            ga_level='P54',
            product_type='EODS_NBAR',
            platform=ptype.PlatformMetadata(code='LANDSAT_8'),
            instrument=ptype.InstrumentMetadata(name='OLI_TIRS'),
            format_=ptype.FormatMetadata(name='GeoTiff'),
            acquisition=ptype.AcquisitionMetadata(aos=datetime.datetime(2014, 10, 12, 3, 23, 36),
                                                  los=datetime.datetime(2014, 10, 12, 3, 29, 10),
                                                  groundstation=ptype.GroundstationMetadata(code='LGS')),
            extent=ptype.ExtentMetadata(
                center_dt=datetime.datetime(2014, 10, 12, 0, 56, 6),
                from_dt=datetime.datetime(2014, 10, 12, 0, 55, 54),
                to_dt=datetime.datetime(2014, 10, 12, 0, 56, 18)
            ),
            image=ptype.ImageMetadata(satellite_ref_point_start=ptype.Point(x=101, y=78),
                                      satellite_ref_point_end=ptype.Point(x=101, y=78),
                                      bands={bandname: ptype.BandMetadata(number=bandname,
                                                                          path=Path(input_folder, dataset_folder,
                                                                                    'scene01', bandfile))})
        )
        dataset = ptype.DatasetMetadata(
            id_=_EXPECTED_NBAR.id_
        )
        received = drivers.EODSDriver().fill_metadata(dataset, input_folder.joinpath(dataset_folder))
        self.assert_same(expected, received)

    def test_nbar_fill_metadata(self):
        input_folder = write_files({
            'reflectance_brdf_1.bin': '',
            'reflectance_brdf_1.bin.aux.xml': '',
            'reflectance_brdf_1.hdr': '',
            'reflectance_brdf_2.bin': '',
            'reflectance_brdf_2.bin.aux.xml': '',
            'reflectance_brdf_2.hdr': '',
            'reflectance_brdf_3.bin': '',
            'reflectance_brdf_3.bin.aux.xml': '',
            'reflectance_brdf_3.hdr': '',
            'reflectance_brdf_4.bin': '',
            'reflectance_brdf_4.bin.aux.xml': '',
            'reflectance_brdf_4.hdr': '',
            'reflectance_brdf_5.bin': '',
            'reflectance_brdf_5.bin.aux.xml': '',
            'reflectance_brdf_5.hdr': '',
            'reflectance_brdf_6.bin': '',
            'reflectance_brdf_6.bin.aux.xml': '',
            'reflectance_brdf_6.hdr': '',
            'reflectance_brdf_7.bin': '',
            'reflectance_brdf_7.bin.aux.xml': '',
            'reflectance_brdf_7.hdr': '',
            'reflectance_terrain_1.bin': '',
            'reflectance_terrain_1.bin.aux.xml': '',
            'reflectance_terrain_1.hdr': '',
            'reflectance_terrain_2.bin': '',
            'reflectance_terrain_2.bin.aux.xml': '',
            'reflectance_terrain_2.hdr': '',
            'reflectance_terrain_3.bin': '',
            'reflectance_terrain_3.bin.aux.xml': '',
            'reflectance_terrain_3.hdr': '',
            'reflectance_terrain_4.bin': '',
            'reflectance_terrain_4.bin.aux.xml': '',
            'reflectance_terrain_4.hdr': '',
            'reflectance_terrain_5.bin': '',
            'reflectance_terrain_5.bin.aux.xml': '',
            'reflectance_terrain_5.hdr': '',
            'reflectance_terrain_6.bin': '',
            'reflectance_terrain_6.bin.aux.xml': '',
            'reflectance_terrain_6.hdr': '',
            'reflectance_terrain_7.bin': '',
            'reflectance_terrain_7.bin.aux.xml': '',
            'reflectance_terrain_7.hdr': '',
            'nbar-metadata.yml': dedent(
                """
                    algorithm_information:
                        software_version: 1.0
                        algorithm_version: 1.0
                        arg25_doi:
                        nbar_doi:
                        nbar_terrain_corrected_doi:
                    ancillary_data: {}
                """),
        })
        dataset = ptype.DatasetMetadata(
            id_=_EXPECTED_NBAR.id_,
            lineage=ptype.LineageMetadata(
                source_datasets={
                    'ortho': test_ls8.EXPECTED_OUT
                }
            )
        )
        received_dataset = drivers.NbarDriver('terrain').fill_metadata(dataset, input_folder)

        self.assert_same(_EXPECTED_NBAR, received_dataset)

    def test_nbar_label(self):
        self.assertEqual(
            "LS8_OLITIRS_NBART_P54_GANBART01-032_101_078_20141012",
            drivers.NbarDriver('terrain').get_ga_label(_EXPECTED_NBAR)
        )

    def test_nbar_brdf_label(self):
        self.assertEqual(
            "LS8_OLITIRS_NBAR_P54_GANBAR01-032_101_078_20141012",
            drivers.NbarDriver('brdf').get_ga_label(_EXPECTED_NBAR)
        )

    def test_pqa_fill(self):
        input_folder = write_files({
            'pqa.tif': '',
            'pq_metadata.yml': dedent(
                """
                    algorithm_information:
                        software_version: 1.0
                        pq_doi:
                    ancillary: {}
                """)
        })

        dataset = ptype.DatasetMetadata(
            id_=_EXPECTED_PQA.id_,
            lineage=ptype.LineageMetadata(
                source_datasets={
                    'nbar': _EXPECTED_NBAR
                }
            )
        )

        received_dataset = drivers.PqaDriver().fill_metadata(dataset, input_folder)

        self.assert_same(_EXPECTED_PQA, received_dataset)

    def test_pqa_label(self):
        self.assertEqual(
            "LS8_OLITIRS_PQ_P55_GAPQ01-032_101_078_20141012",
            drivers.PqaDriver().get_ga_label(_EXPECTED_PQA)
        )

    def test_pqa_translate_path(self):
        input_folder = write_files({
            'pqa.tif': '',
            'process.log': '',
            'passinfo': ''
        })
        self.assertEqual(
            input_folder.joinpath('LS8_OLITIRS_PQ_P55_GAPQ01-032_101_078_20141012.tif'),
            drivers.PqaDriver().translate_path(
                _EXPECTED_PQA,
                input_folder.joinpath('pqa.tif')
            )
        )
        # Other files unchanged.
        self.assertEqual(
            input_folder.joinpath('process.log'),
            drivers.PqaDriver().translate_path(
                _EXPECTED_PQA,
                input_folder.joinpath('process.log')
            )
        )
        self.assertEqual(
            input_folder.joinpath('passinfo'),
            drivers.PqaDriver().translate_path(
                _EXPECTED_PQA,
                input_folder.joinpath('passinfo')
            )
        )

    def test_default_landsat_bands(self):
        # Default bands for each satellite.
        d = drivers.OrthoDriver()
        self.assertEqual(
            ('7', '5', '2'),
            d.browse_image_bands(test_ls8.EXPECTED_OUT)
        )
        self.assertEqual(
            ('7', '4', '1'),
            d.browse_image_bands(test_ls7_definitive.EXPECTED_OUT)
        )
        self.assertEqual(
            ('7', '4', '1'),
            d.browse_image_bands(test_ls5_definitive.EXPECTED_OUT)
        )

    def test_pqa_to_band(self):
        input_folder = write_files({
            'pqa.tif': '',
            'process.log': '',
            'passinfo': '',
        })

        # Creates a single band.
        self.assertEqual(
            ptype.BandMetadata(path=input_folder.joinpath('pqa.tif'), number='pqa'),
            drivers.PqaDriver().to_band(None, input_folder.joinpath('pqa.tif'))
        )

        # Other files should not be bands.
        self.assertIsNone(drivers.PqaDriver().to_band(None, input_folder.joinpath('process.log')))
        self.assertIsNone(drivers.PqaDriver().to_band(None, input_folder.joinpath('passinfo')))

    def test_pqa_defaults(self):
        # A one-band browse image.
        self.assertEqual(drivers.PqaDriver().browse_image_bands(_EXPECTED_PQA), ('pqa',))

        self.assertEqual('pqa', drivers.PqaDriver().get_id())
        self.assertEqual(drivers.NbarDriver('brdf'), drivers.PqaDriver().expected_source())
