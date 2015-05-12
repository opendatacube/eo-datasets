# coding=utf-8
from __future__ import absolute_import
import datetime
from uuid import UUID

from pathlib import Path

from eodatasets import drivers
from tests.metadata.mtl import test_ls8, test_ls7_definitive, test_ls5_definitive
from tests import write_files, TestCase
import eodatasets.type as ptype


_LS5_RAW = ptype.DatasetMetadata(
    id_=UUID('c86809b3-e894-11e4-8958-1040f381a756'),
    usgs_dataset_id='L5TB2005152015110ASA111',
    ga_level='P00',
    product_type='raw',
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
    usgs_dataset_id='L7ET2005007020028ASA123',
    ga_level='P00',
    product_type='raw',
    creation_dt=datetime.datetime(2015, 4, 15, 1, 42, 47),
    size_bytes=7698644992,
    checksum_path=Path('package.sha1'),
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
        source_datasets={'ortho': test_ls8.EXPECTED_OUT}
    )
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
        source_datasets={'nbar_brdf': _EXPECTED_NBAR}
    )
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
            'LS8_OLITIRS_STD-MDF_P00_LC81160740842015089ASA00_116_074-084_20150330T022553Z20150330T022657',
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
            "LS8_OLITIRS_TNBAR_P54_GALPGS01-032_101_078_20141012",
            drivers.NbarDriver('terrain').get_ga_label(_EXPECTED_NBAR)
        )

    def test_nbar_brdf_label(self):
        self.assertEqual(
            "LS8_OLITIRS_NBAR_P54_GALPGS01-032_101_078_20141012",
            drivers.NbarDriver('brdf').get_ga_label(_EXPECTED_NBAR)
        )

    def test_pqa_fill(self):
        input_folder = write_files({
            'pqa.tif': ''
        })

        dataset = ptype.DatasetMetadata(
            id_=_EXPECTED_PQA.id_,
            lineage=ptype.LineageMetadata(
                source_datasets={
                    'nbar_brdf': _EXPECTED_NBAR
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