# coding=utf-8
from __future__ import absolute_import
import datetime
import unittest
from uuid import UUID

from pathlib import Path

from eodatasets.drivers import RawDriver, OrthoDriver
from eodatasets.metadata.tests.mtl import test_ls8, test_ls7_definitive, test_ls5_definitive
from eodatasets.tests import write_files
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


class TestDrivers(unittest.TestCase):
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
        raw_driver = RawDriver()
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
            RawDriver().get_ga_label(_LS5_RAW)
        )

    def test_raw_ls7_label(self):
        self.assertEqual(
            'LS7_ETM_STD-RCC_P00_L7ET2005007020028ASA123_0_0_20050107T020028Z20050107T020719',
            RawDriver().get_ga_label(_LS7_RAW)
        )

    def test_ortho_ls8_label(self):
        self.assertEqual(
            "LS8_OLITIRS_OTH_P51_GALPGS01-032_101_078_20141012",
            OrthoDriver().get_ga_label(test_ls8.EXPECTED_OUT)
        )

    def test_ortho_ls7_label(self):
        self.assertEqual(
            "LS7_ETM_SYS_P31_GALPGS01-002_114_073_20050107",
            OrthoDriver().get_ga_label(test_ls7_definitive.EXPECTED_OUT)
        )

    def test_ortho_ls5_label(self):
        self.assertEqual(
            "LS5_TM_OTH_P51_GALPGS01-002_113_063_20050601",
            OrthoDriver().get_ga_label(test_ls5_definitive.EXPECTED_OUT)
        )
