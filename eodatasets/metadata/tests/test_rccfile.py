# coding=utf-8
from __future__ import absolute_import
import unittest
import datetime

from eodatasets import type as ptype
from eodatasets.metadata import rccfile
from eodatasets.tests import write_files


class TestRccExtract(unittest.TestCase):
    def test_extract_ls7_rcc_filename_fields(self):
        md = rccfile._extract_rcc_filename_fields(ptype.DatasetMetadata(), 'L7EB2012028010752ASA111I.data')
        self.assertEqual(md.platform.code, 'LANDSAT_7')
        self.assertEqual(md.instrument.name, 'ETM')
        self.assertEqual(md.instrument.operation_mode, 'BUMPER')
        self.assertEqual(md.acquisition.groundstation.code, 'ASA')

        # Is AOS correct here? The RCC spec is not clear exactly when the timestamp is taken.
        self.assertEqual(md.acquisition.aos, datetime.datetime(2012, 1, 28, 1, 7, 52))

    def test_extract_ls5_rcc_filename_fields(self):
        md = rccfile._extract_rcc_filename_fields(ptype.DatasetMetadata(), 'L5TT2003339014237ASA111I00.data')
        self.assertEqual(md.platform.code, 'LANDSAT_5')
        self.assertEqual(md.instrument.name, 'TM')
        self.assertEqual(md.instrument.operation_mode, 'SAM')
        self.assertEqual(md.acquisition.groundstation.code, 'ASA')

        self.assertEqual(md.format_.version, 0)

        # Is AOS correct here? The RCC spec is not clear exactly when the timestamp is taken.
        self.assertEqual(md.acquisition.aos, datetime.datetime(2003, 12, 5, 1, 42, 37))

    def test_parse_rcc_filenames(self):
        d = write_files({
            'L7EB2013259012832ASN213I00.data': 'nothing',
            'L7EB2013259012832ASN213Q00.data': 'nothing'
        })
        md = rccfile.extract_md(ptype.DatasetMetadata(), d)

        self.assertEqual(md.platform.code, 'LANDSAT_7')
        self.assertEqual(md.instrument.name, 'ETM')
        self.assertEqual(md.acquisition.groundstation.code, 'ASN')
        self.assertEqual(md.ga_level, 'P00')
        self.assertEqual(md.format_.name, 'RCC')
        self.assertEqual(md.usgs_dataset_id, 'L7EB2013259012832ASN213')

        self.assertEqual(md.acquisition.aos, datetime.datetime(2013, 9, 16, 1, 28, 32))

        # From the old onreceipt codebase,
        # Default L7 LOS is: AOS + (I.data fileSize) * 8.0 / 75000000.0
        self.assertEqual(md.acquisition.los, datetime.datetime(2013, 9, 16, 1, 28, 32))

    def test_parse_l5_rcc_filenames(self):
        d = write_files({
            'L5TB2003339014237ASA111I00.data': 'nothing'
        })
        md = rccfile.extract_md(ptype.DatasetMetadata(), d)

        self.assertEqual(md.platform.code, 'LANDSAT_5')
        self.assertEqual(md.instrument.name, 'TM')
        self.assertEqual(md.acquisition.groundstation.code, 'ASA')
        self.assertEqual(md.format_.name, 'RCC')
        self.assertEqual(md.usgs_dataset_id, 'L5TB2003339014237ASA111')

        self.assertEqual(md.acquisition.aos, datetime.datetime(2003, 12, 5, 1, 42, 37))

        # From the old onreceipt codebase,
        # Default L5 LOS is: AOS + (I.data fileSize) * 8.0 / 84900000.0
        self.assertEqual(md.acquisition.los, datetime.datetime(2003, 12, 5, 1, 42, 37))
