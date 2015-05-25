# coding=utf-8
from __future__ import absolute_import
import datetime

from eodatasets import type as ptype
from eodatasets.metadata import rccfile
from tests import write_files, TestCase


class TestRccExtract(TestCase):
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
        self._check_rcc_parse(d)

    def test_parse_rcc_with_subdir(self):
        d = write_files({
            'RCCDATA': {
                'L7EB2013259012832ASN213I00.data': 'nothing',
                'L7EB2013259012832ASN213Q00.data': 'nothing'
            }
        })
        self._check_rcc_parse(d)

    def _check_rcc_parse(self, d):
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

    def test_parse_variations(self):
        new_examples = {
            'L7EB2007303000923ASA222Q.data':
                ptype.DatasetMetadata(
                    ga_level='P00',
                    usgs_dataset_id='L7EB2007303000923ASA222',
                    platform=ptype.PlatformMetadata(code='LANDSAT_7'),
                    instrument=ptype.InstrumentMetadata(name='ETM', operation_mode='BUMPER'),
                    format_=ptype.FormatMetadata(name='RCC'),
                    acquisition=ptype.AcquisitionMetadata(
                        aos=datetime.datetime(2007, 10, 30, 0, 9, 23),
                        groundstation=ptype.GroundstationMetadata(code='ASA')
                    )
                ),
            'L7EB2015118010116ASA213Q00.data':
                ptype.DatasetMetadata(
                    ga_level='P00',
                    usgs_dataset_id='L7EB2015118010116ASA213',
                    platform=ptype.PlatformMetadata(code='LANDSAT_7'),
                    instrument=ptype.InstrumentMetadata(name='ETM', operation_mode='BUMPER'),
                    format_=ptype.FormatMetadata(name='RCC', version=0),
                    acquisition=ptype.AcquisitionMetadata(
                        aos=datetime.datetime(2015, 4, 28, 1, 1, 16),
                        groundstation=ptype.GroundstationMetadata(code='ASA')
                    )
                ),
            'L7EB2011239021036ASA111Q.data':
                ptype.DatasetMetadata(
                    ga_level='P00',
                    usgs_dataset_id='L7EB2011239021036ASA111',
                    platform=ptype.PlatformMetadata(code='LANDSAT_7'),
                    instrument=ptype.InstrumentMetadata(name='ETM', operation_mode='BUMPER'),
                    format_=ptype.FormatMetadata(name='RCC'),
                    acquisition=ptype.AcquisitionMetadata(
                        aos=datetime.datetime(2011, 8, 27, 2, 10, 36),
                        groundstation=ptype.GroundstationMetadata(code='ASA')
                    )
                ),
            'L5TB2005120001242ASA111I.data':
                ptype.DatasetMetadata(
                    ga_level='P00',
                    usgs_dataset_id='L5TB2005120001242ASA111',
                    platform=ptype.PlatformMetadata(code='LANDSAT_5'),
                    instrument=ptype.InstrumentMetadata(name='TM', operation_mode='BUMPER'),
                    format_=ptype.FormatMetadata(name='RCC'),
                    acquisition=ptype.AcquisitionMetadata(
                        aos=datetime.datetime(2005, 4, 30, 0, 12, 42),
                        groundstation=ptype.GroundstationMetadata(code='ASA')
                    )
                ),
            'L5TT1995117002206ASA111I00.data':
                ptype.DatasetMetadata(
                    ga_level='P00',
                    usgs_dataset_id='L5TT1995117002206ASA111',
                    platform=ptype.PlatformMetadata(code='LANDSAT_5'),
                    instrument=ptype.InstrumentMetadata(name='TM', operation_mode='SAM'),
                    format_=ptype.FormatMetadata(name='RCC', version=0),
                    acquisition=ptype.AcquisitionMetadata(
                        aos=datetime.datetime(1995, 4, 27, 0, 22, 6),
                        groundstation=ptype.GroundstationMetadata(code='ASA')
                    )
                ),
            'L5TT1990118013106ASA111I00.data':
                ptype.DatasetMetadata(
                    ga_level='P00',
                    usgs_dataset_id='L5TT1990118013106ASA111',
                    platform=ptype.PlatformMetadata(code='LANDSAT_5'),
                    instrument=ptype.InstrumentMetadata(name='TM', operation_mode='SAM'),
                    format_=ptype.FormatMetadata(name='RCC', version=0),
                    acquisition=ptype.AcquisitionMetadata(
                        aos=datetime.datetime(1990, 4, 28, 1, 31, 6),
                        groundstation=ptype.GroundstationMetadata(code='ASA')
                    )
                ),
            'L7ET2005302020634ASA123Q.data':
                ptype.DatasetMetadata(
                    ga_level='P00',
                    usgs_dataset_id='L7ET2005302020634ASA123',
                    platform=ptype.PlatformMetadata(code='LANDSAT_7'),
                    instrument=ptype.InstrumentMetadata(name='ETM', operation_mode='SAM'),
                    format_=ptype.FormatMetadata(name='RCC'),
                    acquisition=ptype.AcquisitionMetadata(
                        aos=datetime.datetime(2005, 10, 29, 2, 6, 34),
                        groundstation=ptype.GroundstationMetadata(code='ASA')
                    )
                ),
            'L5TB2011299000126ASA111I00.data':
                ptype.DatasetMetadata(
                    ga_level='P00',
                    usgs_dataset_id='L5TB2011299000126ASA111',
                    platform=ptype.PlatformMetadata(code='LANDSAT_5'),
                    instrument=ptype.InstrumentMetadata(name='TM', operation_mode='BUMPER'),
                    format_=ptype.FormatMetadata(name='RCC', version=0),
                    acquisition=ptype.AcquisitionMetadata(
                        aos=datetime.datetime(2011, 10, 26, 0, 1, 26),
                        groundstation=ptype.GroundstationMetadata(code='ASA')
                    )
                ),
            'L5TB2010119010045ASA214I.data':
                ptype.DatasetMetadata(
                    ga_level='P00',
                    usgs_dataset_id='L5TB2010119010045ASA214',
                    platform=ptype.PlatformMetadata(code='LANDSAT_5'),
                    instrument=ptype.InstrumentMetadata(name='TM', operation_mode='BUMPER'),
                    format_=ptype.FormatMetadata(name='RCC'),
                    acquisition=ptype.AcquisitionMetadata(
                        aos=datetime.datetime(2010, 4, 29, 1, 0, 45),
                        groundstation=ptype.GroundstationMetadata(code='ASA')
                    )
                ),
            'L7ET2000296234136ASA111Q.data':
                ptype.DatasetMetadata(
                    ga_level='P00',
                    usgs_dataset_id='L7ET2000296234136ASA111',
                    platform=ptype.PlatformMetadata(code='LANDSAT_7'),
                    instrument=ptype.InstrumentMetadata(name='ETM', operation_mode='SAM'),
                    format_=ptype.FormatMetadata(name='RCC'),
                    acquisition=ptype.AcquisitionMetadata(
                        aos=datetime.datetime(2000, 10, 22, 23, 41, 36),
                        groundstation=ptype.GroundstationMetadata(code='ASA')
                    )
                ),
        }

        for file_name, expected_output in new_examples.items():
            output = rccfile._extract_rcc_filename_fields(ptype.DatasetMetadata(), file_name)

            # The ids will be different — clear them before comparison.
            output.id_ = None
            expected_output.id_ = None

            self.assert_same(expected_output, output)
