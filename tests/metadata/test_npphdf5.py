# coding=utf-8
from __future__ import absolute_import

import datetime
import unittest

from eodatasets import type as ptype, drivers
from eodatasets.metadata import npphdf5 as extraction
from tests import write_files, TestCase


class TestNPPMetadataExtraction(TestCase):
    def testExtractHDF5FilenameFields(self):
        metadata = extraction._extract_hdf5_filename_fields(
            ptype.DatasetMetadata(),
            'RNSCA-RVIRS_npp_d20130422_t0357358_e0410333_b07686_c20130422041225898000_nfts_drl.h5'
        )
        self.assertEqual(metadata.platform.code, 'NPP')
        self.assertEqual(metadata.instrument.name, 'VIIRS')

        self.assertEqual(metadata.acquisition.platform_orbit, 7686)

        self.assertEqual(metadata.acquisition.aos,
                         datetime.datetime(2013, 4, 22, 3, 57, 35))
        self.assertEqual(metadata.acquisition.los,
                         datetime.datetime(2013, 4, 22, 4, 12, 25))

    def testParseHdf5Filenames(self):
        d = write_files({
            'RNSCA-RVIRS_npp_d20130422_t0357358_e0410333_b07686'
            '_c20130422041225898000_nfts_drl.h5': ''
        })
        metadata = extraction.extract_md(ptype.DatasetMetadata(), d)

        self.assertEqual(metadata.platform.code, 'NPP')
        self.assertEqual(metadata.instrument.name, 'VIIRS')
        self.assertEqual(metadata.ga_level, 'P00')
        self.assertEqual(metadata.product_level, None)
        self.assertEqual(metadata.format_.name, 'HDF5')

        self.assertEqual(metadata.acquisition.aos,
                         datetime.datetime(2013, 4, 22, 3, 57, 35))
        self.assertEqual(metadata.acquisition.los,
                         datetime.datetime(2013, 4, 22, 4, 12, 25))

        self.assertEqual(metadata.acquisition.platform_orbit, 7686)

    def test_parse_from_driver(self):
        d = write_files({
            'NPP.VIIRS.11361.ALICE': {
                'RNSCA-RVIRS_npp_d20140106_t0444094_e0451182_'
                'b11361_c20140106045941812000_nfts_drl.h5': ''
            }
        })

        metadata = drivers.RawDriver().fill_metadata(
            ptype.DatasetMetadata(),
            d.joinpath('NPP.VIIRS.11361.ALICE')
        )

        self.assertEqual(metadata.platform.code, 'NPP')
        self.assertEqual(metadata.instrument.name, 'VIIRS')
        self.assertEqual(metadata.ga_level, 'P00')
        self.assertEqual(metadata.format_.name, 'HDF5')

        # Groundstation should be found from surrounding adsfolder.
        self.assertEqual(
            metadata.acquisition.groundstation,
            ptype.GroundstationMetadata(code='ASA')
        )

        self.assertEqual(metadata.acquisition.aos,
                         datetime.datetime(2014, 1, 6, 4, 44, 9))
        self.assertEqual(metadata.acquisition.los,
                         datetime.datetime(2014, 1, 6, 4, 59, 41))

        self.assertEqual(metadata.acquisition.platform_orbit, 11361)


if __name__ == '__main__':
    unittest.main()
