# coding=utf-8
from __future__ import absolute_import
import unittest

import eodatasets.metadata.adsfolder as adsfolder
from tests import write_files
import eodatasets.type as ptype


class AdsFolderExtractionTest(unittest.TestCase):
    def test_directory(self):
        d = write_files({
            'LANDSAT-8.11308': {
                'LC80880750762013254ASA00': {
                    '446.000.2013254233714881.ASA': 'a',
                    '447.000.2013254233711482.ASA': 'a',
                    'LC80880750762013254ASA00_IDF.xml': 'a',
                    'LC80880750762013254ASA00_MD5.txt': 'a',
                }
            }
        })

        # Read orbit from folder name
        d = d.joinpath('LANDSAT-8.11308')
        ds = ptype.DatasetMetadata()
        metadata = adsfolder.extract_md(ds, d)
        self.assertEqual(metadata.acquisition.platform_orbit, 11308)

        # Read orbit from parent folder name.
        d = d.joinpath('LC80880750762013254ASA00')
        ds = ptype.DatasetMetadata()
        metadata = adsfolder.extract_md(ds, d)
        self.assertEqual(metadata.acquisition.platform_orbit, 11308)

    def test_extract_groundstation(self):
        d = write_files({
            'LANDSAT-7.ALICE': {
                'dataset': {}
            }
        })

        d = d.joinpath('LANDSAT-7.ALICE')
        md = adsfolder.extract_md(ptype.DatasetMetadata(), d)
        self.assertIsNotNone(md.acquisition)
        self.assertEqual(md.acquisition.groundstation, ptype.GroundstationMetadata(code='ASA'))

        d = d.joinpath('dataset')
        md = adsfolder.extract_md(ptype.DatasetMetadata(), d)
        self.assertIsNotNone(md.acquisition)
        self.assertEqual(md.acquisition.groundstation, ptype.GroundstationMetadata(code='ASA'))

if __name__ == '__main__':
    import doctest

    doctest.testmod(adsfolder)
    unittest.main()
