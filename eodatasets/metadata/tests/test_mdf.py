
import unittest

import datetime
import pathlib

import eodatasets.metadata.mdf as mdf
from eodatasets.tests import write_files
import eodatasets.type as ptype


class MdfTests(unittest.TestCase):
    def test_directory_parse(self):
        dataset_id = 'LC80880750762013254ASA00'
        metadata = mdf._extract_mdf_directory_fields(ptype.DatasetMetadata(), pathlib.Path(dataset_id))

        self.assertEquals(metadata.usgs_dataset_id, dataset_id)
        self.assertEquals(metadata.platform.code, 'LANDSAT_8')
        self.assertEquals(metadata.instrument.name, 'OLI_TIRS')
        self.assertEquals(metadata.image.satellite_ref_point_start, ptype.Point(88, 75))
        self.assertEquals(metadata.image.satellite_ref_point_end, ptype.Point(88, 76))

        self.assertEquals(metadata.acquisition.groundstation.code, 'ASA')
        # No longer bother with vague center date.
        # self.assertEqual(metadata.extent.center_dt, datetime.date(2013, 9, 11))

    def test_directory(self):
        d = write_files({
            'LC80880750762013254ASA00': {
                '446.000.2013254233714881.ASA': 'a',
                '447.000.2013254233711482.ASA': 'a',
                'LC80880750762013254ASA00_IDF.xml': 'a',
                'LC80880750762013254ASA00_MD5.txt': 'a',
                }
        })

        def _test_mdf_output(metadata):
            self.assertEquals(metadata.usgs_dataset_id, 'LC80880750762013254ASA00')
            self.assertEquals(metadata.platform.code, 'LANDSAT_8')
            self.assertEquals(metadata.instrument.name, 'OLI_TIRS')
            self.assertEqual(metadata.format_.name, 'MD')
            self.assertEqual(metadata.ga_level, 'P00')
            self.assertEquals(metadata.image.satellite_ref_point_start, ptype.Point(88, 75))
            self.assertEquals(metadata.image.satellite_ref_point_end, ptype.Point(88, 76))
            self.assertEquals(metadata.acquisition.groundstation.code, 'ASA')
            # No longer bother with vague center date.
            # self.assertEqual(metadata.extent.center_dt, datetime.date(2013, 9, 11))
            self.assertEqual(metadata.acquisition.aos, datetime.datetime(2013, 9, 11, 23, 36, 11))
            self.assertEqual(metadata.acquisition.los, datetime.datetime(2013, 9, 11, 23, 37, 14))

        metadata = mdf.extract_md(ptype.DatasetMetadata(), d)
        _test_mdf_output(metadata)

        # It should also work when given the specific MDF folder.
        metadata = mdf.extract_md(ptype.DatasetMetadata(), d.joinpath('LC80880750762013254ASA00'))
        _test_mdf_output(metadata)


if __name__ == '__main__':
    import doctest

    doctest.testmod(mdf)
    unittest.main()