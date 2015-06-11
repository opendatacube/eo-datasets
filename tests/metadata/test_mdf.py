# coding=utf-8
from __future__ import absolute_import
import unittest
import datetime
import uuid

import eodatasets.metadata.mdf as mdf
from tests import write_files, assert_same
import eodatasets.type as ptype


class MdfTests(unittest.TestCase):
    def test_directory_parse(self):
        dataset_id = 'LC80880750762013254ASA00'
        metadata = mdf._extract_mdf_id_fields(ptype.DatasetMetadata(), dataset_id)

        self.assertEquals(metadata.usgs.interval_id, dataset_id)
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
            self.assertEquals(metadata.usgs.interval_id, 'LC80880750762013254ASA00')
            self.assertEquals(metadata.platform.code, 'LANDSAT_8')
            self.assertEquals(metadata.instrument.name, 'OLI_TIRS')
            self.assertEqual(metadata.format_.name, 'MDF')
            self.assertEqual(metadata.ga_level, 'P00')
            self.assertEquals(metadata.image.satellite_ref_point_start, ptype.Point(88, 75))
            self.assertEquals(metadata.image.satellite_ref_point_end, ptype.Point(88, 76))
            self.assertEquals(metadata.acquisition.groundstation.code, 'ASA')
            # No longer bother with vague center date.
            # self.assertEqual(metadata.extent.center_dt, datetime.date(2013, 9, 11))
            self.assertEqual(metadata.acquisition.aos, datetime.datetime(2013, 9, 11, 23, 36, 11, 482000))
            self.assertEqual(metadata.acquisition.los, datetime.datetime(2013, 9, 11, 23, 37, 14, 881000))

        metadata = mdf.extract_md(ptype.DatasetMetadata(), d)
        _test_mdf_output(metadata)

        # It should also work when given the specific MDF folder.
        metadata = mdf.extract_md(ptype.DatasetMetadata(), d.joinpath('LC80880750762013254ASA00'))
        _test_mdf_output(metadata)

    def test_unchanged_without_id(self):
        # No MDF directory, only files. Don't try to extract information from the files.
        d = write_files({
            '446.000.2013254233714881.ASA': 'a',
            '447.000.2013254233711482.ASA': 'a',
        })

        id_ = uuid.uuid1()
        date = datetime.datetime.utcnow()
        metadata = mdf.extract_md(ptype.DatasetMetadata(id_=id_, creation_dt=date), d)
        # Should be unchanged: No USGS ID found.
        assert_same(metadata, ptype.DatasetMetadata(id_=id_, creation_dt=date))

    def test_files_with_usgs_id(self):
        # No MDF directory, only files. Can we still extract enough info?
        d = write_files({
            '446.000.2013254233714881.ASA': 'a',
            '447.000.2013254233711482.ASA': 'a',
            'LC80880750762013254ASA00_IDF.xml': 'a',
            'LC80880750762013254ASA00_MD5.txt': 'a',
        })

        def _test_mdf_output(metadata):
            self.assertEquals(metadata.usgs.interval_id, 'LC80880750762013254ASA00')
            self.assertEquals(metadata.platform.code, 'LANDSAT_8')
            self.assertEquals(metadata.instrument.name, 'OLI_TIRS')
            self.assertEqual(metadata.format_.name, 'MDF')
            self.assertEqual(metadata.ga_level, 'P00')
            self.assertEquals(metadata.image.satellite_ref_point_start, ptype.Point(88, 75))
            self.assertEquals(metadata.image.satellite_ref_point_end, ptype.Point(88, 76))
            self.assertEquals(metadata.acquisition.groundstation.code, 'ASA')
            # No longer bother with vague center date.
            # self.assertEqual(metadata.extent.center_dt, datetime.date(2013, 9, 11))
            self.assertEqual(metadata.acquisition.aos, datetime.datetime(2013, 9, 11, 23, 36, 11, 482000))
            self.assertEqual(metadata.acquisition.los, datetime.datetime(2013, 9, 11, 23, 37, 14, 881000))

        metadata = mdf.extract_md(ptype.DatasetMetadata(), d)
        _test_mdf_output(metadata)

    def test_find_mdf_directory(self):
        d = write_files({
            'LC80880750762013254ASA00': {
                '446.000.2013254233714881.ASA': 'a',
                '447.000.2013254233711482.ASA': 'a',
                'LC80880750762013254ASA00_IDF.xml': 'a',
                'LC80880750762013254ASA00_MD5.txt': 'a',
            }
        })

        expected_dir = d.joinpath('LC80880750762013254ASA00')
        expected_return = (
            expected_dir,
            {
                expected_dir.joinpath('446.000.2013254233714881.ASA'),
                expected_dir.joinpath('447.000.2013254233711482.ASA')
            }
        )

        # The actual directory.
        self.assertEqual(
            expected_return,
            mdf.find_mdf_files(d.joinpath('LC80880750762013254ASA00'))
        )
        # A directory containing just the actual directory.
        self.assertEqual(
            expected_return,
            mdf.find_mdf_files(d)
        )

    def test_find_mdf_dir_with_input(self):
        # A structure encountered from some NCI processors: An extra input-directory.
        # The 'input' folder is passed as the dataset.
        d = write_files({
            'LC80880750762013254ASA00': {
                'input': {
                    '446.000.2013254233714881.ASA': 'a',
                    '447.000.2013254233711482.ASA': 'a',
                    'LC80880750762013254ASA00_IDF.xml': 'a',
                    'LC80880750762013254ASA00_MD5.txt': 'a',
                }
            }
        })

        expected_dir = d.joinpath('LC80880750762013254ASA00')
        expected_return = (
            expected_dir,
            {
                expected_dir.joinpath('input', '446.000.2013254233714881.ASA'),
                expected_dir.joinpath('input', '447.000.2013254233711482.ASA')
            }
        )

        self.assertEqual(
            expected_return,
            mdf.find_mdf_files(d.joinpath('LC80880750762013254ASA00', 'input'))
        )

    def test_no_directory(self):
        d = write_files({
            'L7EB2013259012832ASN213I00.data': 'nothing',
            'L7EB2013259012832ASN213Q00.data': 'nothing'
        })

        self.assertEqual((None, set()), mdf.find_mdf_files(d))

        # Make sure that metadata is not modified when no MDF is found.
        starting_md = ptype.DatasetMetadata()
        id_ = starting_md.id_
        creation_dt = starting_md.creation_dt
        expected_dt = ptype.DatasetMetadata(id_=id_, creation_dt=creation_dt)

        output = mdf.extract_md(starting_md, d)
        self.assertEqual(expected_dt, output)


if __name__ == '__main__':
    import doctest

    doctest.testmod(mdf)
    unittest.main()
