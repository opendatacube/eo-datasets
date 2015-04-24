import unittest

from eodatasets import package
from eodatasets.tests import write_files


class TestPackage(unittest.TestCase):
    def test_prepare_same_destination(self):
        dataset_path = write_files({'LC81010782014285LGN00_B6.TIF': 'test'})

        size_bytes = package.prepare_target_imagery(dataset_path, dataset_path, compress_imagery=False)
        self.assertEqual(size_bytes, 4)

    def test_prepare_copy_destination(self):
        test_path = write_files({'source_dir': {'LC81010782014285LGN00_B6.TIF': 'test'}})
        source_path = test_path.joinpath('source_dir')
        dest_path = test_path.joinpath('dest_dir')

        size_bytes = package.prepare_target_imagery(source_path, dest_path, compress_imagery=False)
        self.assertEqual(size_bytes, 4)

        # Ensure dest file was created.
        self.assertTrue(dest_path.is_dir())
        dest_file = dest_path.joinpath('LC81010782014285LGN00_B6.TIF')
        self.assertTrue(dest_file.is_file())
        self.assertTrue(dest_file.stat().st_size, 4)

        # Ensure source path was not touched.
        source_file = dest_path.joinpath('LC81010782014285LGN00_B6.TIF')
        self.assertTrue(source_file.is_file())
        self.assertTrue(source_file.stat().st_size, 4)

