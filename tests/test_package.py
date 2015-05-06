# coding=utf-8
from __future__ import absolute_import
import unittest

from eodatasets import package
from tests import write_files, TestCase


class TestPackage(TestCase):
    def test_prepare_same_destination(self):
        dataset_path = write_files({'LC81010782014285LGN00_B6.TIF': 'test'})

        size_bytes = package.prepare_target_imagery(dataset_path, dataset_path, compress_imagery=False)
        self.assertEqual(size_bytes, 4)

    def test_prepare_copy_destination(self):
        test_path = write_files({'source_dir': {
            'LC81010782014285LGN00_B6.img': 'test'
        }})
        source_path = test_path.joinpath('source_dir')
        dest_path = test_path.joinpath('dest_dir')

        size_bytes = package.prepare_target_imagery(source_path, dest_path, compress_imagery=False)
        self.assertEqual(size_bytes, 4)

        # Ensure dest file was created.
        self.assertTrue(dest_path.is_dir())
        dest_file = dest_path.joinpath('LC81010782014285LGN00_B6.img')
        self.assertTrue(dest_file.is_file())
        self.assertTrue(dest_file.stat().st_size, 4)

        # Ensure source path was not touched.
        source_file = source_path.joinpath('LC81010782014285LGN00_B6.img')
        self.assertTrue(source_file.is_file())
        self.assertTrue(source_file.stat().st_size, 4)

    def test_multi_copy_hardlink(self):
        # Copy two files.
        test_path = write_files({'source_dir': {
            'LC81010782014285LGN00_B6.img': 'test',
            'LC81010782014285LGN00_B4.tif': 'best'
        }})
        source_path = test_path.joinpath('source_dir')
        dest_path = test_path.joinpath('dest_dir')

        size_bytes = package.prepare_target_imagery(
            source_path,
            dest_path,
            compress_imagery=False,
            hard_link=True
        )
        # Four bytes each == 8 bytes
        self.assertEqual(size_bytes, 8)

        # Ensure dest files were created.
        self.assertTrue(dest_path.is_dir())
        dest_file = dest_path.joinpath('LC81010782014285LGN00_B6.img')
        self.assertTrue(dest_file.is_file())
        self.assertTrue(dest_file.stat().st_size, 4)
        dest_file = dest_path.joinpath('LC81010782014285LGN00_B4.tif')
        self.assertTrue(dest_file.is_file())
        self.assertTrue(dest_file.stat().st_size, 4)

        # Source should be untouched.
        source_file = source_path.joinpath('LC81010782014285LGN00_B4.tif')
        self.assertTrue(source_file.is_file())
        self.assertTrue(source_file.stat().st_size, 4)

        # Ensure they were hard linked (share the same inode)
        self.assertEqual(source_file.stat().st_ino, dest_file.stat().st_ino)
