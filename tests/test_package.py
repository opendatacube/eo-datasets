# coding=utf-8
from __future__ import absolute_import

from eodatasets import package, drivers, type as ptype
from tests import write_files, TestCase, assert_file_structure


class TestPackage(TestCase):
    def test_prepare_copy_destination(self):
        test_path = write_files({'source_dir': {
            'LC81010782014285LGN00_B6.img': 'test'
        }})
        source_path = test_path.joinpath('source_dir')
        dest_path = test_path.joinpath('dest_dir')

        package.prepare_target_imagery(
            source_path,
            dest_path,
            compress_imagery=False
        )

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

        package.prepare_target_imagery(
            source_path,
            dest_path,
            compress_imagery=False,
            hard_link=True
        )

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

    def test_copy_callbacks_called(self):
        test_path = write_files({'source_dir': {
            'LC81010782014285LGN00_B6.img': 'test',
            'LC81010782014285LGN00_B6.swamp': 'test'
        }})
        source_path = test_path.joinpath('source_dir')
        dest_path = test_path.joinpath('dest_dir')

        called_back = []
        package.prepare_target_imagery(
            source_path,
            dest_path,
            translate_path=lambda p: p.with_suffix('.tif') if p.suffix == '.img' else None,
            after_file_copy=lambda source, dest: called_back.append((source, dest)),
            compress_imagery=False
        )
        dest_file = dest_path.joinpath('LC81010782014285LGN00_B6.tif')

        # The after_file_copy() callback should be called for each copied file.
        # *.swamp should not be returned, as it received None from our path translation.
        self.assertEqual(
            [
                (source_path.joinpath('LC81010782014285LGN00_B6.img'), dest_file)
            ], called_back
        )

        assert_file_structure(
            test_path,
            {
                'source_dir': {
                    'LC81010782014285LGN00_B6.img': 'test',
                    'LC81010782014285LGN00_B6.swamp': 'test'
                },
                'dest_dir': {
                    'LC81010782014285LGN00_B6.tif': 'test',
                }
            }
        )
        self.assertTrue(dest_file.stat().st_size, 4)
        # Ensure source path was not touched.
        source_file = source_path.joinpath('LC81010782014285LGN00_B6.img')
        self.assertTrue(source_file.stat().st_size, 4)

    def test_total_file_size(self):
        # noinspection PyProtectedMember
        f = write_files({
            'first.txt': 'test',
            'second.txt': 'test2'
        })

        self.assertEqual(9, package._file_size_bytes(*f.iterdir()))

    def test_prepare_metadata(self):
        f = write_files({
            'first.txt': 'test',
            'second.txt': 'test2'
        })

        class FauxDriver(drivers.DatasetDriver):
            def to_band(self, dataset, path):
                numbers = {
                    'first': ptype.BandMetadata(path=path, number='1'),
                    'second': None
                }
                return numbers.get(path.stem)

            def get_ga_label(self, dataset):
                return 'DATASET_ID_1234'

            def get_id(self):
                return 'faux'

        d = ptype.DatasetMetadata()
        d = package.expand_driver_metadata(FauxDriver(), d, list(f.iterdir()))

        self.assert_same(
            d,
            ptype.DatasetMetadata(
                id_=d.id_,
                ga_label='DATASET_ID_1234',
                product_type='faux',
                size_bytes=9,
                image=ptype.ImageMetadata(
                    bands={
                        '1': ptype.BandMetadata(path=f.joinpath('first.txt'), number='1')
                    }
                )
            )
        )

    def test_expand_metadata_without_bands(self):
        # We have imagery files but no bands (eg: RAW data)

        f = write_files({
            'first.txt': 'test',
            'second.txt': 'test2'
        })

        class FauxDriver(drivers.DatasetDriver):
            def to_band(self, dataset, path):
                return None

            def get_ga_label(self, dataset):
                return 'DATASET_ID_1234'

            def get_id(self):
                return 'faux'

        d = ptype.DatasetMetadata()
        # Iterator is falsey, but returns files. This triggered a bug previously.
        # noinspection PyTypeChecker
        d = package.expand_driver_metadata(FauxDriver(), d, f.iterdir())

        self.assert_same(
            d,
            ptype.DatasetMetadata(
                id_=d.id_,
                ga_label='DATASET_ID_1234',
                product_type='faux',
                size_bytes=9
            )
        )
