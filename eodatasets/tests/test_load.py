import unittest

from pathlib import PosixPath, Path

import eodatasets.package as load
from eodatasets.metadata.tests import write_files
from eodatasets.type import *


BASIC_BANDS = {
    '11': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B11.TIF'),
        number='11',
    ),
    '10': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B10.TIF'),
        number='10',
    ),
    '1': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B1.TIF'),
        number='1',
    ),
    '3': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B3.TIF'),
        number='3',
    ),
    '2': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B2.TIF'),
        number='2',
    ),
    '5': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B5.TIF'),
        number='5',
    ),
    '4': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B4.TIF'),
        number='4',
    ),
    '7': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B7.TIF'),
        number='7',
    ),
    '6': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B6.TIF'),
        number='6',
    ),
    '9': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B9.TIF'),
        number='9',
    ),
    '8': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B8.TIF'),
        number='8',
    ),
    'quality': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_BQA.TIF'),
        number='quality',
    )
}

EXPANDED_BANDS = {
    '11': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B11.TIF'),
        type_=u'thermal',
        label=u'Thermal Infrared 2',
        number='11',
        cell_size=25.0
    ),
    '10': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B10.TIF'),
        type_=u'thermal',
        label=u'Thermal Infrared 1',
        number='10',
        cell_size=25.0
    ),
    '1': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B1.TIF'),
        type_=u'reflective',
        label=u'Coastal Aerosol',
        number='1',
        cell_size=25.0
    ),
    '3': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B3.TIF'),
        type_=u'reflective',
        label=u'Visible Green',
        number='3',
        cell_size=25.0
    ),
    '2': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B2.TIF'),
        type_=u'reflective',
        label=u'Visible Blue',
        number='2',
        cell_size=25.0
    ),
    '5': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B5.TIF'),
        type_=u'reflective',
        label=u'Near Infrared',
        number='5',
        cell_size=25.0
    ),
    '4': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B4.TIF'),
        type_=u'reflective',
        label=u'Visible Red',
        number='4',
        cell_size=25.0
    ),
    '7': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B7.TIF'),
        type_=u'reflective',
        label=u'Short-wave Infrared 2',
        number='7',
        cell_size=25.0
    ),
    '6': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B6.TIF'),
        type_=u'reflective',
        label=u'Short-wave Infrared 1',
        number='6',
        cell_size=25.0
    ),
    '9': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B9.TIF'),
        type_=u'atmosphere',
        label=u'Cirrus',
        number='9',
        cell_size=25.0
    ),
    '8': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B8.TIF'),
        type_=u'panchromatic',
        label=u'Panchromatic',
        number='8',
        cell_size=12.5
    ),
    'quality': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_BQA.TIF'),
        type_=u'quality',
        label=u'Quality',
        number='quality',
        cell_size=25.0
    )
}


class TestBandExpansion(unittest.TestCase):
    def test_expand_band(self):
        # Create fake image file.
        image_file = write_files({'LC81010782014285LGN00_B6.TIF': 'test'})
        image_file = image_file.joinpath('LC81010782014285LGN00_B6.TIF')

        md = load.expand_band_information(
            'LANDSAT_8', 'OLI_TIRS',
            BandMetadata(path=image_file, number='6')
        )

        expected = BandMetadata(
            path=Path(image_file),
            type_=u'reflective',
            label=u'Short-wave Infrared 1',
            number='6',
            # MD5 of image contents ('test')
            checksum_md5='098f6bcd4621d373cade4e832627b4f6',
            cell_size=25.0
        )
        self.assertEqual(md, expected)

    def test_expand_all_bands(self):
        for number, band_metadata in BASIC_BANDS.items():
            load.expand_band_information('LANDSAT_8', 'OLI_TIRS', band_metadata, checksum=False)

        self.assertEqual(BASIC_BANDS, EXPANDED_BANDS)

    def test_prepare_same_destination(self):
        dataset_path = write_files({'LC81010782014285LGN00_B6.TIF': 'test'})

        size_bytes = load.prepare_target_imagery(dataset_path, dataset_path, compress_imagery=False)
        self.assertEqual(size_bytes, 4)

    def test_prepare_copy_destination(self):
        test_path = write_files({'source_dir': {'LC81010782014285LGN00_B6.TIF': 'test'}})
        source_path = test_path.joinpath('source_dir')
        dest_path = test_path.joinpath('dest_dir')

        size_bytes = load.prepare_target_imagery(source_path, dest_path, compress_imagery=False)
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

