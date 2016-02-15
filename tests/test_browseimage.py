# coding=utf-8

from __future__ import absolute_import

from eodatasets import browseimage, drivers, type as ptype
from tests import write_files, assert_same


def test_create_typical_browse_metadata():
    class TestDriver(drivers.DatasetDriver):
        def browse_image_bands(self, d):
            return '5', '1', '3'

    d = write_files({})
    dataset = browseimage.create_typical_browse_metadata(TestDriver(), ptype.DatasetMetadata(), d)

    expected = ptype.DatasetMetadata(
        browse={
            'full':
                ptype.BrowseMetadata(
                    path=d.joinpath('browse.fr.jpg'),
                    file_type='image/jpg',
                    red_band='5',
                    green_band='1',
                    blue_band='3'
                ),
            'medium':
                ptype.BrowseMetadata(
                    path=d.joinpath('browse.jpg'),
                    # Default medium size.
                    shape=ptype.Point(1024, None),
                    file_type='image/jpg',
                    red_band='5',
                    green_band='1',
                    blue_band='3'
                )
        }
    )

    expected.id_, dataset.id_ = None, None
    assert_same(expected, dataset)


def test_create_mono_browse_metadata():
    # A single band for the browse image.

    class TestDriver(drivers.DatasetDriver):
        def browse_image_bands(self, d):
            return '5'

    d = write_files({})
    dataset = browseimage.create_typical_browse_metadata(TestDriver(), ptype.DatasetMetadata(), d)

    expected = ptype.DatasetMetadata(
        browse={
            'full':
                ptype.BrowseMetadata(
                    path=d.joinpath('browse.fr.jpg'),
                    file_type='image/jpg',
                    red_band='5',
                    green_band='5',
                    blue_band='5'
                ),
            'medium':
                ptype.BrowseMetadata(
                    path=d.joinpath('browse.jpg'),
                    # Default medium size.
                    shape=ptype.Point(1024, None),
                    file_type='image/jpg',
                    red_band='5',
                    green_band='5',
                    blue_band='5'
                )
        }
    )

    expected.id_, dataset.id_ = None, None
    assert_same(expected, dataset)
