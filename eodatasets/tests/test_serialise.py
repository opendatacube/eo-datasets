# coding=utf-8
"""
Most serialisation tests are coupled with the type tests (test_type.py)
"""
from __future__ import absolute_import

import unittest

from eodatasets.tests import write_files
from eodatasets import serialise


class TestSerialise(unittest.TestCase):
    def test_expected_metadata_path(self):
        files = write_files({
            'directory_dataset': {'file1.txt': 'test'},
            'file_dataset.tif': 'test'
        })

        # A dataset directory will have an internal 'ga-metadata.yaml' file.
        self.assertEqual(
            serialise.expected_metadata_path(files.joinpath('directory_dataset')).absolute(),
            files.joinpath('directory_dataset', 'ga-metadata.yaml').absolute()
        )

        # A dataset file will have a sibling file ending in 'ga-md.yaml'
        self.assertEqual(
            serialise.expected_metadata_path(files.joinpath('file_dataset.tif')).absolute(),
            files.joinpath('file_dataset.tif.ga-md.yaml').absolute()
        )

        # Nonexistent dataset raises a ValueError.
        with self.assertRaises(ValueError):
            serialise.expected_metadata_path(files.joinpath('missing-dataset.tif'))