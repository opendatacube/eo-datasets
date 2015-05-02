# coding=utf-8
"""
Most serialisation tests are coupled with the type tests (test_type.py)
"""
from __future__ import absolute_import

from eodatasets.tests import write_files, TestCase, slow
from eodatasets import serialise, compat, type as ptype
from hypothesis import given
from hypothesis.specifiers import dictionary


class TestSerialise(TestCase):
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

    def test_as_key_value(self):
        self.assert_values_equal(
            serialise.as_flat_key_value({
                'a': 1,
                'b': compat.long_int(2),
                'c': 2.3,
                'd': {
                    'd_inner': {
                        'a': 42
                    }
                }
            }),
            [
                ('a', 1),
                ('b', compat.long_int(2)),
                ('c', 2.3),
                ('d.d_inner.a', 42)
            ]
        )

    @slow
    @given(dictionary(str, int))
    def test_flat_dict_flattens_identically(self, dict_):
        self.assert_items_equal(
            dict_,
            serialise.as_flat_key_value(dict_)
        )

    def test_key_value_simple_obj(self):
        class Test1(ptype.SimpleObject):
            def __init__(self, a, b, c, d=None):
                self.a = a
                self.b = b
                self.c = c
                self.d = d

        self.assert_values_equal(
            serialise.as_flat_key_value(
                Test1(
                    a=1,
                    b=compat.long_int(2),
                    c=2.3,
                    d=Test1(
                        a=1,
                        b=2,
                        c={'a': 42}
                    )
                )
            ),
            [
                ('a', 1),
                ('b', compat.long_int(2)),
                ('c', 2.3),
                ('d.a', 1),
                ('d.b', 2),
                ('d.c.a', 42)
            ]
        )