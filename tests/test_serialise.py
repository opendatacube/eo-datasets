# coding=utf-8
"""
Most serialisation tests are coupled with the type tests (test_type.py)
"""
from __future__ import absolute_import
import uuid
import datetime

from pathlib import Path
from hypothesis import given, strategy

from hypothesis.specifiers import dictionary, strings

from tests import write_files, TestCase, slow
from eodatasets import serialise, compat, type as ptype


strings_without_trailing_underscore = strategy(
    strings([chr(i) for i in range(128)])
) \
    .filter(lambda s: not s.endswith('_'))


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
    @given(dictionary(strings_without_trailing_underscore, int))
    def test_flat_dict_flattens_identically(self, dict_):
        print(dict_)
        self.assert_items_equal(
            dict_.items(),
            serialise.as_flat_key_value(dict_)
        )

    @slow
    @given(dictionary(str, int))
    def test_flat_dict_flattens_without_underscore_suffix(self, dict_):
        # A (single) trailing underscore should be stripped from key names if present, as these are added
        # for python name conflicts.

        # If we append an underscore to every key, the result should be identical.
        self.assert_items_equal(
            dict_.items(),
            serialise.as_flat_key_value({k + '_': v for k, v in dict_.items()})
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

    def test_key_value_types(self):
        class Test1(ptype.SimpleObject):
            def __init__(self, a, b, c, d=None):
                self.a = a
                self.b = b
                self.c = c
                self.d = d

        uuid_ = uuid.uuid1()
        date_ = datetime.datetime.utcnow()
        self.assert_values_equal(
            [
                ('a:0', 'a'),
                ('a:1', 'b'),
                ('a:2:a', 1),
                ('a:2:b', 2),
                ('b', compat.long_int(2)),
                ('c', date_.isoformat()),
                ('d:a', str(uuid_)),
                ('d:b', 'something/testpath.txt'),
                ('d:c:a', 42)
            ],
            serialise.as_flat_key_value(
                Test1(
                    a=['a', 'b', Test1(1, 2, None)],
                    b=compat.long_int(2),
                    c=date_,
                    d=Test1(
                        a=uuid_,
                        b=Path('/tmp/something/testpath.txt'),
                        c={'a': 42}
                    )
                ),
                relative_to=Path('/tmp'),
                key_separator=':'
            )
        )