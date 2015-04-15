import unittest

import datetime
from pathlib import Path, PosixPath
from eodatasets.type import *
from eodatasets.metadata import mtl
import eodatasets.type as ptype


def assert_expected_mtl(mtl_file, expected_ds, base_folder=Path('/tmp/fake-folder')):
    """

    :type mtl_file: Path
    :type expected_ds: ptype.DatasetMetadata

    """

    assigned_id = uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756')

    ds = DatasetMetadata(id_=assigned_id)
    ds = mtl.populate_from_mtl(ds, mtl_file, base_folder=base_folder)

    expected_ds.id_ = assigned_id

    _assert_same(ds, expected_ds)


class TestMtlRead(unittest.TestCase):
    def test_ls8_equivalence(self):

        mtl_path = Path(os.path.join(os.path.dirname(__file__), 'data', 'ls8_mtl.txt'))

        ds = DatasetMetadata(id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'))
        ds = mtl.populate_from_mtl(ds, mtl_path, base_folder=Path('/tmp/fake-folder'))

        _assert_same(ds, _EXPECTED_LS8_OUT)

        # Sanity check: different dataset_id is not equal.
        ds = DatasetMetadata()
        ds = mtl.populate_from_mtl(ds, mtl_path, base_folder=Path('/tmp/fake-folder'))
        self.assertNotEqual(ds, _EXPECTED_LS8_OUT)

    def test_ls5_equivalence(self):

        mtl_path = Path(os.path.join(os.path.dirname(__file__), 'data', 'ls5_definitive_mtl.txt'))

        ds = DatasetMetadata(id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'))
        ds = mtl.populate_from_mtl(ds, mtl_path, base_folder=Path('/tmp/fake-folder'))

        _assert_same(ds, _EXPECTED_LT5_OUT)

    def test_ls7_equivalence(self):

        mtl_path = Path(os.path.join(os.path.dirname(__file__), 'data', 'ls7_predictive_mtl.txt'))

        ds = DatasetMetadata(id_=uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756'))
        ds = mtl.populate_from_mtl(ds, mtl_path, base_folder=Path('/tmp/fake-folder'))

        _assert_same(ds, _EXPECTED_L7_OUT)


def _assert_same(o1, o2, prefix=''):
    """
    Assert the two are equal.

    Compares property values one-by-one recursively to print friendly error messages.

    (ie. the exact property that differs)

    :type o1: object
    :type o2: object
    :raises: AssertionError
    """

    def _compare(k, val1, val2):
        _assert_same(val1, val2, prefix=prefix+'.'+str(k))

    if isinstance(o1, SimpleObject):
        assert o1.__class__ == o2.__class__, "Differing classes %r: %r and %r" \
                                             % (prefix, o1.__class__.__name__, o2.__class__.__name__)

        for k, val in o1.items_ordered(skip_nones=False):
            _compare(k, val, getattr(o2, k))
    elif isinstance(o1, list) and isinstance(o2, list):
        assert len(o1) == len(o2), "Differing lengths: %s" % prefix

        for i, val in enumerate(o1):
            _compare(i, val, o2[i])
    elif isinstance(o1, dict) and isinstance(o2, dict):
        assert len(o1) == len(o2), "Differing lengths: %s" % prefix

        for k, val in o1.items():
            _compare(k, val, o2[k])
    elif o1 != o2:
        print repr(o1)
        print repr(o2)
        raise AssertionError("Mismatch for property %r:  %r != %r" % (prefix, o1, o2))


def load_tests(loader=None, standard_tests=None, pattern=None):
    # top level directory cached on loader instance
    this_dir = os.path.dirname(__file__)
    package_tests = loader.discover(start_dir=this_dir, pattern=pattern)
    standard_tests.add
    return standard_tests


if __name__ == '__main__':

    import unittest
    unittest.main()
