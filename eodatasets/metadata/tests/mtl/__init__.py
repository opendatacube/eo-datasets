from pathlib import Path

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
        _assert_same(val1, val2, prefix=prefix + '.' + str(k))

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
        assert len(o1) == len(o2), "Differing lengths: %s\n\t%r\n\t%r" % (prefix, o1, o2)

        for k, val in o1.items():
            assert k in o2, "%s[%r] is missing.\n\t%r\n\t%r" % (prefix, k, o1, o2)
            _compare(k, val, o2[k])

    elif o1 != o2:
        print repr(o1)
        print repr(o2)
        raise AssertionError("Mismatch for property %r:  %r != %r" % (prefix, o1, o2))



if __name__ == '__main__':
    import unittest

    unittest.main()
