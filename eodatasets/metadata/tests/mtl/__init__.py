# coding=utf-8
from __future__ import absolute_import
import uuid

from pathlib import Path
from eodatasets.tests import assert_same
from eodatasets.metadata import mtl
import eodatasets.type as ptype


def assert_expected_mtl(mtl_file, expected_ds, base_folder=Path('/tmp/fake-folder')):
    """

    :type mtl_file: Path
    :type expected_ds: ptype.DatasetMetadata

    """

    assigned_id = uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756')

    ds = ptype.DatasetMetadata(id_=assigned_id)
    ds = mtl.populate_from_mtl(ds, mtl_file, base_folder=base_folder)

    expected_ds.id_ = assigned_id

    assert_same(ds, expected_ds)


if __name__ == '__main__':
    import unittest

    unittest.main()
