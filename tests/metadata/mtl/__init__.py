# coding=utf-8
from __future__ import absolute_import

import uuid

from pathlib import Path

import eodatasets.type as ptype
from eodatasets.metadata import ortho
from tests import assert_same


def assert_expected_mtl(mtl_file, expected_ds, base_folder=Path('/tmp/fake-folder')):
    """

    :type mtl_file: Path
    :type expected_ds: ptype.DatasetMetadata

    """

    assigned_id = uuid.UUID('3ff71eb0-d5c5-11e4-aebb-1040f381a756')

    ds = ptype.DatasetMetadata(id_=assigned_id)
    ds = ortho._populate_ortho_from_files(base_folder, ds, mtl_file, None)

    expected_ds.id_ = assigned_id

    assert_same(ds, expected_ds)
