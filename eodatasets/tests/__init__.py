# coding=utf-8
from __future__ import absolute_import
import unittest
import atexit
import os
import shutil
import tempfile
import sys

from eodatasets import compat

import pathlib

import eodatasets.type as ptype


def assert_same(o1, o2, prefix=''):
    """
    Assert the two are equal.

    Compares property values one-by-one recursively to print friendly error messages.

    (ie. the exact property that differs)

    :type o1: object
    :type o2: object
    :raises: AssertionError
    """

    def _compare(k, val1, val2):
        assert_same(val1, val2, prefix=prefix + '.' + str(k))

    if isinstance(o1, ptype.SimpleObject):
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
        sys.stderr.write("%r\n" % o1)
        sys.stderr.write("%r\n" % o2)
        raise AssertionError("Mismatch for property %r:  %r != %r" % (prefix, o1, o2))


class TestCase(unittest.TestCase):
    def assert_values_equal(self, a, b, msg=None):
        """
        Assert sequences of values are equal.

        This is like assertSeqEqual, but doesn't require len() or indexing.
        (ie. it works with generators and general iterables)
        """
        self.assertListEqual(list(a), list(b), msg=msg)

    def assert_items_equal(self, a, b, msg=None):
        """
        Assert the two contain the same items, in any order.

        (python 2 contained something similar, but appears to be removed in python 3?)
        """
        self.assertSetEqual(set(a), set(b), msg=None)

    def assert_same(self, a, b, msg=None):
        return assert_same(a, b)


def write_files(file_dict):
    """
    Convenience method for writing a bunch of files to a temporary directory.

    Dict format is "filename": "text content"

    If content is another dict, it is created recursively in the same manner.

    writeFiles({'test.txt': 'contents of text file'})

    :type file_dict: dict
    :rtype: pathlib.Path
    :return: Created temporary directory path
    """
    containing_dir = tempfile.mkdtemp(suffix='neotestrun')
    _write_files_to_dir(containing_dir, file_dict)

    def remove_if_exists(path):
        if os.path.exists(path):
            shutil.rmtree(path)

    atexit.register(remove_if_exists, containing_dir)
    return pathlib.Path(containing_dir)


def _write_files_to_dir(directory_path, file_dict):
    """
    Convenience method for writing a bunch of files to a given directory.

    :type directory_path: str
    :type file_dict: dict
    """
    for filename, contents in file_dict.items():
        path = os.path.join(directory_path, filename)
        if isinstance(contents, dict):
            os.mkdir(path)
            _write_files_to_dir(path, contents)
        else:
            with open(path, 'w') as f:
                if isinstance(contents, list):
                    f.writelines(contents)
                elif isinstance(contents, compat.string_types):
                    f.write(contents)
                else:
                    raise Exception('Unexpected file contents: %s' % type(contents))


def temp_dir():
    """
    Create and return a temporary directory that will be deleted automatically on exit.

    :rtype: str
    """
    return write_files({})


def temp_file(suffix=""):
    """
    Get a temporary file path that will be cleaned up on exit.

    Simpler than NamedTemporaryFile--- just a file path, no open mode or anything.
    :return:
    """
    f = tempfile.mktemp(suffix=suffix)

    def permissive_ignore(file_):
        if os.path.exists(file_):
            os.remove(file_)

    atexit.register(permissive_ignore, f)
    return f


def file_of_size(path, size_mb):
    """
    Create a blank file of the given size.
    """
    with open(path, "wb") as f:
        f.seek(size_mb * 1024 * 1024 - 1)
        f.write("\0")
