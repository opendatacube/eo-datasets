# coding=utf-8
from __future__ import absolute_import

import atexit
import os
import pathlib
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


def assert_same(o1, o2, prefix=""):
    """
    Assert the two are equal.

    Compares property values one-by-one recursively to print friendly error messages.

    (ie. the exact property that differs)

    :type o1: object
    :type o2: object
    :raises: AssertionError
    """
    __tracebackhide__ = True

    def _compare(k, val1, val2):
        assert_same(val1, val2, prefix=prefix + "." + str(k))

    if isinstance(o1, list) and isinstance(o2, list):
        assert len(o1) == len(o2), "Differing lengths: %s" % prefix

        for i, val in enumerate(o1):
            _compare(i, val, o2[i])
    elif isinstance(o1, dict) and isinstance(o2, dict):
        for k, val in o1.items():
            assert k in o2, "%s[%r] is missing.\n\t%r\n\t%r" % (prefix, k, o1, o2)
        for k, val in o2.items():
            assert k in o1, "%s[%r] is missing.\n\t%r\n\t%r" % (prefix, k, o2, o1)
            _compare(k, val, o1[k])
    elif o1 != o2:
        sys.stderr.write("%r\n" % o1)
        sys.stderr.write("%r\n" % o2)
        raise AssertionError("Mismatch for property %r:  %r != %r" % (prefix, o1, o2))


def assert_file_structure(folder, expected_structure, root=""):
    """
    Assert that the contents of a folder (filenames and subfolder names recursively)
    match the given nested dictionary structure.

    :type folder: pathlib.Path
    :type expected_structure: dict[str,str|dict]
    """
    __tracebackhide__ = True
    required_filenames = set(
        name for name, option in expected_structure.items() if option != "optional"
    )
    optional_filenames = set(
        name for name, option in expected_structure.items() if option == "optional"
    )

    actual_filenames = {f.name for f in folder.iterdir()}

    if required_filenames != (actual_filenames - optional_filenames):
        missing_files = required_filenames - actual_filenames
        missing_text = "Missing: %r" % (sorted(list(missing_files)))
        extra_files = actual_filenames - required_filenames
        added_text = "Extra  : %r" % (sorted(list(extra_files)))
        raise AssertionError(
            "Folder mismatch of %r\n\t%s\n\t%s" % (root, missing_text, added_text)
        )

    for k, v in expected_structure.items():
        id_ = "%s/%s" % (root, k) if root else k

        is_optional = v == "optional"

        f = folder.joinpath(k)

        if not f.exists():
            if is_optional:
                continue

            assert False, "%s is missing" % (id_,)
        elif isinstance(v, dict):
            assert f.is_dir(), "%s is not a dir" % (id_,)
            assert_file_structure(f, v, id_)
        elif isinstance(v, str):
            assert f.is_file(), "%s is not a file" % (id_,)
        else:
            assert (
                False
            ), "Only strings and dicts expected when defining a folder structure."


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
        la, lb = list(a), list(b)
        self.assertEqual(len(la), len(lb), msg=msg)
        self.assertSetEqual(set(la), set(lb), msg=msg)

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
    containing_dir = tempfile.mkdtemp(suffix="neotestrun")
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
            with open(path, "w") as f:
                if isinstance(contents, list):
                    f.writelines(contents)
                elif isinstance(contents, str):
                    f.write(contents)
                else:
                    raise Exception("Unexpected file contents: %s" % type(contents))


def temp_dir():
    """
    Create and return a temporary directory that will be deleted automatically on exit.

    :rtype: pathlib.Path
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


def as_file_list(path):
    """
    Build a flat list of filenames relative to the given folder
    (similar to the contents of package.sha1 files)
    """
    output = []
    for directory, _, files in os.walk(str(path)):
        output.extend(
            str(Path(directory).relative_to(path).joinpath(file_)) for file_ in files
        )
    return output
