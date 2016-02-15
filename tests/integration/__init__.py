# coding=utf-8
"""
Module
"""
from __future__ import absolute_import


def load_checksum_filenames(output_metadata_path):
    return [line.split('\t')[-1][:-1] for line in output_metadata_path.open('r').readlines()]


def on_same_filesystem(path1, path2):
    return path1.stat().st_dev == path2.stat().st_dev


def hardlink_arg(path1, path2):
    return '--hard-link' if on_same_filesystem(path1, path2) else '--no-hard-link'


def directory_size(directory):
    """
    Total size of files in the given directory.
    :type file_paths: Path
    :rtype: int
    """
    return sum(p.stat().st_size
               for p in directory.rglob('*') if p.is_file())
