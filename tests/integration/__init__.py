# coding=utf-8
"""
Module
"""
from __future__ import absolute_import
from pathlib import Path


def get_script_path(module):
    m = Path(module.__file__)
    if m.suffix == '.pyc':
        return m.with_suffix('.py')

    return m


def load_checksum_filenames(output_metadata_path):
    return [line.split('\t')[-1][:-1] for line in output_metadata_path.open('r').readlines()]
