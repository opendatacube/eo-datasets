# coding=utf-8
"""
Package an LS8 NBAR dataset.
"""
from __future__ import absolute_import
from subprocess import check_call
import uuid
from pathlib import Path
import yaml

import eodatasets.scripts.package
from tests import temp_dir, assert_file_structure

script_path = Path(eodatasets.scripts.package.__file__)
if script_path.suffix == '.pyc':
    script_path = script_path.with_suffix('.py')

source_folder = Path(__file__).parent.joinpath('input', 'ls8-nbar')
assert source_folder.exists()

source_dataset = source_folder.joinpath('data')
assert source_dataset.exists()

parent_dataset = source_folder.joinpath('parent')
assert parent_dataset.exists()


def test_package():
    output_path = temp_dir()

    check_call(
        [
            'python',
            str(script_path),
            'nbar_brdf',
            '--parent', str(parent_dataset),
            str(source_dataset), str(output_path)
        ]
    )

    output_dataset = output_path.joinpath('LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126')

    assert_file_structure(output_path, {
        'LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126': {
            'browse.jpg': '',
            'browse.fr.jpg': '',
            'package': {
                'LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B1.tif': '',
                'LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B2.tif': '',
                'LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B3.tif': '',
                'LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B4.tif': '',
                'LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B5.tif': '',
                'LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B6.tif': '',
                'LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B7.tif': '',
            },
            'ga-metadata.yaml': '',
            'package.sha1': ''
        }
    })

    # TODO: Check metadata fields are sensible.
    output_metadata_path = output_dataset.joinpath('ga-metadata.yaml')
    assert output_metadata_path.exists()
    md = yaml.load(str(output_metadata_path))

    # TODO: Asset all files are checksummed.
    output_metadata_path = output_dataset.joinpath('package.sha1')
    assert output_metadata_path.exists()
    md = yaml.load(str(output_metadata_path))

    print(repr(md))
