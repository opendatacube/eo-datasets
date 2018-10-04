#!/usr/bin/env python
# coding=utf-8

import os

from setuptools import setup, find_packages
from eodatasets import __version__ as version

# Append TeamCity build number if it gives us one.
if 'BUILD_NUMBER' in os.environ and version.endswith('b'):
    version += '' + os.environ['BUILD_NUMBER']

setup(
    name="eodatasets",
    description="Packaging, metadata and provenance for GA EO datasets",
    version=version,
    packages=find_packages(exclude=('tests', 'tests.*')),
    package_data={
        '': ['*.json'],
    },
    install_requires=[
        'click',
        'python-dateutil',
        'checksumdir',
        'ciso8601',
        'gdal',
        'numpy',
        'pyyaml',
        'rasterio',
        'shapely',

        'scipy'
    ],
    extras_require=dict(
        test=[
            'pytest',
            'pytest-flake8',
            'deepdiff',
            'flake8',
            'pep8-naming',
        ],
    ),
    entry_points='''
        [console_scripts]
        eod-package=eodatasets.scripts.genpackage:run
        eod-generate-metadata=eodatasets.scripts.genmetadata:run
        eod-generate-browse=eodatasets.scripts.genbrowse:run
    ''',
)
