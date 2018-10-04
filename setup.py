#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages

import versioneer

setup(
    name="eodatasets",
    description="Packaging, metadata and provenance for GA EO datasets",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
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
            'hypothesis',
            'mock',
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
