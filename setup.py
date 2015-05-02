#!/usr/bin/env python

import os

from setuptools import setup, find_packages
from eodatasets import __version__ as version

# Append TeamCity build number if it gives us one.
if 'BUILD_NUMBER' in os.environ and version.endswith('b'):
    version += '' + os.environ['BUILD_NUMBER']

setup(
    name="eodatasets",
    version=version,
    packages=find_packages(),
    install_requires=[
        'click',
        'python-dateutil',
        'gdal',
        'numpy',
        'pathlib',
        'pyyaml',
    ],
    entry_points='''
        [console_scripts]
        eod-package=eodatasets.scripts.package:cli
        eod-generate-browse=eodatasets.scripts.generatebrowse:cli
    ''',
)
