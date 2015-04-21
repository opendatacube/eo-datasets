#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="eodatasets",
    version="0.1b",
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
