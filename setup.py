#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="eodatasets",
    version="0.1b",
    packages=find_packages(),
    install_requires=[
        'click',
        'dateutil',
        'gdal',
        'numpy',
        'pathlib',
        'pyyaml',
    ]
)
