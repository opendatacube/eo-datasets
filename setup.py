#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages

import versioneer


tests_require = [
    "deepdiff",
    "flake8",
    "hypothesis",
    "mock",
    "pep8-naming",
    "pytest",
    "pytest-flake8",
    "python-rapidjson",
]


setup(
    name="eodatasets",
    description="Packaging, metadata and provenance for GA EO datasets",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(exclude=("tests", "tests.*")),
    package_data={"": ["*.json"]},
    install_requires=[
        "attrs",
        "boltons",
        "cattrs",
        "click",
        "python-dateutil",
        "checksumdir",
        "ciso8601",
        "gdal",
        "jsonschema",
        "numpy",
        "PyYAML!=5.1",
        "pyproj",
        "netCDF4",
        "rasterio",
        "ruamel.yaml",
        "shapely",
        "scipy",
        "structlog",
        "h5py",
    ],
    tests_require=tests_require,
    extras_require={"test": tests_require},
    entry_points="""
        [console_scripts]
        eod-validate=eodatasets.prepare.validate:run
        eod-prepare=eodatasets.scripts.genprepare:run
        eod-recompress-tar=eodatasets.scripts.recompress:main
    """,
)
