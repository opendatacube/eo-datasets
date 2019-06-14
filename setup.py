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
    name="eodatasets2",
    description="Packaging, metadata and provenance for ODC EO datasets",
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
        eo2-validate=eodatasets2.prepare.validate:run
        eo2-prepare=eodatasets2.scripts.genprepare:run
        eo2-recompress-tar=eodatasets2.scripts.recompress:main
    """,
)
