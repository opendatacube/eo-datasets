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
    "rio_cogeo",
]


setup(
    name="eodatasets3",
    description="Packaging, metadata and provenance for OpenDataCube EO3 datasets",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(exclude=("tests", "tests.*")),
    package_data={"": ["*.json", "*.yaml"]},
    install_requires=[
        "attrs",
        "boltons",
        "cattrs",
        "checksumdir",
        "ciso8601",
        "click",
        "gdal",
        "h5py",
        "jsonschema",
        "netCDF4",
        "numpy",
        "pyproj",
        "python-dateutil",
        "PyYAML!=5.1",
        "rasterio",
        "ruamel.yaml",
        "scipy",
        "shapely",
        "scikit-image",
        "structlog",
    ],
    tests_require=tests_require,
    extras_require={"test": tests_require},
    entry_points="""
        [console_scripts]
        eo3-validate=eodatasets3.validate:run
        eo3-prepare=eodatasets3.scripts.genprepare:run
        eo3-recompress-tar=eodatasets3.scripts.recompress:main
        eo3-package-wagl=eodatasets3.scripts.packagewagl:run
        eo3-to-stac=eodatasets3.scripts.tostac:run
    """,
)
