## EO Datasets

[![Linting](https://github.com/GeoscienceAustralia/eo-datasets/actions/workflows/lint.yml/badge.svg)](https://github.com/GeoscienceAustralia/eo-datasets/actions/workflows/lint.yml)
[![Tests](https://github.com/GeoscienceAustralia/eo-datasets/actions/workflows/test.yml/badge.svg)](https://github.com/GeoscienceAustralia/eo-datasets/actions/workflows/test.yml)
[![Coverage Status](
https://coveralls.io/repos/GeoscienceAustralia/eo-datasets/badge.svg?branch=eodatasets3
)](https://coveralls.io/r/GeoscienceAustralia/eo-datasets?branch=eodatasets3)

A tool to easily write, validate and convert [ODC](https://github.com/opendatacube/datacube-core) 
datasets and metadata.


## Installation

    pip install eodatasets3

Python 3.6+ is supported.

## Dataset assembly

The assembler api aims to make it easy to write datasets.

```python
    from eodatasets3 import DatasetAssembler
    from datetime import datetime
    from pathlib import Path
    
    with DatasetAssembler(
            Path('/some/output/collection/path'), 
            naming_conventions='default') as p:
        p.datetime = datetime(2019, 7, 4, 13, 7, 5)
        p.product_family = "level1"
        p.processed_now()
        
        # Support for custom metadata fields
        p.properties['fmask:cloud_shadow'] = 42.0

        # Write measurements. They can be from numpy arrays, open rasterio datasets,
        # file paths, ODC Datasets...
        p.write_measurement("red", red_path)
        ...  # now write more measurements
        
        # Create a jpg thumbnail image using the measurements we've written
        p.write_thumbnail(red="swir1", green="swir2", blue="red")
        
        # Validate the dataset and write it to the destination folder atomically.
        p.done()
```

The assembler will write a folder of [COG](https://www.cogeo.org/) imagery, an [eo3](#open-data-cube-compatibility) 
metadata doc for Open Data Cube, and create appropriate file and folder structures for the chosen naming conventions. 

Many other fields are available, see [the docs](https://eodatasets.readthedocs.io/en/latest/).

Further examples can be seen in the tests [tests/integration/test_assemble.py](tests/integration/test_assemble.py),
[L1](eodatasets3/prepare/landsat_l1_prepare.py) or [ARD](eodatasets3/wagl.py) packagers.

## Open Data Cube compatibility

The assembler writes a format called "eo3", which will be the native metadata format for Open Data Cube
2.0. We recommend new products are written with this format, even if targeting Open Data Cube 1.
Datacube versions from 1.8 onwards are compatible natively with eo3.

eo3 adds information about the native grid of the data, and aims to be more easily interoperable 
with the upcoming [Stac Item metadata](https://github.com/radiantearth/stac-spec/tree/master/item-spec).

## Validator


`eo3-validate` a lint-like checker to check eo3 metadata.

     $ eo3-validate --help
    Usage: eo3-validate [OPTIONS] [PATHS]...
    
      Validate ODC dataset documents
    
      Paths can be both product and dataset documents, but each product must
      come before its datasets to be matched against it.
    
    Options:
      -W, --warnings-as-errors  Fail if any warnings are produced
      --thorough                Attempt to read the data/measurements, and check
                                their properties match the product
      -q, --quiet               Only print problems, one per line
      --help                    Show this message and exit.

## Conversion to Stac metadata

`eo3-to-stac`: Convert an ODC metadata to a Stac Item json file (BETA/Incomplete)

     $ eo3-to-stac --help
    Usage: eo3-to-stac [OPTIONS] [ODC_METADATA_FILES]...
    
      Convert a new-style ODC metadata doc to a Stac Item.
    
    Options:
      -u, --stac-base-url TEXT      Base URL of the STAC file
      -e, --explorer-base-url TEXT  Base URL of the ODC Explorer
      --validate / --no-validate    Flag it for stac document validation. By
                                    default flagged
      --help  Show this message and exit.


# Development

Run the tests using [pytest](http://pytest.org/).

    pytest

All code is formatted using [black](https://github.com/ambv/black), and checked
with [pyflakes](https://github.com/PyCQA/pyflakes).

They are included when installing the test dependencies:

    pip install -e .[test]

You may want to configure your editor to run black automatically on file save
(see the Black page for directions), or install the pre-commit hook within Git:

## Pre-commit setup

A [pre-commit](https://pre-commit.com/) config is provided to automatically format
and check your code changes. This allows you to immediately catch and fix
issues before you raise a failing pull request (which run the same checks under
Travis).

If you don't use Conda, install pre-commit from pip:

    pip install pre-commit

If you do use Conda, install from conda-forge (*required* because the pip
version uses virtualenvs which are incompatible with Conda's environments)

    conda install pre_commit

Now install the pre-commit hook to the current repository:

    pre-commit install

Your code will now be formatted and validated before each commit. You can also
invoke it manually by running `pre-commit run`


# DEA Prep

Some included scripts to prepare existing DEA products.

`eo3-prepare`: Prepare ODC metadata from the commandline.

Some preparers need the ancillary dependencies: `pip install .[ancillary]`

     $ eo3-prepare --help
    Usage: eo3-prepare [OPTIONS] COMMAND [ARGS]...
    
    Options:
      --version  Show the version and exit.
      --help     Show this message and exit.
    
    Commands:
      landsat-l1     Prepare eo3 metadata for USGS Landsat Level 1 data.
      modis-mcd43a1  Prepare MODIS MCD43A1 tiles for indexing into a Data...
      noaa-prwtr     Prepare NCEP/NCAR reanalysis 1 water pressure datasets...
      s2-awspds      Preparation code for Sentinel-2 L1C AWS PDS Generates...
      s2-cophub      Preparation code for Sentinel-2 L1C SCIHUB ZIP Generates...

`eo3-package-wagl`: Convert and package WAGL HDF5 outputs.

 Needs the wagl dependencies group: `pip install .[wagl]`
     
     $ eo3-package-wagl --help
    Usage: eo3-package-wagl [OPTIONS] H5_FILE
    
      Package WAGL HDF5 Outputs
    
      This will convert the HDF5 file (and sibling fmask/gqa files) into
      GeoTIFFS (COGs) with datacube metadata using the DEA naming conventions
      for files.
    
    Options:
      --level1 FILE                   Optional path to the input level1 metadata
                                      doc (otherwise it will be loaded from the
                                      level1 path in the HDF5)
      --output DIRECTORY              Put the output package into this directory
                                      [required]
      -p, --product [nbar|nbart|lambertian|sbt]
                                      Package only the given products (can specify
                                      multiple times)
      --with-oa / --no-oa             Include observation attributes (default:
                                      true)
      --help                          Show this message and exit.


## Creating Releases

```
git fetch origin

# Create a tag for the new version
git tag eodatasets3-<version> origin/eodatasets3

# Push it to main repository
git push origin --tags

# Create a wheel locally
python3 setup.py sdist bdist_wheel

# Upload it (Jeremy, Damien, Kirill have pypi ownership) 
python3 -m twine upload  dist/*

```
