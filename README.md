## Earth Observation Datasets

[![Build Status](https://travis-ci.org/GeoscienceAustralia/eo-datasets.svg?branch=develop)](https://travis-ci.org/GeoscienceAustralia/eo-datasets)
[![Coverage Status](https://coveralls.io/repos/GeoscienceAustralia/eo-datasets/badge.svg?branch=develop)](https://coveralls.io/r/GeoscienceAustralia/eo-datasets?branch=develop)

Packaging, metadata and provenance libraries for GA EO datasets. See [LICENSE](LICENSE) for
license details.

### Installation

    python ./setup.py install

Python 3.5+ is supported. A [GDAL](http://www.gdal.org/) installation is required 
to use most packaging commands. Modis packaging requires [pdsinfo](https://github.com/GeoscienceAustralia/pds-tools)
to be on the path.

### Tests

Run tests using [pytest](http://pytest.org/).

    py.test eodatasets tests

Integration tests will not run by default (and are not included in reported
[test coverage](https://coveralls.io/r/GeoscienceAustralia/eo-datasets)). Include the `--runslow`
parameter to run all tests.

    py.test --runslow eodatasets tests

### Included Scripts

`eod-package`: Package a dataset from the commandline.

    $ eod-package --help
    Usage: package.py [OPTIONS] TYPE [DATASET]... DESTINATION
    
      Package the given imagery folders.
    
    Options:
      --parent PATH  Path of the parent dataset (that these datasets were derived
                     from.)
      --debug        Enable debug logging
      --hard-link    Hard-link output files if possible (faster than copying)
      --help         Show this message and exit.
    $

Create a series of ortho packages with provenance linked to a (parent) raw dataset:

     $ eod-package level1 --parent /data/packages/LS8_OLITIRS_STD-MD_P00... \
              lpgs_out/* \
              /data/packages/   
