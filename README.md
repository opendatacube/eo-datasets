## Earth Observation Datasets

[![Build Status](https://travis-ci.org/GeoscienceAustralia/eo-datasets.svg?branch=develop)](https://travis-ci.org/GeoscienceAustralia/eo-datasets)
[![Coverage Status](https://coveralls.io/repos/GeoscienceAustralia/eo-datasets/badge.svg?branch=develop)](https://coveralls.io/r/GeoscienceAustralia/eo-datasets?branch=develop)

Packaging, metadata and provenance libraries for GA EO datasets. See [LICENSE](LICENSE) for
license details.

### Installation

    pip install -e .

Python 3.6+ is supported. A [GDAL](http://www.gdal.org/) installation is required 
to use most packaging commands.

### Tests

Run tests using [pytest](http://pytest.org/).

    pytest

### Included Scripts

`eo2-validate` a lint-like checker to check ODC metadata.

     $ eo2-validate --help
    Usage: eo2-validate [OPTIONS] [PATHS]...
    
      Validate an ODC document
    
    Options:
      -W, --warnings-as-errors  Fail if any warnings are produced
      -q, --quiet               Only print problems, one per line
      --help                    Show this message and exit.

`eo2-prepare`: Prepare ODC metadata from the commandline.

     $ eo2-prepare --help
    Usage: eo2-prepare [OPTIONS] COMMAND [ARGS]...
    
    Options:
      --version  Show the version and exit.
      --help     Show this message and exit.
    
    Commands:
      ls-usgs        Prepare USGS Landsat Collection 1 data for ingestion
                     into...
      modis-mcd43a1  Prepare MODIS MCD43A1 tiles for indexing into a Data...
      noaa-prwtr     Prepare NCEP/NCAR reanalysis 1 water pressure datasets...
      s2-awspds      Preparation code for Sentinel-2 L1C AWS PDS Generates...
      s2-cophub      Preparation code for Sentinel-2 L1C SCIHUB ZIP Generates...

`eo2-package-wagl`: Convert and package WAGL HDF5 outputs.

    $ eo2-package-wagl --help
    Usage: eo2-package-wagl [OPTIONS] H5_FILE
    
      Package WAGL HDF5 Outputs
    
      This will convert the HDF5 file (and sibling fmask/gqa files) into
      GeoTIFFS (COGs) with datacube metadata using the DEA naming conventions
      for files.
    
    Options:
      --level1 FILE                   the path to the input level1 metadata doc
                                      [required]
      --output DIRECTORY              Put the output package into this directory
                                      [required]
      -p, --product [nbar|nbart|lambertian|sbt]
                                      Package only the given products (can specify
                                      multiple times)
      --with-oa / --no-oa             Include observation attributes (default:
                                      true)
      --help                          Show this message and exit

# Metadata creation API

_TODO: Write an overview._ 

See an example in [tests/integration/test_assemble.py](tests/integration/test_assemble.py)
