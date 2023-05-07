## EO Datasets

[![Linting](https://github.com/GeoscienceAustralia/eo3/actions/workflows/lint.yml/badge.svg)](https://github.com/GeoscienceAustralia/eo3/actions/workflows/lint.yml)
[![Tests](https://github.com/GeoscienceAustralia/eo3/actions/workflows/test.yml/badge.svg)](https://github.com/GeoscienceAustralia/eo3/actions/workflows/test.yml)
<!--[![Coverage Status](https://img.shields.io/codecov/c/github/GeoscienceAustralia/eo-datasets)](https://app.codecov.io/gh/GeoscienceAustralia/eo-datasets)-->

This package contains:

1. A [draft specification](SPECIFICATION.md) for the EO3 format.
2. Extensible libraries to easily read, write, validate and convert [ODC](https://github.com/opendatacube/datacube-core)
datasets.

This package is designed to ignore all metadata except that directly interpreted by
[ODC](https://github.com/opendatacube/datacube-core)

Other metadata validation, creation, conversion, etc. can be handled by
[eo-datasets](https://github.com/opendatacube/eo-datasets), from which this
package was originally forked.

## Installation

    git clone git@github.com:opendatacube/eo3.git
    pip install -e ./eo3

    # optional
    pip istall eo-datasets

Python 3.8+ is supported.
