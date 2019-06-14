#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

pycodestyle tests --max-line-length 120

# pylint -j 2 --reports no eodatasets2
flake8 -j 2 eodatasets2

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
py.test --cov eodatasets2 --durations=5 eodatasets2 tests $@

