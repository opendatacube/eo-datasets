#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

flake8 -j 2 eodatasets3
black --check eodatasets3 tests

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
py.test --cov eodatasets3 --durations=5 eodatasets3 tests $@

