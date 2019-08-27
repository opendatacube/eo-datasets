#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

shopt -s globstar

flake8 -j 2 ./**/*.py
black --check ./**/*.py

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
pytest --cov eodatasets3 --durations=5 ./**/*.py $@

