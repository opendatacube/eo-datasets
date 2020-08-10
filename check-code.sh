#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

flake8 -j 2 .
black --check .

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
pytest --cov eodatasets3 --durations=5 . $@

