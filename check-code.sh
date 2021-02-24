#!/usr/bin/env bash
# Convenience script for running CI-like checks

set -eu
set -x

pre-commit run -a

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
pytest --cov eodatasets3 --durations=5 . $@

