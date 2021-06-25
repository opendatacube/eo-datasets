#!/usr/bin/env bash
# Convenience script for running just the tests

set -eu
set -x

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
pytest --cov eodatasets3 --cov-report=xml --durations=5 "${script_dir}" $@

# Run sphinx inline tests
#
# Unfortunately the pytest sphinx plugin doesn't work well
# with multiple test blocks, so we run the genuine sphinx
# tester separately instead. It will not count in coverage.
pushd "${script_dir}/docs"
    make doctest
popd
