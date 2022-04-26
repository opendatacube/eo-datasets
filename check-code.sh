#!/usr/bin/env bash
# Convenience script for running all CI-like checks

set -eu
set -x

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd script_dir

pre-commit run -a

./run-tests.sh $@
