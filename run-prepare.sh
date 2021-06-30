#!/bin/bash

echo ">>> $(basename ${BASH_SOURCE[0]})"

set -o errexit
set -o pipefail
set -o nounset



# INIT WORKING DIR
# ======================================================================================================
cd "$(dirname "${BASH_SOURCE[0]}")"
CWD="$(pwd)"


export PYTHONPATH=$CWD



mypy ./unipipeline ./example

python3 -m unipipeline.bin --config-file ./example/dag.yml --verbose yes check --create

rm -rf ./build

rm -rf ./dist

python3 setup.py sdist bdist_wheel

echo "EVERYTHING IS OK"
