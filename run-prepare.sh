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

python3 ./unipipeline/main.py --config-file ./example/dag.yml --verbose=yes check --create
python3 ./unipipeline/main.py --config-file ./example/dag.yml --verbose=yes init

rm -rf ./build

rm -rf ./dist

python3 setup.py sdist bdist_wheel

echo "EVERYTHING IS OK"
