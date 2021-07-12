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

pytest ./unipipeline --strict-markers

mypy ./unipipeline ./example

FLAKE_IGNORE="D100,D101,D102,D103,D104,D107,D105,D106,D200,D400,D413,E501,SF01,T484,W503,E402,N815,N805"


flake8 --max-line-length=120 --ignore=$FLAKE_IGNORE ./unipipeline

flake8 --max-line-length=120 --ignore=$FLAKE_IGNORE ./example

python3 ./unipipeline/main.py --config-file ./example/dag.yml --verbose=yes scaffold
python3 ./unipipeline/main.py --config-file ./example/dag.yml --verbose=yes check

rm -rf ./build

rm -rf ./dist

python3 setup.py sdist bdist_wheel

echo "EVERYTHING IS OK"
