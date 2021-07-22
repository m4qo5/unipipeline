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

echo "MYPY package"
mypy ./unipipeline

echo "MYPY example"
mypy ./example

FLAKE_IGNORE="D100,D101,D102,D103,D104,D107,D105,D106,D200,D400,D413,E501,SF01,T484,W503,E402,N815,N805"

echo "FLAKE package"
flake8 --max-line-length=120 --ignore=$FLAKE_IGNORE ./unipipeline

echo "FLAKE example"
flake8 --max-line-length=120 --ignore=$FLAKE_IGNORE ./example

echo "PYTEST package"
pytest ./unipipeline --strict-markers

echo "EVERYTHING IS OK"
