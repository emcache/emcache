#!/bin/bash

set -x

PYTHON=/usr/local/bin/python$PYTHON_VERSION

$PYTHON -m venv /$PYTHON_VERSION
source /$PYTHON_VERSION/bin/activate

cd /io
git config --global --add safe.directory /io

cd /io/vendor/murmur3
make static
cd /io

python -m pip install -U pip
python -m pip install auditwheel Cython setuptools wheel -U
make compile
python setup.py bdist_wheel
auditwheel repair dist/emcache-*.whl -w dist
rm dist/emcache-*-linux*
