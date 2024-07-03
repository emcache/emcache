#!/bin/bash

set -x

PYTHON=/usr/local/bin/python$PYTHON_VERSION

$PYTHON -m venv /$PYTHON_VERSION
source /$PYTHON_VERSION/bin/activate

cd /io

pacman -Syu base-devel linux-headers

pushd vendor/murmur3
make static
popd

python -m pip install --upgrade pip
python -m pip install auditwheel cython setuptools build -U
make compile
python -m build
auditwheel repair dist/emcache-*.whl -w dist
rm dist/emcache-*-linux*
