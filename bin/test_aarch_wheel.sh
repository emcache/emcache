#!/bin/bash
set -eou
cd /io

cd vendor/murmur3
make static
cd /io

pip install --upgrade pip
pip install setuptools wheel Cython
make install-dev
pip uninstall -y emcache

pip install test_wheel/emcache-*.whl
make unit
