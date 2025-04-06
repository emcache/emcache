#!/bin/bash
set -eou

cd /io/vendor/murmur3
make static
cd /io

pip install setuptools -U
make install-dev
pip uninstall -y emcache

pip install test_wheel/emcache-*.whl
make unit
