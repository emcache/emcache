#!/bin/bash
set -eou

# Install the wheel exactly as published; do not rebuild the source package here.
# `make install-dev` runs `make clean`, which deletes dist/ on the host.
pip install --upgrade pip
pip install pytest==8.2.2 pytest-mock==3.14.0 pytest-asyncio==0.23.8

WHEEL=$(find /io/wheelhouse -maxdepth 1 -name 'emcache-*.whl' -print -quit)
pip install "$WHEEL"

# Run tests outside the source tree to test the installed wheel.
cd /tmp
python -m pytest -sv --import-mode=importlib -p no:cacheprovider /io/tests/unit
