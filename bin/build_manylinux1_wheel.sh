#!/bin/bash

if [ $PYTHON_VERSION == "3.8" ]
then
    PYTHON=/opt/python/cp38-cp38/bin/python
    PIP=/opt/python/cp38-cp38/bin/pip
    CYTHON=/opt/python/cp38-cp38/bin/cython
    AUDITWHEEL=/opt/python/cp38-cp38/bin/auditwheel
elif [ $PYTHON_VERSION == "3.9" ]
then
    PYTHON=/opt/python/cp39-cp39/bin/python
    PIP=/opt/python/cp39-cp39/bin/pip
    CYTHON=/opt/python/cp39-cp39/bin/cython
    AUDITWHEEL=/opt/python/cp39-cp39/bin/auditwheel
elif [ $PYTHON_VERSION == "3.10" ]
then
    PYTHON=/opt/python/cp310-cp310/bin/python
    PIP=/opt/python/cp310-cp310/bin/pip
    CYTHON=/opt/python/cp310-cp310/bin/cython
    AUDITWHEEL=/opt/python/cp310-cp310/bin/auditwheel
elif [ $PYTHON_VERSION == "3.11" ]
then
    PYTHON=/opt/python/cp311-cp311/bin/python
    PIP=/opt/python/cp311-cp311/bin/pip
    CYTHON=/opt/python/cp311-cp311/bin/cython
    AUDITWHEEL=/opt/python/cp311-cp311/bin/auditwheel
else
    exit 1
fi

cd /io/vendor/murmur3
make static
cd /io
${PYTHON} -m pip install --upgrade pip
${PIP} install Cython
${PIP} install auditwheel
PYTHON=${PYTHON} PIP=${PIP} CYTHON=${CYTHON} make compile
${PYTHON} setup.py bdist_wheel
${AUDITWHEEL} repair dist/emcache-*.whl -w dist
rm dist/emcache-*-linux*
