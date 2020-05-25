#!/bin/bash

if [ $PYTHON_VERSION == "3.7" ]
then
    PYTHON=/opt/python/cp37-cp37m/bin/python
    PIP=/opt/python/cp37-cp37m/bin/pip
    CYTHON=/opt/python/cp37-cp37m/bin/cython
    AUDITWHEEL=/opt/python/cp37-cp37m/bin/auditwheel
elif [ $PYTHON_VERSION == "3.8" ]
then
    PYTHON=/opt/python/cp38-cp38/bin/python
    PIP=/opt/python/cp38-cp38/bin/pip
    CYTHON=/opt/python/cp38-cp38/bin/cython
    AUDITWHEEL=/opt/python/cp38-cp38/bin/auditwheel
else
    exit 1
fi

cd /io/vendor/murmur3
make static
cd /io
${PYTHON} -m pip install --upgrade pip
${PIP} install auditwheel
PYTHON=${PYTHON} PIP=${PIP} CYTHON=${CYTHON} make compile
${PYTHON} setup.py sdist bdist_wheel
${AUDITWHEEL} repair dist/emcache-*.whl -w dist
rm dist/emcache-*-linux*
