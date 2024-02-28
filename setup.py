# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import os
import re
import sys

from setuptools import Extension, setup

if sys.platform in ("win32", "cygwin", "cli"):
    raise RuntimeError("emcache does not support Windows at the moment")

vi = sys.version_info
if vi < (3, 7):
    raise RuntimeError("emcache requires Python 3.7 or greater")

MURMUR3_DIR = os.path.join(os.path.dirname(__file__), "vendor", "murmur3")

extensions = [
    Extension(
        "emcache._cython.cyemcache",
        sources=["emcache/_cython/cyemcache.c"],
        include_dirs=[MURMUR3_DIR],
        library_dirs=[MURMUR3_DIR],
        libraries=["murmur3"],
    )
]

dev_requires = [
    "Cython==0.29.32",
    "pytest==6.2.5",
    "pytest-mock==3.1.0",
    "pytest-asyncio==0.11.0",
    "pytest-cov==2.8.1",
    "black==22.3.0",
    "isort==4.3.21",
    "flake8==6.1.0",
]


def get_version():
    with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "emcache/version.py")) as fp:
        try:
            return re.findall(r"^__version__ = \"([^']+)\"\r?$", fp.read())[0]
        except IndexError:
            raise RuntimeError("Unable to determine version.")


with open(os.path.join(os.path.dirname(__file__), "README.rst")) as f:
    readme = f.read()

setup(
    version=get_version(),
    name="emcache",
    description="A high performance asynchronous Python client for Memcached with full batteries included",
    long_description=readme,
    url="http://github.com/emcache/emcache",
    author="Pau Freixes",
    author_email="pfreixes@gmail.com",
    platforms=["*nix"],
    packages=["emcache"],
    package_data={"emcache": ["py.typed"]},
    ext_modules=extensions,
    extras_require={"dev": dev_requires},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
    ],
)
