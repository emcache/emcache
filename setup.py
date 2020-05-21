import os
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
    "Cython==0.29.18",
    "pytest==5.4.1",
    "pytest-mock==3.1.0",
    "pytest-asyncio==0.11.0",
    "uvloop==0.14.0",
    "asynctest==0.13.0",
    "pytest-cov==2.8.1",
    "black==19.10b0",
    "isort==4.3.21",
    "flake8==3.7.9",
]

doc_requires = [
    "Sphinx==3.0.3"
]

setup(
    version="0.1.0b",
    name="emcache",
    description="A high performance asynchronous Python client for Memcached with full batteries included",
    url="http://github.com/pfreixes/emcache",
    author="Pau Freixes",
    author_email="pfreixes@gmail.com",
    platforms=["*nix"],
    packages=["emcache"],
    ext_modules=extensions,
    extras_require={"dev": dev_requires, "doc": doc_requires},
)
