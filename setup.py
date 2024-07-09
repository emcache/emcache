# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import os
import sys

from setuptools import Extension, setup

if sys.platform in ("win32", "cygwin", "cli"):
    raise RuntimeError("emcache does not support Windows at the moment")

MURMUR3_DIR = os.path.join("vendor", "murmur3")

extensions = [
    Extension(
        "emcache._cython.cyemcache",
        sources=["emcache/_cython/cyemcache.c"],
        include_dirs=[MURMUR3_DIR],
        library_dirs=[MURMUR3_DIR],
        libraries=["murmur3"],
    )
]

setup(ext_modules=extensions)
