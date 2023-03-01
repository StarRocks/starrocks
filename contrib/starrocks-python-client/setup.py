#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ast
import re

from setuptools import find_packages, setup

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("starrocks/sqlalchemy/__init__.py", "rb") as f:
    python_client_version = _version_re.search(f.read().decode("utf-8"))
    assert python_client_version is not None
    version = str(ast.literal_eval(python_client_version.group(1)))

setup(
    name="starrocks",
    version=version,
    description="StarRocks Dialect for SQLAlchemy",
    author="StarRocks Team",
    url="https://github.com/StarRocks/starrocks",
    license="Apache 2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database :: Front-Ends",
    ],
    install_requires=[
        "sqlalchemy>=1.4, <2",
        "sqlalchemy-utils>=0.38.3, <0.39",
    ],
    packages=find_packages(include=["starrocks", "starrocks.*"]),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'sqlalchemy.dialects': [
            'starrocks = starrocks.sqlalchemy.dialect:StarRocksDialect',
        ]
    },
)
