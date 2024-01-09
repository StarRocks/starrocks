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

import os
import sys

if sys.version_info < (3, 7) or sys.version_info >= (3, 12):
    print("Error: dbt-starrocks does not support this version of Python.")
    print("Please install Python 3.7 or higher but less than 3.12.")
    sys.exit(1)

from setuptools import find_namespace_packages, setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print(
        'Please upgrade setuptools with "pip install --upgrade setuptools" '
        "and try again"
    )
    sys.exit(1)

# pull long description from README
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()

package_name = "dbt-starrocks"
# make sure this always matches dbt/adapters/starrocks/__version__.py
package_version = "1.4.1"
description = """The Starrocks adapter plugin for dbt"""


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="fujianhj, long2ice, astralidea",
    author_email="fujianhj@gmail.com, long2ice@gmail.com, astralidea@163.com",
    url="https://github.com/StarRocks/starrocks/tree/main/contrib/dbt-connector",
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    include_package_data=True,
    install_requires=[
        "dbt-core==1.6.2",
        "mysql-connector-python>=8.1",
    ],
    zip_safe=False,
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7,<3.12",
)
