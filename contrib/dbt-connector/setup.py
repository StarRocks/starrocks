#! /usr/bin/python3
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

from setuptools import find_namespace_packages, setup

package_name = "dbt-starrocks"
# make sure this always matches dbt/adapters/starrocks/__version__.py
package_version = "1.0.0"
description = """The starrocks adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="fujianhj, long2ice",
    author_email="fujianhj@gmail.com, long2ice@gmail.com",
    url="",
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    include_package_data=True,
    install_requires=[
        "dbt-core==1.1.0",
        "mysql-connector-python>=8.0.0,<8.1",
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
    ],
    python_requires=">=3.7,<3.10",
)
