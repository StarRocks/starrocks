#! /usr/bin/python3
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
import os
import re
import sys

from setuptools import find_packages
from setuptools import setup

v = open(
    os.path.join(
        os.path.dirname(os.path.realpath(sys.argv[0])), "sqlalchemy-starrocks", "__init__.py"
    )
)
VERSION = re.compile(r".*__version__ = \"(.*?)\"", re.S).match(v.read()).group(1)
v.close()


setup(
    name="sqlalchemy-starrocks",
    version=VERSION,
    description="StarRocks Dialect for SQLAlchemy",
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: StarRocks License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database :: Front-Ends",
    ],
    install_requires=["sqlalchemy"],
    tests_require=[],
    keywords="db database analytics starrocks superset sqlalchemy dialect",
    author="fujianhj",
    author_email="fujianhj@gmail.com",
    url="https://github.com/StarRocks/starrocks/contrib/sqlalchemy-connector",
    license="StarRocks",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'sqlalchemy.dialects': [
            'starrocks = sqlalchemy_starrocks.dialect:StarRocksDialect',
            'sr = sqlalchemy_starrocks.dialect:StarRocksDialect',
        ]
    },
)
