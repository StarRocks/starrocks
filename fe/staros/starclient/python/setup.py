# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Setuptools/distutils extension for generating Python protobuf code."""

from setuptools import setup

setup(
    name='starclient',
    version='0.2.4',
    maintainer='sr@starrocks.com',
    packages=['starclient', 'starclient.grpc_gen'],
    maintainer_email='sr@starrocks.com',
    license='Apache License, Version 2.0',
    description=('This is a distutils extension to generate Python code for '
                 '.proto files using an installed protoc binary.'),
    long_description='star client python variant',
    url='https://github.com/StarRocks/starrocks',
    install_requires=['grpcio'],
    entry_points={
        'console_scripts': ['starclient=starclient.command_line:main'],
    }
)
