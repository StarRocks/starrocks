#! /usr/bin/python3
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

from dbt.adapters.starrocks.connections import StarRocksCredentials
from dbt.adapters.starrocks.impl import StarRocksAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import starrocks


Plugin = AdapterPlugin(
    adapter=StarRocksAdapter,
    credentials=StarRocksCredentials,
    include_path=starrocks.PACKAGE_PATH)
