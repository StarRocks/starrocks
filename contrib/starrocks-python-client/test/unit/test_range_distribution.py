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

"""Unit tests for reflecting and recognizing RANGE distribution (StarRocks 4.1+)."""

from sqlalchemy import testing
from sqlalchemy.testing import fixtures

from starrocks.common.consts import TableConfigKey
from starrocks.common.utils import is_range_distribution
from starrocks.engine.interfaces import ReflectedDistributionInfo
from starrocks.reflection import StarRocksTableDefinitionParser


def _parser() -> StarRocksTableDefinitionParser:
    return StarRocksTableDefinitionParser.__new__(StarRocksTableDefinitionParser)


class IsRangeDistributionTest(fixtures.TestBase):
    @testing.combinations(
        ("RANGE", True),
        ("range", True),
        ("RANGE BUCKETS 1", True),
        ("  RANGE", True),
        ("RANDOM", False),
        ("RANDOM BUCKETS 4", False),
        ("HASH(range_col)", False),
        ("", False),
        (None, False),
        (ReflectedDistributionInfo(type="RANGE", columns=None, distribution_method=None, buckets=1), True),
        (ReflectedDistributionInfo(type="HASH", columns=["id"], distribution_method=None, buckets=8), False),
        argnames="value, expected",
    )
    def test_is_range_distribution(self, value, expected):
        assert is_range_distribution(value) is expected


class ReflectedRangeDistributionTest(fixtures.TestBase):
    @testing.combinations((None,), ("",), ("k1, k2",), argnames="distribute_key")
    @testing.combinations(("RANGE",), ("range",), argnames="distribute_type")
    def test_reflected_range_distribution_drops_buckets_and_key(self, distribute_type, distribute_key):
        """StarRocks reports a placeholder bucket count of 1 and no key for RANGE."""
        info = _parser()._get_distribution_info({
            TableConfigKey.DISTRIBUTE_TYPE: distribute_type,
            TableConfigKey.DISTRIBUTE_KEY: distribute_key,
            TableConfigKey.DISTRIBUTE_BUCKET: 1,
        })
        assert str(info) == "RANGE"

    def test_reflected_hash_distribution_unchanged(self):
        info = _parser()._get_distribution_info({
            TableConfigKey.DISTRIBUTE_TYPE: "HASH",
            TableConfigKey.DISTRIBUTE_KEY: "id",
            TableConfigKey.DISTRIBUTE_BUCKET: 8,
        })
        assert str(info) == "HASH(id) BUCKETS 8"
