# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tests that reflected dataclasses are hashable, which is required by
SQLAlchemy's reflection cache (sqlalchemy.engine.reflection.cache).

See: https://github.com/StarRocks/starrocks/issues/70733
"""

from starrocks.engine.interfaces import (
    ReflectedDistributionInfo,
    ReflectedPartitionInfo,
    ReflectedRefreshInfo,
    ReflectedTableKeyInfo,
)


class TestReflectedPartitionInfoHashable:

    def test_hashable(self):
        info = ReflectedPartitionInfo(type="RANGE", partition_method="RANGE(dt)")
        assert hash(info) == hash(info)

    def test_usable_as_dict_key(self):
        info = ReflectedPartitionInfo(type="RANGE", partition_method="RANGE(dt)")
        d = {info: "value"}
        assert d[info] == "value"

    def test_equal_instances_have_equal_hash(self):
        a = ReflectedPartitionInfo(type="RANGE", partition_method="RANGE(dt)")
        b = ReflectedPartitionInfo(type="RANGE", partition_method="RANGE(dt)")
        assert hash(a) == hash(b)

    def test_with_pre_created_partitions(self):
        info = ReflectedPartitionInfo(
            type="RANGE",
            partition_method="RANGE(dt)",
            pre_created_partitions="(PARTITION p1 VALUES LESS THAN ('2026-01-01'))",
        )
        assert hash(info) == hash(info)


class TestReflectedDistributionInfoHashable:

    def test_hashable(self):
        info = ReflectedDistributionInfo(
            type="HASH", columns=["id"], distribution_method="HASH(id)", buckets=4
        )
        assert hash(info) == hash(info)

    def test_usable_as_dict_key(self):
        info = ReflectedDistributionInfo(
            type="HASH", columns=["id"], distribution_method="HASH(id)", buckets=4
        )
        d = {info: "value"}
        assert d[info] == "value"

    def test_equal_instances_have_equal_hash(self):
        a = ReflectedDistributionInfo(
            type="HASH", columns=["id"], distribution_method="HASH(id)", buckets=4
        )
        b = ReflectedDistributionInfo(
            type="HASH", columns=["id"], distribution_method="HASH(id)", buckets=4
        )
        assert hash(a) == hash(b)


class TestReflectedTableKeyInfoHashable:

    def test_hashable(self):
        info = ReflectedTableKeyInfo(type="PRIMARY KEY", columns="id, name")
        assert hash(info) == hash(info)

    def test_usable_as_dict_key(self):
        info = ReflectedTableKeyInfo(type="PRIMARY KEY", columns="id, name")
        d = {info: "value"}
        assert d[info] == "value"

    def test_equal_instances_have_equal_hash(self):
        a = ReflectedTableKeyInfo(type="PRIMARY KEY", columns="id, name")
        b = ReflectedTableKeyInfo(type="PRIMARY KEY", columns="id, name")
        assert hash(a) == hash(b)

    def test_hashable_with_list_columns(self):
        info = ReflectedTableKeyInfo(type="PRIMARY KEY", columns=["id", "name"])
        assert hash(info) == hash(info)

    def test_usable_as_dict_key_with_list_columns(self):
        info = ReflectedTableKeyInfo(type="PRIMARY KEY", columns=["id", "name"])
        d = {info: "value"}
        assert d[info] == "value"

    def test_equal_instances_with_list_columns_have_equal_hash(self):
        a = ReflectedTableKeyInfo(type="PRIMARY KEY", columns=["id", "name"])
        b = ReflectedTableKeyInfo(type="PRIMARY KEY", columns=["id", "name"])
        assert hash(a) == hash(b)


class TestReflectedRefreshInfoHashable:

    def test_hashable(self):
        info = ReflectedRefreshInfo(moment="ASYNC", type="FULL")
        assert hash(info) == hash(info)

    def test_usable_as_dict_key(self):
        info = ReflectedRefreshInfo(moment="ASYNC", type="FULL")
        d = {info: "value"}
        assert d[info] == "value"

    def test_equal_instances_have_equal_hash(self):
        a = ReflectedRefreshInfo(moment="ASYNC", type="FULL")
        b = ReflectedRefreshInfo(moment="ASYNC", type="FULL")
        assert hash(a) == hash(b)


class TestReflectedInfoUsableInCacheTuple:
    """
    Simulates SQLAlchemy's reflection cache behavior where reflected info
    objects end up as elements inside a tuple used as a dictionary key.
    """

    def test_tuple_with_partition_and_distribution_as_cache_key(self):
        partition = ReflectedPartitionInfo(type="RANGE", partition_method="RANGE(dt)")
        distribution = ReflectedDistributionInfo(
            type="HASH", columns=["id"], distribution_method="HASH(id)", buckets=4
        )
        key = ("schema", "table", partition, distribution)
        cache = {key: "cached_columns"}
        assert cache[key] == "cached_columns"
