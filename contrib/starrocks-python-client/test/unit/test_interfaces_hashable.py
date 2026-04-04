"""Tests that reflection dataclasses are hashable and safe for use as dict keys."""
import pytest

from starrocks.engine.interfaces import (
    ReflectedDistributionInfo,
    ReflectedPartitionInfo,
    ReflectedRefreshInfo,
    ReflectedTableKeyInfo,
)


class TestReflectedRefreshInfoHashable:
    def test_usable_as_dict_key(self):
        info = ReflectedRefreshInfo(moment="ASYNC", type="FULL")
        d = {info: "value"}
        assert d[info] == "value"

    def test_hash_stable_after_str(self):
        info = ReflectedRefreshInfo(moment="ASYNC", type="FULL")
        h_before = hash(info)
        str(info)
        assert hash(info) == h_before


class TestReflectedTableKeyInfoHashable:
    def test_usable_as_dict_key(self):
        info = ReflectedTableKeyInfo(type="PRIMARY KEY", columns="id")
        d = {info: "value"}
        assert d[info] == "value"

    def test_usable_as_dict_key_with_list_columns(self):
        info = ReflectedTableKeyInfo(type="PRIMARY KEY", columns=["id", "name"])
        d = {info: "value"}
        assert d[info] == "value"

    def test_hash_stable_after_str(self):
        info = ReflectedTableKeyInfo(type="primary key", columns=" id ")
        h_before = hash(info)
        str(info)
        assert hash(info) == h_before

    def test_hash_stable_with_list_columns(self):
        info = ReflectedTableKeyInfo(type="PRIMARY KEY", columns=["id", "name"])
        h_before = hash(info)
        str(info)
        assert hash(info) == h_before

    def test_str_does_not_mutate_fields(self):
        info = ReflectedTableKeyInfo(type="primary key", columns=" id ")
        str(info)
        assert info.type == "primary key"
        assert info.columns == " id "


class TestReflectedPartitionInfoHashable:
    def test_usable_as_dict_key(self):
        info = ReflectedPartitionInfo(type="RANGE", partition_method="RANGE(dt)")
        d = {info: "value"}
        assert d[info] == "value"

    def test_hash_stable_after_str(self):
        info = ReflectedPartitionInfo(type="range", partition_method=" RANGE(dt) ")
        h_before = hash(info)
        str(info)
        assert hash(info) == h_before

    def test_str_does_not_mutate_fields(self):
        info = ReflectedPartitionInfo(type="range", partition_method=" RANGE(dt) ")
        str(info)
        assert info.type == "range"
        assert info.partition_method == " RANGE(dt) "


class TestReflectedDistributionInfoHashable:
    def test_usable_as_dict_key(self):
        info = ReflectedDistributionInfo(
            type="HASH", columns="id", distribution_method="HASH(id)", buckets=4
        )
        d = {info: "value"}
        assert d[info] == "value"

    def test_usable_as_dict_key_with_list_columns(self):
        info = ReflectedDistributionInfo(
            type="HASH", columns=["id", "name"], distribution_method=None, buckets=4
        )
        d = {info: "value"}
        assert d[info] == "value"

    def test_hash_stable_after_str(self):
        info = ReflectedDistributionInfo(
            type="HASH", columns="id", distribution_method=None, buckets=4
        )
        h_before = hash(info)
        str(info)
        assert hash(info) == h_before

    def test_hash_stable_with_list_columns(self):
        info = ReflectedDistributionInfo(
            type="HASH", columns=["id", "name"], distribution_method=None, buckets=4
        )
        h_before = hash(info)
        str(info)
        assert hash(info) == h_before

    def test_str_does_not_mutate_fields(self):
        info = ReflectedDistributionInfo(
            type="HASH", columns="id", distribution_method=None, buckets=4
        )
        str(info)
        assert info.distribution_method is None

    def test_usable_in_set(self):
        info1 = ReflectedDistributionInfo(
            type="HASH", columns="id", distribution_method="HASH(id)", buckets=4
        )
        info2 = ReflectedDistributionInfo(
            type="HASH", columns="id", distribution_method="HASH(id)", buckets=4
        )
        s = {info1, info2}
        assert len(s) == 1

    def test_usable_in_set_with_list_columns(self):
        info1 = ReflectedDistributionInfo(
            type="HASH", columns=["id", "name"], distribution_method=None, buckets=4
        )
        info2 = ReflectedDistributionInfo(
            type="HASH", columns=["id", "name"], distribution_method=None, buckets=4
        )
        s = {info1, info2}
        assert len(s) == 1
