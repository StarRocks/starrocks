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

"""Unit tests for comparing a model's secondary index with the database's, by type, comment and properties."""

from typing import Any

from sqlalchemy import Column, Index, MetaData, Table, testing
from sqlalchemy.testing import eq_, fixtures

from starrocks.alembic.starrocks import StarRocksImpl
from starrocks.datatype import INTEGER, VARCHAR
from starrocks.dialect import StarRocksDialect


def _index(**options: Any) -> Index:
    table = Table("t", MetaData(), Column("id", INTEGER), Column("s", VARCHAR(50)))
    return Index("ix_s", table.c.s, **options)


def _impl() -> StarRocksImpl:
    return StarRocksImpl(
        dialect=StarRocksDialect(), connection=None, as_sql=False, transactional_ddl=False, output_buffer=None,
        context_opts={},
    )


NGRAM_DEFAULTS = {"gram_num": "4", "bf_size": "256", "case_sensitive": "true"}


class CompareIndexesTest(fixtures.TestBase):
    @testing.combinations(
        ({}, {"starrocks_using": "BITMAP"}),
        ({}, {"starrocks_using": "NGRAMBF", "starrocks_comment": "a"}),
        ({"starrocks_using": "NGRAMBF"}, {"starrocks_using": "NGRAMBF", "starrocks_comment": "a"}),
        ({"starrocks_using": "bitmap"}, {"starrocks_using": "BITMAP"}),
        ({"starrocks_comment": "a"}, {"starrocks_using": "BITMAP", "starrocks_comment": "a"}),
        (
            {"starrocks_using": "NGRAMBF"},
            {"starrocks_using": "NGRAMBF", "starrocks_properties": NGRAM_DEFAULTS},
        ),
        (
            {"starrocks_using": "NGRAMBF", "starrocks_properties": {"GRAM_NUM": "4", "case_sensitive": "TRUE"}},
            {"starrocks_using": "NGRAMBF", "starrocks_properties": NGRAM_DEFAULTS},
        ),
        argnames="model,database",
    )
    def test_an_index_with_the_same_options_is_equal(self, model, database):
        """StarRocks reports BITMAP for an index with no type, and every property an index defaulted."""
        assert _impl().compare_indexes(_index(**model), _index(**database)).is_equal

    @testing.combinations(
        ("type", {"starrocks_using": "BITMAP"}, {"starrocks_using": "NGRAMBF"}),
        ("comment", {"starrocks_using": "BITMAP", "starrocks_comment": "b"}, {"starrocks_using": "BITMAP", "starrocks_comment": "a"}),
        ("comment_removed", {"starrocks_using": "BITMAP", "starrocks_comment": ""}, {"starrocks_using": "BITMAP", "starrocks_comment": "a"}),
        (
            "property",
            {"starrocks_using": "NGRAMBF", "starrocks_properties": {"gram_num": "3"}},
            {"starrocks_using": "NGRAMBF", "starrocks_properties": NGRAM_DEFAULTS},
        ),
        (
            "property_unreported",
            {"starrocks_using": "NGRAMBF", "starrocks_properties": {"other": "1"}},
            {"starrocks_using": "NGRAMBF", "starrocks_properties": NGRAM_DEFAULTS},
        ),
        argnames="what,model,database",
    )
    def test_a_changed_type_comment_or_property_is_a_difference(self, what, model, database):
        result = _impl().compare_indexes(_index(**model), _index(**database))
        assert result.is_different
        assert what.split("_")[0] in result.message

    def test_the_name_and_columns_are_still_compared(self):
        table = Table("t", MetaData(), Column("id", INTEGER), Column("s", VARCHAR(50)))
        other = Index("ix_s", table.c.id)
        result = _impl().compare_indexes(Index("ix_s", table.c.s), other)
        assert result.is_different
        eq_("type" in result.message, False)
