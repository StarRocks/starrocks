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

"""Tests for performance optimizations and default value updates."""

from sqlalchemy import Boolean, Integer, testing
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from starrocks.common.defaults import ReflectionTableDefaults
from starrocks.common.utils import TableAttributeNormalizer, canonical_default_as_kept
from starrocks.reflection import StarRocksTableDefinitionParser


class RegexPatternTest(fixtures.TestBase):
    """Test performance optimizations for regex patterns."""

    def test_parse_distribution_clause_with_compiled_regex(self):
        """Test that _parse_distribution_string works with pre-compiled regex."""
        # Test with BUCKETS
        result = StarRocksTableDefinitionParser.parse_distribution_clause("HASH(id) BUCKETS 8")
        assert result.distribution_method == "HASH(id)"
        assert result.buckets == 8

        # Test without BUCKETS
        result = StarRocksTableDefinitionParser.parse_distribution_clause("HASH(id)")
        assert result.distribution_method == "HASH(id)"
        assert result.buckets is None

        # Test with different case
        result = StarRocksTableDefinitionParser.parse_distribution_clause("HASH(id) buckets 16")
        assert result.distribution_method == "HASH(id)"
        assert result.buckets == 16

        # Test with extra spaces
        result = StarRocksTableDefinitionParser.parse_distribution_clause("HASH(id)   BUCKETS   32")
        assert result.distribution_method == "HASH(id)"
        assert result.buckets == 32

        # Test empty string
        result = StarRocksTableDefinitionParser.parse_distribution_clause("")
        assert result is None

    def test_normalize_column_identifiers_with_compiled_regex(self):
        """Test that _normalize_column_identifiers works with pre-compiled regex."""
        # Test backticks removal
        result = TableAttributeNormalizer.normalize_column_identifiers("`id`, `name`, `created_at`")
        assert result == "id, name, created_at"

        # Test whitespace normalization
        result = TableAttributeNormalizer.normalize_column_identifiers("id   name    created_at")
        assert result == "id name created_at"

        # Test mixed backticks and whitespace
        result = TableAttributeNormalizer.normalize_column_identifiers("`id`   `name`    `created_at`")
        assert result == "id name created_at"

        # Test empty string
        result = TableAttributeNormalizer.normalize_column_identifiers("")
        assert result == ""

        # Test None
        result = TableAttributeNormalizer.normalize_column_identifiers(None)
        assert result is None

    def test_normalize_distributed_by(self):
        """Test distribution string normalization."""
        result = TableAttributeNormalizer.normalize_distribution_string("HASH(`id`)   BUCKETS 8")
        assert result == "HASH(id) BUCKETS 8"

        result = TableAttributeNormalizer.normalize_distribution_string("RANDOM   BUCKETS   16")
        assert result == "RANDOM BUCKETS 16"

    def test_normalize_order_by(self):
        """Test ORDER BY string normalization."""
        # Test string input
        result = TableAttributeNormalizer.normalize_order_by_string("`id` ASC, `name` DESC")
        assert result == "id ASC, name DESC"

        # Test list input
        result = TableAttributeNormalizer.normalize_order_by_string(["`id`", "`name`"])
        assert result == "id, name"

        # Test None input
        result = TableAttributeNormalizer.normalize_order_by_string(None)
        assert result is None


class DefaultValuesTest(fixtures.TestBase):
    """Test default value constants."""

    def test_engine_normalization(self):
        """Test engine normalization logic."""
        # Test None -> DEFAULT
        result = ReflectionTableDefaults.normalize_engine(None)
        assert result == ReflectionTableDefaults.engine()

        # Test empty string -> DEFAULT
        result = ReflectionTableDefaults.normalize_engine("")
        assert result == ReflectionTableDefaults.engine()

        # Test DEFAULT -> DEFAULT
        result = ReflectionTableDefaults.normalize_engine(ReflectionTableDefaults.engine())
        assert result == ReflectionTableDefaults.engine()

        # Test other engine -> unchanged
        result = ReflectionTableDefaults.normalize_engine("MYSQL")
        assert result == "MYSQL"


class PropertyValueNormalizationTest(fixtures.TestBase):
    @testing.combinations(
        ("name, k", "k, name"),
        ("`K`,`Name`", "k, name"),
        ("k,name,", "k,  name"),
        argnames="model_value,database_value",
    )
    def test_bloom_filter_columns_compare_in_any_order_and_case(self, model_value, database_value):
        eq_(
            ReflectionTableDefaults.normalize_property_value("bloom_filter_columns", model_value),
            ReflectionTableDefaults.normalize_property_value("bloom_filter_columns", database_value),
        )

    def test_another_bloom_filter_column_is_a_change(self):
        normalize = ReflectionTableDefaults.normalize_property_value
        assert normalize("bloom_filter_columns", "k, name") != normalize("bloom_filter_columns", "k, other")

    @testing.combinations(
        ("unique_constraints", "t.id", "default_catalog.db.t.id"),
        ("unique_constraints", "t.id,t.dt", "default_catalog.db.t.id,dt"),
        ("unique_constraints", "t.dt, t.id; u.id", "default_catalog.db.u.id;default_catalog.db.t.id,dt"),
        ("unique_constraints", "`T`.`id`", "default_catalog.db.t.id"),
        (
            "foreign_key_constraints",
            "t(id) REFERENCES u(id)",
            "default_catalog.db.t(id) REFERENCES default_catalog.db.u(id)",
        ),
        (
            "foreign_key_constraints",
            "t(a) REFERENCES u(b);t(c) references u(d)",
            "default_catalog.db.t(c) REFERENCES default_catalog.db.u(d);"
            " default_catalog.db.t(a) REFERENCES default_catalog.db.u(b)",
        ),
        argnames="key,model_value,database_value",
    )
    def test_constraints_compare_as_the_database_qualifies_them(self, key, model_value, database_value):
        eq_(
            ReflectionTableDefaults.normalize_property_value(key, model_value),
            ReflectionTableDefaults.normalize_property_value(key, database_value),
        )

    @testing.combinations(
        ("unique_constraints", "t.id", "t.dt"),
        ("unique_constraints", "t.id", "u.id"),
        ("unique_constraints", "t.id", "t.id,dt"),
        ("foreign_key_constraints", "t(id) REFERENCES u(id)", "t(id) REFERENCES v(id)"),
        argnames="key,model_value,database_value",
    )
    def test_another_constraint_is_a_change(self, key, model_value, database_value):
        normalize = ReflectionTableDefaults.normalize_property_value
        assert normalize(key, model_value) != normalize(key, database_value)

    @testing.combinations(
        ("excluded_trigger_tables", "t, u", "t,u"),
        ("excluded_trigger_tables", "u,t", "t,u"),
        ("excluded_refresh_tables", "db.u , `t`", "t,db.u"),
        argnames="key,model_value,database_value",
    )
    def test_table_lists_compare_in_any_order_and_spacing(self, key, model_value, database_value):
        eq_(
            ReflectionTableDefaults.normalize_property_value(key, model_value),
            ReflectionTableDefaults.normalize_property_value(key, database_value),
        )

    @testing.combinations(("t,u", "t,v"), ("t", "T"), argnames="model_value,database_value")
    def test_another_excluded_table_is_a_change(self, model_value, database_value):
        normalize = ReflectionTableDefaults.normalize_property_value
        assert normalize("excluded_trigger_tables", model_value) != normalize("excluded_trigger_tables", database_value)


class DefaultAsStarRocksKeepsItTest(fixtures.TestBase):
    """Defaults that StarRocks keeps as one value are equal, however they were written."""

    @testing.combinations(
        ("now_function", ("expression", "now()"), Integer(), True, ("expression", "current_timestamp")),
        ("current_timestamp", ("expression", "current_timestamp"), Integer(), True, ("expression", "current_timestamp")),
        ("current_timestamp_call", ("expression", "current_timestamp()"), Integer(), True, ("expression", "current_timestamp")),
        ("null_keyword", ("expression", "null"), Integer(), True, None),
        ("no_default", None, Integer(), True, None),
        ("another_function", ("expression", "uuid()"), Integer(), True, ("expression", "uuid()")),
        ("boolean_text", ("literal", "TRUE"), Boolean(), True, ("literal", "1")),
        ("boolean_text_kept_as_written", ("literal", "TRUE"), Boolean(), False, ("literal", "TRUE")),
        ("boolean_digit", ("literal", "0"), Boolean(), True, ("literal", "0")),
        ("text_for_another_type", ("literal", "true"), Integer(), True, ("literal", "true")),
        argnames="canonical, column_type, kept_as_digits, expected",
        id_="iaaaa",
    )
    def test_default_as_kept(self, canonical, column_type, kept_as_digits, expected):
        eq_(canonical_default_as_kept(canonical, column_type, kept_as_digits), expected)
