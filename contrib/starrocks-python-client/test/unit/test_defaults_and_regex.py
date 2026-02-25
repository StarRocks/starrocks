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

from starrocks.common.defaults import ReflectionTableDefaults
from starrocks.common.utils import TableAttributeNormalizer
from starrocks.reflection import StarRocksTableDefinitionParser


class TestRegexPattern:
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


class TestDefaultValues:
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
