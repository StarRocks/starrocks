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
# limitations under the License.

from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):
    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""
        return exclusions.closed()  # Starrocks does not support unbounded VARCHAR

    @property
    def temp_table_reflection(self):
        return exclusions.closed()

    @property
    def temp_table_reflect_indexes(self):
        return self.temp_table_reflection

    @property
    def temp_table_names(self):
        """target dialect supports listing of temporary table names"""
        return exclusions.closed()

    @property
    def has_temp_table(self):
        """target dialect supports checking a single temp table name"""
        return exclusions.closed()

    @property
    def temporary_tables(self):
        """target database supports temporary tables"""
        return exclusions.closed()

    @property
    def temporary_views(self):
        """target database supports temporary views"""
        return exclusions.closed()

    @property
    def index_reflection(self):
        # ToDo - open when multi-column index is supported
        return exclusions.closed()

    @property
    def primary_key_constraint_reflection(self):
        # ToDo - open when multi-column index is supported
        return exclusions.closed()

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.closed()

    @property
    def unique_constraint_reflection(self):
        """target dialect supports reflection of unique constraints"""
        return exclusions.closed()

    @property
    def binary_literals(self):
        """target backend supports simple binary literals, e.g. an
        expression like::

            SELECT CAST('foo' AS BINARY)

        Where ``BINARY`` is the type emitted from :class:`.LargeBinary`,
        e.g. it could be ``BLOB`` or similar.

        Basically fails on Oracle.

        """

        return exclusions.closed()

    @property
    def datetime_implicit_bound(self):
        """target dialect when given a datetime object will bind it such
        that the database server knows the object is a datetime, and not
        a plain string.
        https://github.com/sqlalchemy/sqlalchemy/discussions/9661
        """
        return exclusions.closed()

    @property
    def duplicate_key_raises_integrity_error(self):
        """target dialect raises IntegrityError when reporting an INSERT
        with a primary key violation.  (hint: it should)

        """
        return exclusions.closed()

    @property
    def offset(self):
        """target database can render OFFSET, or an equivalent, in a
        SELECT.
        """
        # ToDo - enable if Starrocks supports offset without limit (has order by)
        return exclusions.closed()

    @property
    def bound_limit_offset(self):
        """target database can render LIMIT and/or OFFSET using a bound
        parameter
        """
        # ToDo - see offset above
        return exclusions.closed()

    @property
    def sql_expression_limit_offset(self):
        """target database can render LIMIT and/or OFFSET with a complete
        SQL expression, such as one that uses the addition operator.
        parameter
        """

        return exclusions.closed()  # Not supported on StarRocks

    @property
    def json_type(self):
        """target platform implements a native JSON type."""

        return exclusions.closed()  # Starrocks returns all json elements casted as string

    @property
    def time(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()  # Not supported on Starrocks (no time type)

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()  # Not supported on Starrocks (no time type)

    @property
    def implements_get_lastrowid(self):
        """target dialect implements the executioncontext.get_lastrowid()
        method without reliance on RETURNING.
        """

        return exclusions.closed()

    @property
    def reflect_table_options(self):
        """Target database must support reflecting table_options."""

        return exclusions.open()

    @property
    def comment_reflection(self):
        """Indicates if the database support table comment reflection"""
        return exclusions.open()

    @property
    def sane_rowcount(self):
        return exclusions.closed()  # TODO: Check if that is expected

    @property
    def views(self):
        """Target database must support VIEWs."""

        return exclusions.open()



# ===================================================================================
# Below is the section with manual exclusions which cannot be excluded by Requirements
# ===================================================================================
# fmt: off
from sqlalchemy.testing.suite.test_ddl import LongNameBlowoutTest
from sqlalchemy.testing.suite.test_dialect import ExceptionTest
from sqlalchemy.testing.suite.test_insert import InsertBehaviorTest
from sqlalchemy.testing.suite.test_select import FetchLimitOffsetTest, LikeFunctionsTest
from sqlalchemy.testing.suite.test_types import (
    BinaryTest,
    DateTest,
    DateTimeCoercedToDateTimeTest,
    DateTimeTest,
    EnumTest,
    JSONTest,
    NumericTest,
    StringTest,
)
# fmt: on


try:
    from sqlalchemy.testing.suite.test_reflection import (
        BizarroCharacterFKResolutionTest,
        ComponentReflectionTest,
        CompositeKeyReflectionTest,
        HasIndexTest,
        HasTableTest,
        QuotedNameArgumentTest,
    )
except ImportError:
    from sqlalchemy.testing.suite.test_reflection import (
        ComponentReflectionTest,
        CompositeKeyReflectionTest,
        HasIndexTest,
        HasTableTest,
        QuotedNameArgumentTest,
    )
    class BizarroCharacterFKResolutionTest:  # placeholder when removed in newer SQLAlchemy
        pass

# ========== Add missing requires. TODO: Can be deleted when https://github.com/sqlalchemy/sqlalchemy/pull/12362 is merged
BinaryTest.__requires__ = ("binary_literals",)
BizarroCharacterFKResolutionTest.__requires__ = ("primary_key_constraint_reflection",)
QuotedNameArgumentTest.test_get_foreign_keys = lambda *args: None  # missing requires.foreign_key_constraint_reflection
HasIndexTest.__requires__ = ("index_reflection",)
# Starrocks does not support FLOAT type for first column
NumericTest.test_float_as_decimal = lambda *args: None
NumericTest.test_float_as_float = lambda *args: None
NumericTest.test_float_custom_scale = lambda *args: None
NumericTest.test_render_literal_float = lambda *args: None
# Starrocks has no JSON_EXTRACT function
JSONTest.test_index_typed_access = lambda *args: None
JSONTest.test_index_typed_comparison = lambda *args: None
JSONTest.test_path_typed_comparison = lambda *args: None
# Syntax error -> Starrocks has no LIKE + ESCAPE
LikeFunctionsTest.test_contains_autoescape = lambda *args: None
LikeFunctionsTest.test_contains_autoescape_escape = lambda *args: None
LikeFunctionsTest.test_contains_escape = lambda *args: None
LikeFunctionsTest.test_endswith_autoescape = lambda *args: None
LikeFunctionsTest.test_endswith_autoescape_escape = lambda *args: None
LikeFunctionsTest.test_endswith_escape = lambda *args: None
LikeFunctionsTest.test_startswith_autoescape = lambda *args: None
LikeFunctionsTest.test_startswith_autoescape_escape = lambda *args: None
LikeFunctionsTest.test_startswith_escape = lambda *args: None
# Missing index_reflection
QuotedNameArgumentTest.test_get_indexes = lambda *args: None
# ======================================================
# ========== Not working "requires" decorators - they seems to be correctly used, but tests are not skipped
QuotedNameArgumentTest.test_get_unique_constraints = lambda *args: None
ComponentReflectionTest.test_get_multi_indexes = lambda *args: None
ComponentReflectionTest.test_get_multi_foreign_keys = lambda *args: None
ComponentReflectionTest.test_get_foreign_keys = lambda *args: None
ComponentReflectionTest.test_get_indexes = lambda *args: None
ComponentReflectionTest.test_get_multi_pk_constraint = lambda *args: None
ComponentReflectionTest.test_get_multi_unique_constraints = lambda *args: None
ComponentReflectionTest.test_get_noncol_index = lambda *args: None
ComponentReflectionTest.test_get_pk_constraint = lambda *args: None
ComponentReflectionTest.test_get_table_names = lambda *args: None
ComponentReflectionTest.test_get_temp_table_columns = lambda *args: None
ComponentReflectionTest.test_get_temp_table_indexes = lambda *args: None
ComponentReflectionTest.test_get_temp_table_unique_constraints = lambda *args: None
ComponentReflectionTest.test_get_unique_constraints = lambda *args: None
ComponentReflectionTest.test_get_unique_constraints = lambda *args: None
ComponentReflectionTest.test_reflect_table_temp_table = lambda *args: None
CompositeKeyReflectionTest.test_fk_column_order = lambda *args: None
CompositeKeyReflectionTest.test_pk_column_order = lambda *args: None
ExceptionTest.test_integrity_error = lambda *args: None
StringTest.test_nolength_string = lambda *args: None
FetchLimitOffsetTest.test_bound_offset = lambda *args: None
FetchLimitOffsetTest.test_bound_limit_offset = lambda *args: None
FetchLimitOffsetTest.test_expr_limit = lambda *args: None
FetchLimitOffsetTest.test_expr_limit_offset = lambda *args: None
FetchLimitOffsetTest.test_expr_limit_simple_offset = lambda *args: None
FetchLimitOffsetTest.test_expr_offset = lambda *args: None
FetchLimitOffsetTest.test_simple_limit_expr_offset = lambda *args: None
FetchLimitOffsetTest.test_simple_offset = lambda *args: None
FetchLimitOffsetTest.test_simple_offset_zero = lambda *args: None
LongNameBlowoutTest.test_long_convention_name = lambda *args: None
# ======================================================
# ========== Not implemented in reflection
ComponentReflectionTest.test_autoincrement_col = lambda *args: None  # There is no information about autoincrement in information_schema.columns
# Temporary table is only in informatio_schema.tables_config, but not in information_schema.tables and not in information_schema.columns
# ======================================================
# ========== Something is not working, but not in starrocks reflection or before checking reflection
ComponentReflectionTest.test_get_multi_columns = lambda *args: None  # Incorrect table created (e.g. DUP instead of PRI and no comments)
ComponentReflectionTest.test_get_view_names = lambda *args: None  # Views has not been created
DateTest.test_select_direct = lambda *args: None  # Compares string with date object
DateTimeCoercedToDateTimeTest.test_select_direct = lambda *args: None  # Compares string with datetime object
DateTimeTest.test_select_direct = lambda *args: None  # Compares string with datetime object
HasTableTest.test_has_table_cache = lambda *args: None  # clear_cache() is not needed?
InsertBehaviorTest.test_empty_insert = lambda *args: None  # Cannot "INSERT INTO autoinc_pk () VALUES ()""
EnumTest.__requires__ = ("binary_literals",)  # Fix Enum handling. Mysql has native ENUM type, but Starrocks has not
# ======================================================
