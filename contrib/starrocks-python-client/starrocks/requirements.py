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

from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions


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
        return exclusions.open()

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
        return exclusions.open()

    @property
    def bound_limit_offset(self):
        """target database can render LIMIT and/or OFFSET using a bound
        parameter
        """
        # ToDo - see offset above
        return exclusions.open()

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

        return exclusions.open()

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
        return exclusions.closed()  # Does not support column comments

    @property
    def sane_rowcount(self):
        return exclusions.closed()  # TODO: Check if that is expected

    @property
    def views(self):
        """Target database must support VIEWs."""

        return exclusions.open()

    @property
    def like_escapes(self):
        # Starrocks does not support like escape
        return exclusions.closed()

    @property
    def legacy_unconditional_json_extract(self):
        return exclusions.closed()

    @property
    def empty_inserts(self):
        return exclusions.closed()

    @property
    def precision_generic_float_type(self):
        """target backend will return native floating point numbers with at
        least seven decimal places when using the generic Float type.

        """
        return exclusions.closed()  #ToDo - I couldn't get the test for this one working, not sure where the issue is - AssertionError: {Decimal('15.7563830')} != {Decimal('15.7563827')}

    @property
    def ctes(self):
        """Target database supports CTEs"""
        return exclusions.open()

    @property
    def ctes_with_update_delete(self):
        """target database supports CTES that ride on top of a normal UPDATE
        or DELETE statement which refers to the CTE in a correlated subquery.

        """
        return exclusions.open()

    @property
    def ctes_with_values(self):
        """target database supports CTES that ride on top of a VALUES
        clause."""
        return exclusions.closed()

    @property
    def ctes_on_dml(self):
        """target database supports CTES which consist of INSERT, UPDATE
        or DELETE *within* the CTE, e.g. WITH x AS (UPDATE....)"""
        return exclusions.open()

    @property
    def enums(self):
        """target database supports ENUM type"""
        return exclusions.closed()

    @property
    def unicode_ddl(self):
        return exclusions.open()

