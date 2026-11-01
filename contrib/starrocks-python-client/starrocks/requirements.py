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

from typing import Any, Dict

from alembic.testing.requirements import SuiteRequirements as AlembicSuiteRequirements
import sqlalchemy
from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


IS_SQLALCHEMY_2 = int(sqlalchemy.__version__.split(".")[0]) >= 2


class Requirements(SuiteRequirements, AlembicSuiteRequirements):
    @property
    def sqlalchemy_2(self) -> exclusions.compound:
        """SQLAlchemy 2.0 or greater, which the view, materialized view and Alembic features rely on"""
        return exclusions.skip_if(lambda config: not IS_SQLALCHEMY_2, "SQLAlchemy 2.0 or greater required")

    @property
    def unbounded_varchar(self) -> exclusions.compound:
        """``String()`` without a length compiles as STRING."""
        return exclusions.open()

    @property
    def temp_table_reflection(self) -> exclusions.compound:
        """Closed: the suite's temporary table carries an index, and StarRocks refuses ``CREATE INDEX`` on a
        temporary table ("temporary table doesn't support alter table statement").
        """
        return exclusions.closed()

    @property
    def temp_table_reflect_indexes(self) -> exclusions.compound:
        """Closed, as ``temp_table_reflection``: StarRocks refuses an index on a temporary table."""
        return self.temp_table_reflection

    @property
    def temp_table_names(self) -> exclusions.compound:
        """Closed: the dialect has no ``get_temp_table_names``, though ``SHOW TEMPORARY TABLES`` lists them.

        The suite also creates its temporary table only when ``temp_table_reflection`` is open.
        """
        return exclusions.closed()

    @property
    def has_temp_table(self) -> exclusions.compound:
        """``has_table`` finds a temporary table on SQLAlchemy 2.0, whose MySQL ``has_table`` runs ``DESCRIBE``.

        SQLAlchemy 1.4's reads ``information_schema.tables``, which lists no temporary tables.
        """
        return exclusions.skip_if(
            lambda config: not IS_SQLALCHEMY_2,
            "SQLAlchemy 1.4's has_table reads information_schema.tables, which lists no temporary tables",
        )

    @property
    def temporary_tables(self) -> exclusions.compound:
        """``CREATE TEMPORARY TABLE``. No suite test is gated on it."""
        return exclusions.open()

    @property
    def temporary_views(self) -> exclusions.compound:
        """Closed: StarRocks has no ``CREATE TEMPORARY VIEW``."""
        return exclusions.closed()

    @property
    def index_reflection(self) -> exclusions.compound:
        """Closed: ComponentReflectionTest's fixture tables carry multi-column and unique indexes, which StarRocks
        does not have. ``HasIndexTest``, whose indexes are single-column, is gated on
        ``has_index_waits_for_index_jobs`` instead.

        StarRocks' single-column indexes are reflected from SHOW INDEX, and
        ``test/integration/test_reflection_indexes.py`` reflects BITMAP ones.
        """
        return exclusions.closed()

    @property
    def has_index_waits_for_index_jobs(self) -> exclusions.compound:
        """Open: ``HasIndexTest`` creates single-column indexes, then asks ``has_index`` about them at once.

        StarRocks lists an index only once the job that builds it has ended, and the dialect waits for that job
        after each CREATE INDEX, so the index is there to find. ``HasIndexTest`` is gated on this rather than on
        ``index_reflection``, which stays closed for the unique and multi-column indexes of other suite fixtures.
        """
        return exclusions.open()

    @property
    def primary_key_constraint_reflection(self) -> exclusions.compound:
        """PRIMARY KEY, UNIQUE KEY and AGGREGATE KEY tables reflect their key as the primary key."""
        return exclusions.open()

    @property
    def foreign_key_constraint_reflection(self) -> exclusions.compound:
        """Closed: StarRocks has no foreign keys, so none are reflected."""
        return exclusions.closed()

    @property
    def foreign_key_name_reflection(self) -> exclusions.compound:
        """Alembic: FK names reflect; StarRocks reflects no foreign keys at all"""
        return exclusions.closed()

    @property
    def fk_names(self) -> exclusions.compound:
        """Alembic: FK names reflect (Alembic before 1.20 leaves this open); StarRocks has no foreign keys"""
        return exclusions.closed()

    @property
    def fk_onupdate(self) -> exclusions.compound:
        """Alembic: ON UPDATE round-trips through autogenerate; StarRocks has no foreign keys"""
        return exclusions.closed()

    @property
    def fk_ondelete_restrict(self) -> exclusions.compound:
        """Alembic: ON DELETE RESTRICT round-trips through autogenerate; StarRocks has no foreign keys"""
        return exclusions.closed()

    @property
    def fk_onupdate_restrict(self) -> exclusions.compound:
        """Alembic: ON UPDATE RESTRICT round-trips through autogenerate; StarRocks has no foreign keys"""
        return exclusions.closed()

    @property
    def fk_ondelete_noaction(self) -> exclusions.compound:
        """Alembic: ON DELETE NO ACTION round-trips through autogenerate; StarRocks has no foreign keys"""
        return exclusions.closed()

    @property
    def unique_constraint_reflection(self) -> exclusions.compound:
        """Closed: StarRocks has no unique constraints; a UNIQUE KEY table's key reflects as its primary key."""
        return exclusions.closed()

    @property
    def binary_literals(self) -> exclusions.compound:
        """``CAST('foo' AS BINARY)`` works, and binary values round-trip."""
        return exclusions.open()

    @property
    def datetime_implicit_bound(self) -> exclusions.compound:
        """A datetime bound in a SELECT list reads back as that datetime: the DATETIME type parses the text
        StarRocks returns.
        """
        return exclusions.open()

    @property
    def timestamp_microseconds(self) -> exclusions.compound:
        """``TIMESTAMP`` compiles as DATETIME, which keeps microseconds."""
        return exclusions.open()

    @property
    def timestamp_microseconds_implicit_bound(self) -> exclusions.compound:
        """A datetime with microseconds bound in a SELECT list reads back with them, as ``datetime_implicit_bound``."""
        return exclusions.open()

    @property
    def duplicate_key_raises_integrity_error(self) -> exclusions.compound:
        """Closed: nothing raises. Inserting an existing key replaces the row in a PRIMARY KEY or UNIQUE KEY table,
        and a DUPLICATE KEY table keeps both rows.
        """
        return exclusions.closed()

    @property
    def offset(self) -> exclusions.compound:
        """OFFSET renders after a LIMIT, which the dialect adds when the query has none."""
        return exclusions.open()

    @property
    def bound_limit_offset(self) -> exclusions.compound:
        """LIMIT and OFFSET take bound parameters."""
        return exclusions.open()

    @property
    def sql_expression_limit_offset(self) -> exclusions.compound:
        """Closed: StarRocks takes only a number or a bound parameter in LIMIT and OFFSET, not an expression."""
        return exclusions.closed()

    @property
    def array_type(self) -> exclusions.compound:
        """``sa.ARRAY`` columns round-trip, as ``ArrayTest.test_array_roundtrip`` shows. Before 2.0.33 SQLAlchemy
        has no default for this requirement, and the suite stops at collection without one.

        ``ArrayTest``'s literal round trips are skipped in ``test/test_suite.py``: their table's only column is the
        ARRAY, and StarRocks refuses an ARRAY as a table's first column.
        """
        return exclusions.open()

    @property
    def json_type(self) -> exclusions.compound:
        """StarRocks' JSON column type."""
        return exclusions.open()

    @property
    def time(self) -> exclusions.compound:
        """Closed: StarRocks has no TIME column type."""
        return exclusions.closed()

    @property
    def time_microseconds(self) -> exclusions.compound:
        """Closed: StarRocks has no TIME column type."""
        return exclusions.closed()

    @property
    def implements_get_lastrowid(self) -> exclusions.compound:
        """Closed: StarRocks reports a ``lastrowid`` of 0 after an AUTO_INCREMENT insert, and has no
        ``last_insert_id()``.
        """
        return exclusions.closed()

    @property
    def reflect_table_options(self) -> exclusions.compound:
        """Table options reflect as ``starrocks_*`` keys: engine, key, partitioning, distribution, ORDER BY,
        properties and comment.
        """
        return exclusions.open()

    @property
    def comment_reflection(self) -> exclusions.compound:
        """Table and column comments are reflected, on 3.3 as on later versions."""
        return exclusions.open()

    @property
    def sane_rowcount(self) -> exclusions.compound:
        """UPDATE and DELETE on a PRIMARY KEY table report how many rows they matched.

        A DELETE on a DUPLICATE or UNIQUE KEY table reports 0 rows, even when it deletes some, so the ORM warns that
        it matched none; the suite's tables are PRIMARY KEY tables.
        """
        return exclusions.open()

    @property
    def views(self) -> exclusions.compound:
        """``CREATE VIEW``; views reflect through ``information_schema.views``."""
        return exclusions.open()

    @property
    def like_escapes(self) -> exclusions.compound:
        """Closed: StarRocks has no ``LIKE ... ESCAPE``."""
        return exclusions.closed()

    @property
    def legacy_unconditional_json_extract(self) -> exclusions.compound:
        """``json_query`` returns valid JSON in all cases, as the legacy ``JSON_EXTRACT`` does."""
        return exclusions.open()

    @property
    def update_from(self) -> exclusions.compound:
        """A multi-table ``UPDATE ... FROM``, on PRIMARY KEY tables, the only kind StarRocks lets UPDATE target."""
        return exclusions.open()

    @property
    def delete_from(self) -> exclusions.compound:
        """A multi-table ``DELETE ... USING``, on PRIMARY KEY tables."""
        return exclusions.open()

    @property
    def empty_inserts(self) -> exclusions.compound:
        """Closed: StarRocks has neither ``INSERT INTO t () VALUES ()`` nor ``DEFAULT VALUES``."""
        return exclusions.closed()

    @property
    def precision_generic_float_type(self) -> exclusions.compound:
        """Closed: FLOAT is 32-bit, so 15.7563827 comes back as 15.7563830."""
        return exclusions.closed()

    @property
    def ctes(self) -> exclusions.compound:
        """``WITH ... AS (SELECT ...)``. StarRocks has no recursive CTEs, which ``CTETest`` skips."""
        return exclusions.open()

    @property
    def ctes_with_update_delete(self) -> exclusions.compound:
        """An UPDATE or DELETE that reads a CTE in a correlated subquery, on PRIMARY KEY tables."""
        return exclusions.open()

    @property
    def ctes_with_values(self) -> exclusions.compound:
        """Closed: a CTE whose body is VALUES, ``WITH x AS (VALUES ...)``, is a syntax error."""
        return exclusions.closed()

    @property
    def ctes_on_dml(self) -> exclusions.compound:
        """Closed: a CTE holding INSERT, UPDATE or DELETE, ``WITH x AS (DELETE ...)``, is a syntax error."""
        return exclusions.closed()

    @property
    def enums(self) -> exclusions.compound:
        """Closed: StarRocks has no ENUM type, and ``Enum`` compiles to a VARCHAR. No suite test is gated on it."""
        return exclusions.closed()

    @property
    def unicode_ddl(self) -> exclusions.compound:
        """Table and column names in non-ASCII Unicode, as ``UnicodeSchemaTest`` creates them."""
        return exclusions.open()

    @property
    def window_functions(self) -> exclusions.compound:
        """``OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ...)``"""
        return exclusions.open()

    @property
    def intersect(self) -> exclusions.compound:
        """``INTERSECT``. No suite test is gated on it."""
        return exclusions.open()

    @property
    def except_(self) -> exclusions.compound:
        """``EXCEPT``, which StarRocks also spells ``MINUS``. No suite test is gated on it."""
        return exclusions.open()

    @property
    def nullsordering(self) -> exclusions.compound:
        """``ORDER BY ... NULLS FIRST`` and ``NULLS LAST``"""
        return exclusions.open()

    @property
    def table_value_constructor(self) -> exclusions.compound:
        """``SELECT * FROM (VALUES (...), (...)) AS t``"""
        return exclusions.open()

    @property
    def regexp_match(self) -> exclusions.compound:
        """``regexp_match`` compiles to ``REGEXP``, which StarRocks has."""
        return exclusions.open()

    @property
    def regexp_replace(self) -> exclusions.compound:
        """``regexp_replace`` without flags compiles to StarRocks' ``regexp_replace(str, pattern, repl)``."""
        return exclusions.open()

    @property
    def supports_bitwise_and(self) -> exclusions.compound:
        """``x & y``."""
        return exclusions.open()

    @property
    def supports_bitwise_or(self) -> exclusions.compound:
        """``x | y``."""
        return exclusions.open()

    @property
    def supports_bitwise_xor(self) -> exclusions.compound:
        """``x ^ y``."""
        return exclusions.open()

    @property
    def supports_bitwise_not(self) -> exclusions.compound:
        """``~x``. The shift operators stay closed: StarRocks has ``bit_shift_left`` and ``bit_shift_right``, not ``<<`` and ``>>``."""
        return exclusions.open()

    @property
    def date_historic(self) -> exclusions.compound:
        """Dates from year 1 round-trip."""
        return exclusions.open()

    @property
    def datetime_historic(self) -> exclusions.compound:
        """Datetimes from year 1 round-trip, as dates do."""
        return exclusions.open()

    @property
    def datetime_literals(self) -> exclusions.compound:
        """A date or datetime renders as a quoted literal, which StarRocks reads as DATE or DATETIME."""
        return exclusions.open()

    @property
    def isolation_level(self) -> exclusions.compound:
        """``AUTOCOMMIT``, the default, and ``READ COMMITTED``, which runs DML in StarRocks transactions."""
        return exclusions.open()

    @property
    def autocommit(self) -> exclusions.compound:
        """``AUTOCOMMIT``, the default isolation level, under which StarRocks commits each statement on its own."""
        return exclusions.open()

    def get_isolation_levels(self, config: Any) -> Dict[str, Any]:
        """The default level, and the levels the server can run: ``READ COMMITTED`` needs StarRocks 3.5.

        The levels are stated rather than read from the dialect, so the suite's default-level test checks that
        connections start in ``AUTOCOMMIT``. The deprecated MySQL levels the dialect accepts run as ``AUTOCOMMIT``,
        so they are not levels a connection can be switched to.
        """
        with config.db.connect() as conn:
            supported = ["AUTOCOMMIT"] if conn.dialect.server_version_info < (3, 5) else ["AUTOCOMMIT", "READ COMMITTED"]
            return {"default": "AUTOCOMMIT", "supported": supported}

    @property
    def nvarchar_types(self) -> exclusions.compound:
        """NCHAR and NVARCHAR, which compile to StarRocks' CHAR and VARCHAR, both UTF-8."""
        return exclusions.open()

    @property
    def table_ddl_if_exists(self) -> exclusions.compound:
        """``CREATE TABLE IF NOT EXISTS`` and ``DROP TABLE IF EXISTS``."""
        return exclusions.open()

    @property
    def comment_reflection_full_unicode(self) -> exclusions.compound:
        """Comments of any Unicode text reflect unchanged."""
        return exclusions.open()

    @property
    def schema_create_delete(self) -> exclusions.compound:
        """``CREATE SCHEMA`` and ``DROP SCHEMA``, which StarRocks takes as databases."""
        return exclusions.open()

    @property
    def precision_numerics_retains_significant_digits(self) -> exclusions.compound:
        """DECIMAL keeps trailing zeros: ``Numeric(5, 3)`` reads 1.000 back as ``Decimal('1.000')``.

        ``Numeric`` without a precision is ``decimal(10, 0)`` in StarRocks, so it rounds a fraction away.
        """
        return exclusions.open()

    @property
    def precision_numerics_many_significant_digits(self) -> exclusions.compound:
        """``DECIMAL(38, 12)`` round-trips values with many digits on both sides, such as 319438950232418390.273596."""
        return exclusions.open()

    @property
    def cast_precision_numerics_many_significant_digits(self) -> exclusions.compound:
        """Closed: StarRocks stores a JSON number with a fraction, in an object or an array, as a DOUBLE.

        The suite's JSON tests store 99998969694839.983485848 in an object, which StarRocks keeps as
        99998969694839.98, so the digits are gone before any read. A DECIMAL column keeps them, as
        ``precision_numerics_many_significant_digits`` shows.
        """
        return exclusions.closed()

    @property
    def empty_inserts_executemany(self) -> exclusions.compound:
        """An executemany of an INSERT whose parameter sets are all empty inserts the column defaults."""
        return exclusions.open()

    @property
    def computed_columns(self) -> exclusions.compound:
        """Generated columns work, but StarRocks wants the column type to be the expression's type exactly.

        The suite's tables declare INTEGER columns computed as ``side * side``, which StarRocks types as BIGINT, and
        it rejects them with "Illegal expression type for Generated Column".
        """
        return exclusions.closed()
