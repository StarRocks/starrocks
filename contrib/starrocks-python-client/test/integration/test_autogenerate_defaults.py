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

"""Server default comparisons in autogenerate against tables reflected from StarRocks."""

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Column, MetaData, Table, func, inspect, literal, testing, text
from sqlalchemy.testing import fixtures

import starrocks.alembic  # noqa: F401
from starrocks.common.utils import unquote_sql_string_literal
from starrocks.datatype import BITMAP, BOOLEAN, DATE, DATETIME, HLL, INTEGER, JSON, PERCENTILE, VARCHAR
from test.util import BOOLEAN_DEFAULT_KEPT_AS_DIGITS, FLAT_JSON_PROPERTIES_NEED_4_0


class _DefaultsTestBase(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    @staticmethod
    def _diff_ops(connection, metadata: MetaData, table_prefix: str) -> list:
        context = MigrationContext.configure(
            connection,
            opts={
                "compare_server_default": True,
                "include_name": lambda name, type_, parents: type_ != "table" or name.startswith(table_prefix),
            },
        )
        return compare_metadata(context, metadata)


class SketchColumnDefaultTest(_DefaultsTestBase):
    """HLL and BITMAP columns reflect with no default, though information_schema reports a NUL placeholder for them.

    SHOW CREATE TABLE prints no DEFAULT for these columns, and reflection goes by that. Where SHOW CREATE TABLE gives
    no spelling for a column, the default renderer takes the placeholder for no default; the unit tests of
    ``_ColumnDefaultValueRenderer`` cover that fallback, which no table on a server reaches.
    """

    @testing.requires.sqlalchemy_2
    def test_sketch_columns_have_no_reflected_default(self, connection, metadata):
        Table(
            "sketch_defaults",
            metadata,
            Column("id", INTEGER),
            Column("h", HLL, starrocks_agg_type="HLL_UNION"),
            Column("b", BITMAP, starrocks_agg_type="BITMAP_UNION"),
            Column("p", PERCENTILE, starrocks_agg_type="PERCENTILE_UNION"),
            starrocks_aggregate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        metadata.create_all(connection)
        placeholders = connection.exec_driver_sql(
            "SELECT COLUMN_NAME, COLUMN_DEFAULT FROM information_schema.columns "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'sketch_defaults' AND COLUMN_NAME IN ('h', 'b')"
        ).all()

        reflected = Table("sketch_defaults", MetaData(), autoload_with=connection)

        assert sorted(tuple(row) for row in placeholders) == [("b", "\x00"), ("h", "\x00")]
        assert [c.name for c in reflected.columns if c.server_default is not None] == []
        assert inspect(connection).get_columns("sketch_defaults")[1]["default"] is None
        assert self._diff_ops(connection, metadata, "sketch_defaults") == []


class ServerDefaultComparisonTest(_DefaultsTestBase):
    """A default that is only written differently is not a change, and a different default still is."""

    @staticmethod
    def _table(metadata: MetaData, name: str, **column_defaults) -> Table:
        return Table(
            name,
            metadata,
            Column("id", INTEGER),
            *[Column(column, VARCHAR(40), server_default=default) for column, default in column_defaults.items()],
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )

    @testing.combinations(
        ("bare_function", text("uuid()")),
        ("upper_case_function", text("UUID()")),
        ("wrapped_function", text("(uuid())")),
        ("function_element", func.uuid()),
        argnames="metadata_default",
        id_="ia",
    )
    @testing.requires.sqlalchemy_2
    def test_uuid_default_converges(self, connection, metadata, metadata_default):
        self._table(metadata, "default_uuid", token=text("(uuid())"))
        metadata.create_all(connection)
        model = MetaData()
        self._table(model, "default_uuid", token=metadata_default)

        assert self._diff_ops(connection, model, "default_uuid") == []

    @testing.combinations(
        ("plain", "a-b"),
        ("single_quote", "it's"),
        ("backslash", "x\\y"),
        ("double_quote", 'say "hi"'),
        ("number_like", "5"),
        ("empty", ""),
        ("padded", "  padded  "),
        ("null_word", "NULL"),
        ("call_like", "uuid()"),
        argnames="value",
        id_="ia",
    )
    @testing.requires.sqlalchemy_2
    def test_literal_default_round_trips(self, connection, metadata, value, caplog):
        self._table(metadata, "default_literal", label=value)
        metadata.create_all(connection)

        reflected = Table("default_literal", MetaData(), autoload_with=connection)

        assert unquote_sql_string_literal(reflected.c.label.server_default.arg.text) == value
        assert self._diff_ops(connection, metadata, "default_literal") == []
        assert "Detected DEFAULT change" not in caplog.text

    @testing.requires.sqlalchemy_2
    def test_expression_default_with_a_percent_converges(self, connection, metadata, caplog):
        """An expression default compares as the text StarRocks reflects, so its ``%`` is not doubled."""
        self._table(metadata, "default_percent", label=literal("50%"))
        metadata.create_all(connection)

        assert self._diff_ops(connection, metadata, "default_percent") == []
        assert "Detected DEFAULT change" not in caplog.text

    @testing.requires.sqlalchemy_2
    def test_changed_literal_default_is_still_a_difference(self, connection, metadata, caplog):
        """StarRocks cannot change a default, so the detected change is reported instead of generated."""
        self._table(metadata, "default_changed", label="before")
        metadata.create_all(connection)
        model = MetaData()
        self._table(model, "default_changed", label="after")

        ops = self._diff_ops(connection, model, "default_changed")

        assert ops == []
        assert "Detected DEFAULT change on column 'default_changed.label'" in caplog.text


class StoredDefaultComparisonTest(_DefaultsTestBase):
    """A default that StarRocks stores in another form than it was written is not a change."""

    @staticmethod
    def _table(metadata: MetaData, name: str, column: Column) -> Table:
        return Table(
            name,
            metadata,
            Column("id", INTEGER),
            column,
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )

    @testing.combinations(
        ("now_function", func.now()),
        ("current_timestamp_function", func.current_timestamp()),
        ("current_timestamp_text", text("CURRENT_TIMESTAMP")),
        argnames="metadata_default",
        id_="ia",
    )
    @testing.requires.sqlalchemy_2
    def test_the_time_default_StarRocks_reflects_as_current_timestamp_converges(
        self, connection, metadata, metadata_default, caplog
    ):
        self._table(metadata, "default_time", Column("at", DATETIME, server_default=func.current_timestamp()))
        metadata.create_all(connection)
        model = MetaData()
        self._table(model, "default_time", Column("at", DATETIME, server_default=metadata_default))

        assert self._diff_ops(connection, model, "default_time") == []
        assert "Detected DEFAULT change" not in caplog.text

    @testing.combinations(
        ("as_written", "true"),
        ("upper_case", "TRUE", BOOLEAN_DEFAULT_KEPT_AS_DIGITS),
        ("as_a_digit", "1", BOOLEAN_DEFAULT_KEPT_AS_DIGITS),
        argnames="metadata_default",
        id_="ia",
    )
    @testing.requires.sqlalchemy_2
    def test_boolean_default_converges_however_StarRocks_keeps_it(self, connection, metadata, metadata_default, caplog):
        self._table(metadata, "default_bool", Column("flag", BOOLEAN, server_default="true"))
        metadata.create_all(connection)
        model = MetaData()
        self._table(model, "default_bool", Column("flag", BOOLEAN, server_default=metadata_default))

        assert self._diff_ops(connection, model, "default_bool") == []
        assert "Detected DEFAULT change" not in caplog.text


class ServerAddedPropertiesTest(_DefaultsTestBase):
    """Properties StarRocks adds next to the ones a model sets are left alone without a warning."""

    @testing.combinations(
        (
            "dynamic_partition",
            {
                "dynamic_partition.enable": "true",
                "dynamic_partition.time_unit": "DAY",
                "dynamic_partition.start": "-3",
                "dynamic_partition.end": "3",
                "dynamic_partition.prefix": "p",
                "dynamic_partition.buckets": "1",
            },
            "dynamic_partition.time_zone",
        ),
        (
            "dynamic_partition_without_start",
            {"dynamic_partition.enable": "true", "dynamic_partition.time_unit": "DAY", "dynamic_partition.end": "3",
             "dynamic_partition.prefix": "p", "dynamic_partition.buckets": "1"},
            "dynamic_partition.start",
        ),
        (
            "dynamic_partition_by_week",
            {"dynamic_partition.enable": "true", "dynamic_partition.time_unit": "WEEK", "dynamic_partition.end": "3",
             "dynamic_partition.prefix": "p", "dynamic_partition.buckets": "1"},
            "dynamic_partition.start_day_of_week",
        ),
        (
            "dynamic_partition_by_month",
            {"dynamic_partition.enable": "true", "dynamic_partition.time_unit": "MONTH", "dynamic_partition.end": "3",
             "dynamic_partition.prefix": "p", "dynamic_partition.buckets": "1"},
            "dynamic_partition.start_day_of_month",
        ),
        (
            "dynamic_partition_without_enable",
            {"dynamic_partition.time_unit": "DAY", "dynamic_partition.end": "3", "dynamic_partition.prefix": "p"},
            "dynamic_partition.enable",
        ),
        (
            "dynamic_partition_without_prefix",
            {"dynamic_partition.enable": "true", "dynamic_partition.time_unit": "DAY", "dynamic_partition.end": "3"},
            "dynamic_partition.prefix",
        ),
        ("flat_json", {"flat_json.enable": "true"}, "flat_json.null.factor", FLAT_JSON_PROPERTIES_NEED_4_0),
        argnames="properties, added_key",
        id_="iaa",
    )
    @testing.requires.sqlalchemy_2
    def test_added_properties_do_not_warn(self, connection, metadata, caplog, properties, added_key):
        Table(
            "server_added_properties",
            metadata,
            Column("d", DATE),
            Column("j", JSON),
            starrocks_duplicate_key="d",
            starrocks_partition_by="RANGE(d)()",
            starrocks_distributed_by="HASH(d) BUCKETS 1",
            starrocks_properties={"replication_num": "1", **properties},
        )
        metadata.create_all(connection)
        reported = inspect(connection).get_table_options("server_added_properties")["starrocks_properties"]
        caplog.set_level("WARNING")

        ops = self._diff_ops(connection, metadata, "server_added_properties")

        assert added_key in reported
        assert ops == []
        assert "no default is defined" not in caplog.text
