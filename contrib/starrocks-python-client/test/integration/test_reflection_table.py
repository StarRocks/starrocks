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

from sqlalchemy import Column, Integer, MetaData, String, Table, event, inspect, testing
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.testing import config, fixtures

from starrocks.common.params import TableInfoKeyWithPrefix
from starrocks.common.types import SystemRunMode
from starrocks.engine.interfaces import ReflectedPartitionInfo
from starrocks.sql.ddl import AlterTableProperties
from test.util import normalize_sql


class ReflectionTableOptionsTest(fixtures.TestBase):
    """``get_table_options`` reflects every StarRocks table option from information_schema."""

    __backend__ = True
    __only_on__ = "starrocks"

    def test_reflect_table_options(self, connection, metadata):
        Table(
            "test_reflect_table_options",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(32)),
            comment="Test table with all StarRocks options",
            starrocks_engine="OLAP",
            starrocks_primary_key="id",
            starrocks_partition_by="""RANGE(id) (
                PARTITION p1 VALUES LESS THAN ("10"),
                PARTITION p2 VALUES LESS THAN ("20"),
                PARTITION p3 VALUES LESS THAN (MAXVALUE)
            )""",
            starrocks_distributed_by="HASH(id) BUCKETS 8",
            starrocks_order_by="id, name",
            starrocks_properties={"replication_num": "1", "storage_medium": "SSD"},
        )
        metadata.create_all(connection)

        table_options = inspect(connection).get_table_options("test_reflect_table_options")

        assert table_options[TableInfoKeyWithPrefix.ENGINE] == "OLAP"
        assert normalize_sql(table_options[TableInfoKeyWithPrefix.PRIMARY_KEY]) == "id"
        partition_info = table_options[TableInfoKeyWithPrefix.PARTITION_BY]
        assert isinstance(partition_info, ReflectedPartitionInfo)
        assert normalize_sql(partition_info.partition_method) == normalize_sql("RANGE(id)")
        assert "(PARTITION p1 VALUES" in normalize_sql(partition_info.pre_created_partitions)
        assert normalize_sql(table_options[TableInfoKeyWithPrefix.DISTRIBUTED_BY]) == normalize_sql(
            "HASH(id) BUCKETS 8"
        )
        assert normalize_sql(table_options[TableInfoKeyWithPrefix.ORDER_BY]) == normalize_sql("id, name")
        properties = table_options[TableInfoKeyWithPrefix.PROPERTIES]
        assert properties["replication_num"] == "1"
        if connection.dialect.run_mode == SystemRunMode.SHARED_DATA:
            assert properties.get("storage_medium", "HDD") == "HDD"
        else:
            assert properties["storage_medium"] == "SSD"
        assert table_options[TableInfoKeyWithPrefix.COMMENT] == "Test table with all StarRocks options"

    @staticmethod
    def _table_options_of(connection, name: str, ddl: str) -> dict:
        connection.exec_driver_sql(ddl)
        try:
            return inspect(connection).get_table_options(name)
        finally:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    def test_comments_that_name_partition_by_are_no_partition(self, connection):
        table_options = self._table_options_of(
            connection,
            "reflect_partition_comments",
            "CREATE TABLE reflect_partition_comments (k INT NOT NULL COMMENT 'kept for PARTITION BY month, 12\" wide', "
            "v INT) PRIMARY KEY(k) COMMENT 'table \"PARTITION BY\" day' DISTRIBUTED BY HASH(k) BUCKETS 1 "
            "PROPERTIES ('replication_num' = '1')",
        )
        assert TableInfoKeyWithPrefix.PARTITION_BY not in table_options

    def test_partition_value_that_names_a_later_clause(self, connection):
        table_options = self._table_options_of(
            connection,
            "reflect_partition_values",
            "CREATE TABLE reflect_partition_values (c VARCHAR(20) NOT NULL, v INT) DUPLICATE KEY(c) "
            "PARTITION BY LIST(c) (PARTITION p1 VALUES IN ('a', 'DISTRIBUTED BY'), PARTITION p2 VALUES IN ('b')) "
            "DISTRIBUTED BY HASH(c) BUCKETS 1 PROPERTIES ('replication_num' = '1')",
        )
        partition_info = table_options[TableInfoKeyWithPrefix.PARTITION_BY]
        assert normalize_sql(partition_info.partition_method) == "LIST(c)"
        assert "VALUES IN ('a', 'DISTRIBUTED BY')" in partition_info.pre_created_partitions
        assert "VALUES IN ('b')" in partition_info.pre_created_partitions

    def test_partition_names_ending_in_a_clause_keyword(self, connection):
        """StarRocks prints partition names bare: ``p_texas`` ends in AS, ``p_refresh`` in REFRESH."""
        connection.exec_driver_sql(
            "CREATE TABLE reflect_partition_names (dt DATE NOT NULL, v INT) DUPLICATE KEY(dt) "
            "PARTITION BY RANGE(dt) (PARTITION p_texas VALUES LESS THAN ('2020-01-01'), "
            "PARTITION p_refresh VALUES LESS THAN ('2021-01-01'), "
            "PARTITION p_properties VALUES LESS THAN ('2022-01-01')) "
            "DISTRIBUTED BY HASH(dt) BUCKETS 1 PROPERTIES ('replication_num' = '1')"
        )
        try:
            reflected = Table("reflect_partition_names", MetaData(), autoload_with=connection)
            reflected.to_metadata(MetaData(), name="reflect_partition_names_copy").create(connection)
            copied = inspect(connection).get_table_options("reflect_partition_names_copy")
        finally:
            connection.exec_driver_sql("DROP TABLE IF EXISTS reflect_partition_names_copy")
            connection.exec_driver_sql("DROP TABLE IF EXISTS reflect_partition_names")

        partition_info = copied[TableInfoKeyWithPrefix.PARTITION_BY]
        assert normalize_sql(partition_info.partition_method) == "RANGE(dt)"
        for name in ("p_texas", "p_refresh", "p_properties"):
            assert f"PARTITION {name} VALUES" in partition_info.pre_created_partitions

    def test_index_comments_holding_the_end_of_the_column_list(self, connection):
        """StarRocks prints an index comment in single quotes as it is, so it may hold lines like the table's own."""
        table_options = self._table_options_of(
            connection,
            "reflect_index_comments",
            "CREATE TABLE reflect_index_comments (id BIGINT NOT NULL, v VARCHAR(10) NULL, w VARCHAR(10) NULL, "
            "INDEX i1 (v) USING BITMAP COMMENT 'x\n) ENGINE=OLAP \nDUPLICATE KEY(`id`)\nPARTITION BY nope(', "
            "INDEX i2 (w) USING BITMAP COMMENT 'it''s\n) ENGINE=OLAP y') DUPLICATE KEY(id) COMMENT \"it's\" "
            "PARTITION BY RANGE(id) (PARTITION p_texas VALUES LESS THAN ('10')) "
            "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')",
        )

        partition_info = table_options[TableInfoKeyWithPrefix.PARTITION_BY]
        assert normalize_sql(partition_info.partition_method) == "RANGE(id)"
        assert "PARTITION p_texas VALUES" in partition_info.pre_created_partitions

    @testing.combinations(("opening", "(%Y-%m-%d"), ("closing", ")%Y-%m-%d"), argnames="date_format", id_="ia")
    def test_a_parenthesis_in_a_range_expression_literal(self, connection, date_format):
        table_options = self._table_options_of(
            connection,
            "reflect_range_literal",
            "CREATE TABLE reflect_range_literal (id BIGINT NOT NULL, s VARCHAR(20) NOT NULL) DUPLICATE KEY(id) "
            f"PARTITION BY RANGE(str2date(s, '{date_format}')) (PARTITION p1 VALUES LESS THAN ('2020-01-01')) "
            "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')".replace("%", "%%"),
        )

        partition_info = table_options[TableInfoKeyWithPrefix.PARTITION_BY]
        assert normalize_sql(partition_info.partition_method) == normalize_sql(f"RANGE(str2date(s, '{date_format}'))")
        assert "PARTITION p1 VALUES" in partition_info.pre_created_partitions

    def test_partitions_named_as_a_clause_keyword(self, connection):
        """StarRocks prints even these partition names bare, so the reflected clause can't be sent back as it is."""
        table_options = self._table_options_of(
            connection,
            "reflect_partition_keywords",
            "CREATE TABLE reflect_partition_keywords (dt DATE NOT NULL, v INT) DUPLICATE KEY(dt) "
            "PARTITION BY RANGE(dt) (PARTITION `as` VALUES LESS THAN ('2020-01-01'), "
            "PARTITION `properties` VALUES LESS THAN ('2021-01-01')) "
            "DISTRIBUTED BY HASH(dt) BUCKETS 1 PROPERTIES ('replication_num' = '1')",
        )

        partition_info = table_options[TableInfoKeyWithPrefix.PARTITION_BY]
        assert normalize_sql(partition_info.partition_method) == "RANGE(dt)"
        for name in ("as", "properties"):
            assert f"PARTITION {name} VALUES" in partition_info.pre_created_partitions


class PropertyValueRoundTripTest(fixtures.TestBase):
    """A PROPERTIES value with backslashes and double quotes is stored, and reflected, exactly as given."""

    __backend__ = True
    __only_on__ = "starrocks"

    @staticmethod
    def _reflected_colocate_with(connection, table_name):
        options = inspect(connection).get_table_options(table_name)
        return options[TableInfoKeyWithPrefix.PROPERTIES]["colocate_with"]

    @testing.combinations(
        ("backslash_letter", r"grp\d+"),
        ("double_quote", 'grp"1'),
        ("escaped_quote", r'grp\"1'),
        argnames="value",
        id_="ia",
    )
    def test_create_and_alter(self, connection, metadata, value):
        Table(
            "prop_round_trip",
            metadata,
            Column("id", Integer),
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"colocate_with": value},
        )
        metadata.create_all(connection)
        assert self._reflected_colocate_with(connection, "prop_round_trip") == value

        altered = value + "_2"
        connection.execute(AlterTableProperties("prop_round_trip", {"colocate_with": altered}))
        assert self._reflected_colocate_with(connection, "prop_round_trip") == altered


class ReflectedPrimaryKeyTest(fixtures.TestBase):
    """Only tables with one row per key report their key columns as the primary key."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.combinations(
        ("primary_key", "starrocks_primary_key", ["id", "tenant"]),
        ("unique_key", "starrocks_unique_key", ["id", "tenant"]),
        ("aggregate_key", "starrocks_aggregate_key", ["id", "tenant"]),
        ("duplicate_key", "starrocks_duplicate_key", []),
        argnames="key_option, expected_pk_columns",
        id_="iaa",
    )
    def test_primary_key_reported_by_table_model(self, connection, metadata, key_option, expected_pk_columns):
        value_kwargs = {"starrocks_agg_type": "SUM"} if key_option == "starrocks_aggregate_key" else {}
        Table(
            "reflected_pk_model",
            metadata,
            Column("id", Integer),
            Column("tenant", Integer),
            Column("val", Integer, **value_kwargs),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
            **{key_option: "id, tenant"},
        )
        metadata.create_all(connection)

        inspector = inspect(connection)
        assert inspector.get_pk_constraint("reflected_pk_model")["constrained_columns"] == expected_pk_columns
        assert inspector.get_table_options("reflected_pk_model")[f"{key_option}"] == "id, tenant"
        assert inspector.get_indexes("reflected_pk_model") == []


class ReflectionStatementCountTest(fixtures.TestBase):
    """A table is read from information_schema once, however it is reflected."""

    __backend__ = True
    __only_on__ = "starrocks"

    @staticmethod
    def _create_tables(connection, metadata, count):
        for number in range(count):
            Table(
                f"stmt_count_{number}",
                metadata,
                Column("id", Integer),
                Column("name", String(32)),
                starrocks_duplicate_key="id",
                starrocks_distributed_by="HASH(id) BUCKETS 1",
                starrocks_properties={"replication_num": "1"},
            )
        metadata.create_all(connection)

    @staticmethod
    def _statements_while(connection, reflect):
        statements = []

        def record(conn, cursor, statement, parameters, context, executemany):
            statements.append(" ".join(statement.split()).lower())

        event.listen(connection, "before_cursor_execute", record)
        try:
            reflect()
        finally:
            event.remove(connection, "before_cursor_execute", record)
        return statements

    @staticmethod
    def _reads_of(statements, information_schema_view):
        return [s for s in statements if f"information_schema.{information_schema_view} " in s]

    def test_autoload_reads_each_table_once(self, connection, metadata):
        self._create_tables(connection, metadata, 1)

        statements = self._statements_while(
            connection, lambda: Table("stmt_count_0", MetaData(), autoload_with=connection)
        )

        assert len(self._reads_of(statements, "tables_config")) == 1

    def test_metadata_reflect_reads_no_table_twice(self, connection, metadata):
        self._create_tables(connection, metadata, 3)
        names = [f"stmt_count_{number}" for number in range(3)]

        statements = self._statements_while(connection, lambda: MetaData().reflect(connection, only=names))

        assert len(self._reads_of(statements, "tables_config")) <= 3

    @testing.requires.sqlalchemy_2
    def test_metadata_reflect_reads_information_schema_once(self, connection, metadata):
        self._create_tables(connection, metadata, 3)
        names = [f"stmt_count_{number}" for number in range(3)]

        statements = self._statements_while(connection, lambda: MetaData().reflect(connection, only=names))

        assert [len(self._reads_of(statements, view)) for view in ("tables", "tables_config", "columns")] == [1, 1, 1]

    def test_a_name_given_as_str_and_as_quoted_name_is_read_once(self, connection, metadata):
        """SQLAlchemy 2.0's reflection cache keys a ``quoted_name`` apart from a plain ``str`` of the same name."""
        self._create_tables(connection, metadata, 1)
        inspector = inspect(connection)

        def reflect_under_both_names():
            inspector.get_columns("stmt_count_0")
            inspector.get_pk_constraint(quoted_name("stmt_count_0", None))

        statements = self._statements_while(connection, reflect_under_both_names)

        assert len(self._reads_of(statements, "tables_config")) == 1

    @testing.requires.sqlalchemy_2
    def test_the_temporary_scope_reads_no_base_table(self, connection, metadata):
        from sqlalchemy.engine.reflection import ObjectScope

        self._create_tables(connection, metadata, 2)

        statements = self._statements_while(
            connection, lambda: inspect(connection).get_multi_columns(scope=ObjectScope.TEMPORARY)
        )

        assert statements == []

    @testing.requires.sqlalchemy_2
    def test_a_table_dropped_during_the_bulk_read_is_left_out(self, connection, metadata):
        """Once the table is dropped after information_schema was read, StarRocks refuses its SHOW CREATE TABLE."""
        self._create_tables(connection, metadata, 3)
        names = [f"stmt_count_{number}" for number in range(3)]
        dropped = []

        def drop_a_table_after_the_bulk_columns_read(conn, cursor, statement, parameters, context, executemany):
            if not dropped and "information_schema.columns" in statement and " IN " in statement.upper():
                with config.db.connect() as other_connection:
                    other_connection.exec_driver_sql("DROP TABLE IF EXISTS stmt_count_1 FORCE")
                dropped.append("stmt_count_1")

        event.listen(connection, "after_cursor_execute", drop_a_table_after_the_bulk_columns_read)
        try:
            columns = inspect(connection).get_multi_columns(filter_names=names)
        finally:
            event.remove(connection, "after_cursor_execute", drop_a_table_after_the_bulk_columns_read)

        assert dropped == ["stmt_count_1"]
        assert sorted(name for _, name in columns) == ["stmt_count_0", "stmt_count_2"]


class BatchedReflectionMatchesSingleReflectionTest(fixtures.TestBase):
    """Reading many tables at once reflects exactly what reading them one by one does."""

    __backend__ = True
    __only_on__ = "starrocks"

    NAMES = ("batch_dup", "batch_agg", "batch_pk", "batch_view", "batch_mv")
    PROPERTIES = 'PROPERTIES("replication_num" = "1")'

    @testing.fixture
    def mixed_objects(self, connection):
        for statement in (
            "DROP MATERIALIZED VIEW IF EXISTS batch_mv",
            "DROP VIEW IF EXISTS batch_view",
            "DROP TABLE IF EXISTS batch_dup",
            "DROP TABLE IF EXISTS batch_agg",
            "DROP TABLE IF EXISTS batch_pk",
            f"CREATE TABLE batch_dup (id INT COMMENT 'the id', label VARCHAR(20) DEFAULT 'x') "
            f"DUPLICATE KEY(id) COMMENT 'dup table' DISTRIBUTED BY HASH(id) BUCKETS 2 {self.PROPERTIES}",
            f"CREATE TABLE batch_agg (id BIGINT AUTO_INCREMENT, k INT, total INT SUM, h HLL HLL_UNION) "
            f"AGGREGATE KEY(id, k) DISTRIBUTED BY HASH(id) {self.PROPERTIES}",
            f"CREATE TABLE batch_pk (id BIGINT NOT NULL, v INT) PRIMARY KEY(id) "
            f"PARTITION BY RANGE(id) (PARTITION p1 VALUES LESS THAN ('100')) DISTRIBUTED BY HASH(id) {self.PROPERTIES}",
            "CREATE VIEW batch_view AS SELECT id, label FROM batch_dup",
            f"CREATE MATERIALIZED VIEW batch_mv REFRESH MANUAL {self.PROPERTIES} AS SELECT id, label FROM batch_dup",
        ):
            connection.exec_driver_sql(statement)
        yield
        for statement in (
            "DROP MATERIALIZED VIEW IF EXISTS batch_mv",
            "DROP VIEW IF EXISTS batch_view",
            "DROP TABLE IF EXISTS batch_dup",
            "DROP TABLE IF EXISTS batch_agg",
            "DROP TABLE IF EXISTS batch_pk",
        ):
            connection.exec_driver_sql(statement)

    @staticmethod
    def _comparable(columns):
        """Types do not compare equal, so compare their repr."""
        return [{**column, "type": repr(column["type"])} for column in columns]

    @testing.requires.sqlalchemy_2
    def test_multi_columns_match_single_columns(self, connection, mixed_objects):
        from sqlalchemy.engine.reflection import ObjectKind

        schema = connection.engine.url.database
        batched = dict(
            inspect(connection).get_multi_columns(schema=schema, filter_names=list(self.NAMES), kind=ObjectKind.ANY)
        )

        assert sorted(name for _, name in batched) == sorted(self.NAMES)
        for name in self.NAMES:
            assert self._comparable(batched[(schema, name)]) == self._comparable(
                inspect(connection).get_columns(name, schema=schema)
            ), name

    @testing.requires.sqlalchemy_2
    def test_reflected_tables_match_single_reflection(self, connection, mixed_objects):
        batch = MetaData()
        batch.reflect(connection, only=list(self.NAMES), views=True)

        assert sorted(batch.tables) == sorted(self.NAMES)
        for name in self.NAMES:
            single = Table(name, MetaData(), autoload_with=connection)
            assert [(c.name, repr(c.type), c.nullable, c.comment) for c in batch.tables[name].columns] == [
                (c.name, repr(c.type), c.nullable, c.comment) for c in single.columns
            ], name
            assert batch.tables[name].comment == single.comment, name
            assert batch.tables[name].info == single.info, name
            assert dict(batch.tables[name].kwargs) == dict(single.kwargs), name
        agg = batch.tables["batch_agg"]
        assert agg.c.id.autoincrement is True
        assert agg.c.total.dialect_options["starrocks"]["agg_type"] == "SUM"
