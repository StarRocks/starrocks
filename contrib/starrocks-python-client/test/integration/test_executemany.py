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

"""An executemany of an INSERT is sent as multi-row INSERTs, whatever its VALUES row holds.

Each INSERT StarRocks runs is a load, listed in ``information_schema.loads``, so the tests count the loads an
executemany made, and read back what it stored.
"""

from datetime import date
from decimal import Decimal

from sqlalchemy import JSON, Column, Integer, String, Table, cast, exc, func, select, testing, text
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.testing import eq_, expect_raises, fixtures

from starrocks.datatype import ARRAY, DATE, DECIMAL, INTEGER, MAP, STRUCT, VARCHAR
from starrocks.dialect import StarRocksDialect
from test.util import FLAT_JSON_NULL_AMONG_OBJECTS_READS_AS_EMPTY, FLAT_JSON_PROPERTIES_NEED_4_0


def _json_document(n):
    return {"n": n, "s": "it's 100%", "list": [n, "x"]}


class ExecutemanyLoadsTest(fixtures.TestBase):
    """The rows of an executemany go in one INSERT, or in as few as StarRocks and the driver take."""

    __backend__ = True
    __only_on__ = "starrocks"

    @staticmethod
    def _table(connection, metadata, value_type, *columns):
        table = Table(
            "executemany_target",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("v", value_type),
            *columns,
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        table.create(connection)
        return table

    @staticmethod
    def _loads_into(connection, table):
        return connection.execute(
            text("SELECT COUNT(*) FROM information_schema.loads WHERE DB_NAME = DATABASE() AND TABLE_NAME = :t"),
            {"t": table.name},
        ).scalar()

    def _loads_made_by(self, connection, table, run):
        before = self._loads_into(connection, table)
        result = run()
        return self._loads_into(connection, table) - before, result

    @testing.combinations(
        ("json", JSON(), _json_document),
        ("plain", String(20), str),
        ("array", ARRAY(INTEGER()), lambda n: [n, None, -n]),
        ("array_of_decimal", ARRAY(DECIMAL(10, 2)), lambda n: [Decimal(n) / 4]),
        ("map", MAP(VARCHAR(10), DATE()), lambda n: {f"k{n}": date(2024, 1, 1 + n % 28)}),
        ("struct", STRUCT(a=INTEGER(), s=VARCHAR(20)), lambda n: {"a": n, "s": f"it's {n}%"}),
        argnames="value_type,value",
        id_="iaa",
    )
    def test_the_rows_go_in_one_insert(self, connection, metadata, value_type, value):
        """Each element of an ARRAY, MAP or STRUCT is still rebuilt by its type in the one INSERT."""
        table = self._table(connection, metadata, value_type)
        rows = [{"id": n, "v": value(n)} for n in range(300)]

        loads, result = self._loads_made_by(connection, table, lambda: connection.execute(table.insert(), rows))

        eq_(loads, 1)
        eq_(result.rowcount, 300)
        stored = connection.execute(select(table.c.id, table.c.v).order_by(table.c.id)).all()
        eq_([tuple(row) for row in stored], [(row["id"], row["v"]) for row in rows])

    @testing.combinations(
        ("json", JSON(), False),
        ("plain", String(20), False),
        ("plain_text", String(20), True),
        argnames="value_type,as_text",
        id_="iaa",
    )
    def test_rows_past_starrocks_row_limit_go_in_a_second_insert(self, connection, metadata, value_type, as_text):
        """StarRocks takes at most 10,000 rows in one INSERT ... VALUES, and the driver would send more in one."""
        table = self._table(connection, metadata, value_type)
        rows = [{"id": n, "v": str(n)} for n in range(10001)]
        statement = text(f"INSERT INTO {table.name} (id, v) VALUES (:id, :v)") if as_text else table.insert()

        loads, result = self._loads_made_by(connection, table, lambda: connection.execute(statement, rows))

        eq_(loads, 2)
        eq_(result.rowcount, 10001)
        eq_(connection.execute(select(func.count(), func.sum(table.c.id))).one(), (10001, sum(range(10001))))

    def test_rows_up_to_starrocks_row_limit_go_to_the_driver(self, connection, metadata):
        """An executemany of bare placeholders the driver can send in one INSERT is left to it, as it was."""
        table = self._table(connection, metadata, String(20))
        rows = [{"id": n, "v": str(n)} for n in range(10000)]

        with testing.mock.patch.object(
            StarRocksDialect, "_values_pages", autospec=True, side_effect=StarRocksDialect._values_pages
        ) as dialect_pages:
            loads, result = self._loads_made_by(connection, table, lambda: connection.execute(table.insert(), rows))

        eq_(dialect_pages.call_count, 0)
        eq_(loads, 1)
        eq_(result.rowcount, 10000)

    def test_rows_past_the_drivers_statement_length_go_in_further_inserts(self, connection, metadata):
        """The driver builds no statement longer than its ``max_stmt_length``, a megabyte, from an executemany."""
        table = self._table(connection, metadata, JSON())
        rows = [{"id": n, "v": {"n": n, "pad": "x" * 2000}} for n in range(1000)]

        loads, _ = self._loads_made_by(connection, table, lambda: connection.execute(table.insert(), rows))

        eq_(loads, 3)
        eq_(connection.execute(select(func.count()).select_from(table)).scalar(), 1000)

    def test_a_failing_row_stores_none_of_the_rows(self, connection, metadata):
        """The rows of one INSERT are stored together or not at all."""
        table = self._table(connection, metadata, JSON(), Column("n", Integer, nullable=False))
        rows = [{"id": n, "v": {"n": n}, "n": None if n == 2 else n} for n in range(4)]

        with expect_raises(exc.DBAPIError):
            connection.execute(table.insert(), rows)

        eq_(connection.execute(select(func.count()).select_from(table)).scalar(), 0)

    def test_an_orm_flush_is_one_insert(self, connection, metadata):
        Base = declarative_base(metadata=metadata)

        class Document(Base):
            __tablename__ = "executemany_documents"
            __table_args__ = {
                "starrocks_distributed_by": "HASH(id) BUCKETS 1",
                "starrocks_properties": {"replication_num": "1"},
            }
            id = Column(Integer, primary_key=True, autoincrement=False)
            body = Column(JSON)

        metadata.create_all(connection)
        session = Session(bind=connection)

        def flush():
            session.add_all([Document(id=n, body=_json_document(n)) for n in range(50)])
            session.flush()

        loads, _ = self._loads_made_by(connection, Document.__table__, flush)
        session.close()

        eq_(loads, 1)
        eq_(
            connection.execute(select(Document.__table__.c.body).order_by(Document.__table__.c.id)).scalars().all(),
            [_json_document(n) for n in range(50)],
        )


class JsonNullAmongObjectsTest(fixtures.TestBase):
    """A JSON null inserted in one INSERT with JSON objects reads back as a JSON null, not as an empty object."""

    __backend__ = True
    __only_on__ = "starrocks"

    @staticmethod
    def _stored_json_text(connection, metadata, properties):
        table = Table(
            "json_null_among_objects",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("doc", JSON),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1", **properties},
        )
        table.create(connection)
        connection.execute(table.insert(), [{"id": 1, "doc": None}, {"id": 2, "doc": {"x": 1}}, {"id": 3, "doc": {"x": 2}}])
        return connection.execute(select(table.c.id, cast(table.c.doc, String)).order_by(table.c.id)).all()

    @FLAT_JSON_NULL_AMONG_OBJECTS_READS_AS_EMPTY
    def test_the_json_null_is_kept(self, connection, metadata):
        stored = self._stored_json_text(connection, metadata, {})
        eq_([tuple(row) for row in stored], [(1, "null"), (2, '{"x": 1}'), (3, '{"x": 2}')])

    @FLAT_JSON_PROPERTIES_NEED_4_0
    def test_the_json_null_is_kept_without_flat_json(self, connection, metadata):
        stored = self._stored_json_text(connection, metadata, {"flat_json.enable": "false"})
        eq_([tuple(row) for row in stored], [(1, "null"), (2, '{"x": 1}'), (3, '{"x": 2}')])
