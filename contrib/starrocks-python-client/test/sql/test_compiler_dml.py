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

"""UPDATE, DELETE and INSERT compilation and execution."""

import uuid

from sqlalchemy import (
    JSON,
    Column,
    Integer,
    MetaData,
    Table,
    delete,
    exc,
    insert,
    literal,
    select,
    testing,
    text,
    update,
)
from sqlalchemy.testing import eq_, expect_raises_message, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from starrocks.dialect import StarRocksDialect


METADATA = MetaData()
A = Table("a", METADATA, Column("id", Integer), Column("v", Integer))
B = Table("b", METADATA, Column("id", Integer), Column("w", Integer))
C = Table("c", METADATA, Column("id", Integer), Column("x", Integer))


class MultiTableDmlCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()

    def test_update_from_one_table(self):
        self.assert_compile(
            update(A).values(v=B.c.w).where(A.c.id == B.c.id),
            "UPDATE a SET v=b.w FROM b WHERE a.id = b.id",
        )

    def test_update_from_two_tables(self):
        self.assert_compile(
            update(A).values(v=B.c.w + C.c.x).where(A.c.id == B.c.id).where(B.c.id == C.c.id),
            "UPDATE a SET v=(b.w + c.x) FROM b, c WHERE a.id = b.id AND b.id = c.id",
        )

    def test_update_single_table_has_no_from(self):
        self.assert_compile(update(A).values(v=1).where(A.c.id == 2), "UPDATE a SET v=%s WHERE a.id = %s")

    def test_delete_using_one_table(self):
        self.assert_compile(
            delete(A).where(A.c.id == B.c.id).where(B.c.w == 3),
            "DELETE FROM a USING b WHERE a.id = b.id AND b.w = %s",
        )

    def test_delete_using_two_tables(self):
        self.assert_compile(
            delete(A).where(A.c.id == B.c.id).where(B.c.id == C.c.id),
            "DELETE FROM a USING b, c WHERE a.id = b.id AND b.id = c.id",
        )


class UnsupportedLimitTest(fixtures.TestBase):
    """StarRocks has no ``UPDATE ... LIMIT`` or ``DELETE ... LIMIT``, so the dialect must not accept one."""

    @testing.combinations(
        (lambda: update(A).values(v=1), "Update"),
        (lambda: delete(A), "Delete"),
        argnames="statement_factory, statement_class",
    )
    def test_starrocks_limit_is_rejected(self, statement_factory, statement_class):
        with expect_raises_message(
            exc.ArgumentError,
            f"Argument 'starrocks_limit' is not accepted by dialect 'starrocks' on behalf of .*{statement_class}",
        ):
            statement_factory().with_dialect_options(starrocks_limit=3)


class InsertOverwriteAndLabelCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()

    @testing.combinations(
        ({}, "INSERT INTO a (id, v) VALUES (%s, %s)"),
        ({"starrocks_overwrite": True}, "INSERT OVERWRITE a (id, v) VALUES (%s, %s)"),
        ({"starrocks_overwrite": False}, "INSERT INTO a (id, v) VALUES (%s, %s)"),
        ({"starrocks_label": "l1"}, "INSERT INTO a WITH LABEL l1 (id, v) VALUES (%s, %s)"),
        (
            {"starrocks_overwrite": True, "starrocks_label": "l1"},
            "INSERT OVERWRITE a WITH LABEL l1 (id, v) VALUES (%s, %s)",
        ),
        ({"starrocks_label": "my-label"}, "INSERT INTO a WITH LABEL `my-label` (id, v) VALUES (%s, %s)"),
        argnames="dialect_options, expected",
    )
    def test_insert_values(self, dialect_options, expected):
        self.assert_compile(insert(A).values(id=1, v=2).with_dialect_options(**dialect_options), expected)

    def test_insert_from_select(self):
        self.assert_compile(
            insert(A)
            .from_select(["id", "v"], select(B.c.id, B.c.w))
            .with_dialect_options(starrocks_overwrite=True, starrocks_label="l1"),
            "INSERT OVERWRITE a WITH LABEL l1 (id, v) SELECT b.id, b.w FROM b",
        )

    def test_insert_into_schema_table(self):
        schema_table = Table("t", MetaData(), Column("id", Integer), schema="db1")
        self.assert_compile(
            insert(schema_table).values(id=1).with_dialect_options(starrocks_overwrite=True, starrocks_label="l1"),
            "INSERT OVERWRITE db1.t WITH LABEL l1 (id) VALUES (%s)",
        )

    @testing.combinations(
        ("empty", ""),
        ("not_a_string", 7),
        ("none", None),
        ("backquote", "lbl`q"),
        ("space", "a b"),
        ("dot", "a.b"),
        ("not_ascii", "é"),
        ("too_long", "x" * 129),
        argnames="label",
        id_="ia",
    )
    def test_invalid_label_is_rejected(self, label):
        """StarRocks takes 1 to 128 ASCII letters, digits, ``_`` and ``-``, and drops a backquote from a label."""
        statement = insert(A).values(id=1)
        statement = statement.with_dialect_options(starrocks_label=label)
        if label is None:
            self.assert_compile(statement, "INSERT INTO a (id) VALUES (%s)")
            return
        with expect_raises_message(exc.CompileError, "starrocks_label must be 1 to 128 ASCII letters"):
            statement.compile(dialect=self.__dialect__)

    @testing.combinations(
        ("longest", "x" * 128, "x" * 128),
        ("digits", "123", "`123`"),
        ("uuid", "0b8e3c58-6f1d-4a3e-9c1e-2d5f8a7b6c4d", "`0b8e3c58-6f1d-4a3e-9c1e-2d5f8a7b6c4d`"),
        argnames="label, label_sql",
        id_="iaa",
    )
    def test_valid_label_is_accepted(self, label, label_sql):
        self.assert_compile(
            insert(A).values(id=1).with_dialect_options(starrocks_label=label),
            f"INSERT INTO a WITH LABEL {label_sql} (id) VALUES (%s)",
        )


class InsertOverwriteAndLabelTest(fixtures.TablesTest):
    __backend__ = True
    __only_on__ = "starrocks"

    @classmethod
    def define_tables(cls, metadata):
        Table("overwrite_target", metadata, Column("id", Integer, primary_key=True, autoincrement=False))
        Table(
            "overwrite_documents",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("doc", JSON),
        )

    @staticmethod
    def _loads_labelled(connection, label):
        return connection.execute(
            text("SELECT COUNT(*) FROM information_schema.loads WHERE label = :label"), {"label": label}
        ).scalar()

    def test_overwrite_replaces_existing_rows(self, connection):
        target = self.tables.overwrite_target
        connection.execute(insert(target).values(id=1))
        connection.execute(insert(target).values(id=2).with_dialect_options(starrocks_overwrite=True))
        eq_(connection.execute(select(target.c.id)).all(), [(2,)])

    def test_overwrite_from_select(self, connection):
        target = self.tables.overwrite_target
        connection.execute(insert(target).values(id=1))
        connection.execute(
            insert(target).from_select(["id"], select(literal(5))).with_dialect_options(starrocks_overwrite=True)
        )
        eq_(connection.execute(select(target.c.id)).all(), [(5,)])

    def test_label_names_the_load_job(self, connection):
        target = self.tables.overwrite_target
        label = f"sqla_label_{uuid.uuid4().hex}"
        connection.execute(insert(target).values(id=3).with_dialect_options(starrocks_label=label))
        loads = connection.execute(
            text("SELECT label FROM information_schema.loads WHERE label = :label"), {"label": label}
        ).all()
        eq_(loads, [(label,)])

    def test_a_label_holding_a_backquote_is_refused_since_starrocks_renames_it(self, connection):
        target = self.tables.overwrite_target
        label = f"sqla_label_{uuid.uuid4().hex}"
        connection.exec_driver_sql(f"INSERT INTO overwrite_target WITH LABEL `{label}``q` VALUES (9)")
        eq_((self._loads_labelled(connection, f"{label}`q"), self._loads_labelled(connection, f"{label}q")), (0, 1))
        with expect_raises_message(exc.CompileError, "starrocks_label must be 1 to 128 ASCII letters"):
            connection.execute(insert(target).values(id=3).with_dialect_options(starrocks_label=f"{label}`r"))

    def test_an_executemany_overwrite_stores_every_row(self, connection):
        target = self.tables.overwrite_target
        connection.execute(insert(target).values(id=1))
        connection.execute(insert(target).with_dialect_options(starrocks_overwrite=True), [{"id": 7}, {"id": 8}])
        eq_(sorted(connection.execute(select(target.c.id)).all()), [(7,), (8,)])

    def test_an_executemany_overwrite_of_json_stores_every_row(self, connection):
        """The driver would send these rows one statement each, as JSON's VALUES row holds a bind expression."""
        documents = self.tables.overwrite_documents
        connection.execute(insert(documents).values(id=1, doc={"old": True}))
        rows = [{"id": 7, "doc": {"n": 7}}, {"id": 8, "doc": [8]}]
        connection.execute(insert(documents).with_dialect_options(starrocks_overwrite=True), rows)
        stored = connection.execute(select(documents.c.id, documents.c.doc).order_by(documents.c.id)).all()
        eq_([tuple(row) for row in stored], [(7, {"n": 7}), (8, [8])])

    def test_an_executemany_with_a_label_is_one_load(self, connection):
        target = self.tables.overwrite_target
        label = f"sqla_label_{uuid.uuid4().hex}"
        connection.execute(insert(target).values(id=1))
        connection.execute(insert(target).with_dialect_options(starrocks_label=label), [{"id": 7}, {"id": 8}])
        eq_(sorted(connection.execute(select(target.c.id)).all()), [(1,), (7,), (8,)])
        eq_(self._loads_labelled(connection, label), 1)

    @testing.combinations(
        ("commented_overwrite", "/* load */ INSERT OVERWRITE overwrite_target VALUES (:id)", [7, 8], [(7,), (8,)]),
        ("overwrite_of_an_expression", "INSERT OVERWRITE overwrite_target VALUES (abs(:id))", [-7, 8], [(7,), (8,)]),
        ("label", "INSERT INTO overwrite_target WITH LABEL {label} VALUES (:id)", [7, 8], [(1,), (7,), (8,)]),
        id_="iaaa",
        argnames="statement,ids,stored",
    )
    def test_an_executemany_of_text_stores_every_row(self, connection, statement, ids, stored):
        target = self.tables.overwrite_target
        label = f"sqla_label_{uuid.uuid4().hex}"
        connection.execute(insert(target).values(id=1))
        connection.execute(text(statement.format(label=label)), [{"id": id_} for id_ in ids])
        eq_(sorted(connection.execute(select(target.c.id)).all()), stored)

    def test_an_executemany_of_driver_text_stores_every_row(self, connection):
        target = self.tables.overwrite_target
        connection.execute(insert(target).values(id=1))
        connection.exec_driver_sql("INSERT OVERWRITE overwrite_target VALUES (%s)", [(7,), (8,)])
        eq_(sorted(connection.execute(select(target.c.id)).all()), [(7,), (8,)])

    @testing.combinations(
        ("overwrite", {"starrocks_overwrite": True}),
        ("label", {"starrocks_label": "sqla_label_never_used"}),
        id_="ia",
        argnames="options",
    )
    def test_rows_that_need_two_statements_are_refused(self, connection, options):
        target = self.tables.overwrite_target
        connection.execute(insert(target).values(id=1))
        with testing.mock.patch.object(StarRocksDialect, "_VALUES_ROWS_PER_STATEMENT", 1):
            with expect_raises_message(exc.InvalidRequestError, "in one statement, but these rows need 2"):
                connection.execute(insert(target).with_dialect_options(**options), [{"id": 7}, {"id": 8}])
        eq_(connection.execute(select(target.c.id)).all(), [(1,)])

    def test_text_the_driver_would_send_row_by_row_is_refused(self, connection):
        target = self.tables.overwrite_target
        connection.execute(insert(target).values(id=1))
        with expect_raises_message(exc.InvalidRequestError, "the driver would send this executemany once per row"):
            connection.execute(text("INSERT OVERWRITE overwrite_target SELECT :id"), [{"id": 7}, {"id": 8}])
        eq_(connection.execute(select(target.c.id)).all(), [(1,)])

    def test_overwrite_with_several_rows_in_one_statement(self, connection):
        target = self.tables.overwrite_target
        connection.execute(insert(target).values(id=1))
        connection.execute(
            insert(target).values([{"id": 7}, {"id": 8}]).with_dialect_options(starrocks_overwrite=True)
        )
        eq_(sorted(connection.execute(select(target.c.id)).all()), [(7,), (8,)])

    def test_reused_label_is_rejected_by_server(self, connection):
        target = self.tables.overwrite_target
        label = f"sqla_label_{uuid.uuid4().hex}"
        connection.execute(insert(target).values(id=3).with_dialect_options(starrocks_label=label))
        with expect_raises_message(exc.DBAPIError, "has already been used"):
            connection.execute(insert(target).values(id=4).with_dialect_options(starrocks_label=label))
