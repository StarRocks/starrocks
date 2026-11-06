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

"""Binary values bound through ``starrocks+asyncmy``, which writes bytes as ``_binary'...'`` unless told otherwise."""

from sqlalchemy import Column, Integer, LargeBinary, PickleType, Table, select, testing
from sqlalchemy.testing import eq_, fixtures

from starrocks.datatype import ARRAY, VARBINARY


class AsyncmyBinaryBindTest(fixtures.TablesTest):
    __backend__ = True
    __only_on__ = "starrocks+asyncmy"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "asyncmy_binary",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("large", LargeBinary),
            Column("var", VARBINARY(20)),
            Column("pickled", PickleType),
            Column("elements", ARRAY(VARBINARY())),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )

    @testing.combinations(
        ("ascii", b"abc"),
        ("quotes_and_backslash", b"it's \\ \"q\""),
        ("not_utf8", b"\xff\xfe\x00"),
        ("empty", b""),
        id_="ia",
        argnames="value",
    )
    def test_bytes_round_trip(self, connection, value):
        table = self.tables.asyncmy_binary
        row = {"large": value, "var": value, "pickled": {"v": value}, "elements": [value, None]}
        connection.execute(table.insert(), {"id": 1, **row})
        connection.execute(table.insert(), [{"id": 2, **row}, {"id": 3, **row}])

        rows = connection.execute(select(table.c.large, table.c.var, table.c.pickled, table.c.elements)).all()

        eq_([tuple(r) for r in rows], [tuple(row.values())] * 3)
