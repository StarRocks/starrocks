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

"""Tables and columns named with words StarRocks reserves and MySQL does not, such as ``files`` and ``text``."""

from sqlalchemy import Column, Table, insert, inspect, select
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from starrocks import INTEGER, JSON, VARCHAR


class StarRocksReservedWordTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    def test_a_table_and_columns_named_with_reserved_words(self, metadata, connection):
        files = Table(
            "files",
            metadata,
            Column("id", INTEGER),
            Column("text", VARCHAR(20)),
            Column("json", JSON),
            Column("full", INTEGER),
            starrocks_properties={"replication_num": "1"},
        )
        metadata.create_all(connection)
        connection.execute(insert(files), {"id": 1, "text": "a", "json": {"k": 1}, "full": 2})
        eq_(connection.execute(select(files.c.text, files.c.full)).all(), [("a", 2)])
        eq_([column["name"] for column in inspect(connection).get_columns("files")], ["id", "text", "json", "full"])
