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

"""Autogenerate leaves out the constraints StarRocks tables cannot hold, and matches index names as StarRocks does."""

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Column, ForeignKey, Index, MetaData, Table, UniqueConstraint, testing
from sqlalchemy.testing import fixtures

import starrocks.alembic  # noqa: F401
from starrocks.datatype import INTEGER, VARCHAR


class UnholdableConstraintsTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    @testing.requires.sqlalchemy_2
    def test_no_constraint_ops_for_a_matching_table(self, connection, metadata):
        """The model's constraints were dropped by CREATE TABLE, so a matching model has nothing to add."""
        Table(
            "constraint_parent",
            metadata,
            Column("id", INTEGER, nullable=False),
            starrocks_primary_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )
        Table(
            "constraint_child",
            metadata,
            Column("id", INTEGER, nullable=False),
            Column("parent_id", INTEGER, ForeignKey("constraint_parent.id")),
            Column("code", INTEGER, unique=True),
            Column("other_code", INTEGER),
            UniqueConstraint("other_code", name="uq_constraint_child_other_code"),
            starrocks_primary_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )
        metadata.create_all(connection)

        context = MigrationContext.configure(
            connection,
            opts={"include_name": lambda name, type_, parents: type_ != "table" or name.startswith("constraint_")},
        )
        assert compare_metadata(context, metadata) == []


class IndexNameCaseTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    def test_an_index_named_in_another_case_is_the_same_index(self, connection):
        """StarRocks matches index names without regard to case, so ``idx_mixed`` is the database's ``IDX_Mixed``."""
        connection.exec_driver_sql(
            "CREATE TABLE index_case (id INT NOT NULL, v VARCHAR(10), INDEX IDX_Mixed (v) USING BITMAP) "
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
        )
        try:
            metadata = MetaData()
            Table(
                "index_case",
                metadata,
                Column("id", INTEGER, nullable=False),
                Column("v", VARCHAR(10)),
                Index("idx_mixed", "v", starrocks_using="BITMAP"),
                starrocks_duplicate_key="id",
                starrocks_distributed_by="HASH(id) BUCKETS 1",
            )
            context = MigrationContext.configure(
                connection, opts={"include_name": lambda name, type_, parents: type_ != "table" or name == "index_case"}
            )
            assert compare_metadata(context, metadata) == []
        finally:
            connection.exec_driver_sql("DROP TABLE index_case")


class IndexOptionsTest(fixtures.TestBase):
    """Autogenerate compares an index's type, comment and properties, which StarRocks can only change by recreating."""

    __backend__ = True
    __only_on__ = "starrocks"

    @staticmethod
    def _differences(connection, **index_options):
        metadata = MetaData()
        Table(
            "index_options",
            metadata,
            Column("id", INTEGER, nullable=False),
            Column("v", VARCHAR(10)),
            Index("idx_v", "v", **index_options),
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )
        context = MigrationContext.configure(
            connection, opts={"include_name": lambda name, type_, parents: type_ != "table" or name == "index_options"}
        )
        return [diff[0] for diff in compare_metadata(context, metadata)]

    @testing.combinations(
        ("same_with_the_properties_it_defaulted", {"starrocks_using": "NGRAMBF", "starrocks_comment": "a"}, []),
        (
            "same_with_a_property_stated",
            {"starrocks_using": "NGRAMBF", "starrocks_comment": "a", "starrocks_properties": {"gram_num": "4"}},
            [],
        ),
        ("another_type", {"starrocks_using": "BITMAP", "starrocks_comment": "a"}, ["remove_index", "add_index"]),
        ("another_comment", {"starrocks_using": "NGRAMBF", "starrocks_comment": "b"}, ["remove_index", "add_index"]),
        ("no_comment_stated", {"starrocks_using": "NGRAMBF"}, []),
        ("no_type_or_comment_stated", {}, []),
        ("comment_removed", {"starrocks_using": "NGRAMBF", "starrocks_comment": ""}, ["remove_index", "add_index"]),
        (
            "another_property",
            {"starrocks_using": "NGRAMBF", "starrocks_comment": "a", "starrocks_properties": {"gram_num": "3"}},
            ["remove_index", "add_index"],
        ),
        argnames="index_options,expected",
        id_="iaa",
    )
    def test_an_index_that_differs_in_type_comment_or_property_is_recreated(self, connection, index_options, expected):
        connection.exec_driver_sql(
            "CREATE TABLE index_options (id INT NOT NULL, v VARCHAR(10), "
            "INDEX idx_v (v) USING NGRAMBF (\"gram_num\" = \"4\") COMMENT 'a') "
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')"
        )
        try:
            assert self._differences(connection, **index_options) == expected
        finally:
            connection.exec_driver_sql("DROP TABLE index_options")
