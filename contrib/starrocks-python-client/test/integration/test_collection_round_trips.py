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

"""ARRAY, MAP and STRUCT values whose text StarRocks' own string casts misread, bound and read back exactly.

StarRocks' cast of text to ARRAY strips quotes without reading JSON escapes, and it has no cast of text to MAP or
STRUCT; a MAP key of a DATE, DATETIME or DECIMAL is not a JSON object key either. Each value is bound through
``insert().values`` and through ``executemany``, and must read back as it was.
"""

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Column, Integer, Table, cast, literal, select, testing
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.testing import eq_, fixtures

from starrocks.datatype import ARRAY, DATE, DATETIME, DECIMAL, INTEGER, MAP, STRUCT, VARCHAR


class CollectionRoundTripTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    @testing.combinations(
        (
            "strings_with_escapes",
            ARRAY(VARCHAR(40)),
            ["été 中文 \U0001f600", "back\\slash", "new\nline", "tab\there", 'quote"d', "it's", "\\n"],
        ),
        ("control_characters", ARRAY(VARCHAR(20)), ["\x01\x1f", "\r\n", "nul\x00byte"]),
        (
            "array_of_struct",
            ARRAY(STRUCT(n=INTEGER(), s=VARCHAR(10), d=DATE())),
            [{"n": 1, "s": "x\ny", "d": date(2024, 2, 29)}, None, {"n": None, "s": None, "d": None}],
        ),
        (
            "array_of_map",
            ARRAY(MAP(VARCHAR(10), INTEGER())),
            [{"a\"b": 1, "é": None}, {}, None],
        ),
        ("array_of_map_of_integer_keys", ARRAY(MAP(INTEGER(), VARCHAR(10))), [{1: "x", 2: "y"}, {}]),
        ("map_of_date_keys", MAP(DATE(), INTEGER()), {date(2024, 1, 2): 1, date(1, 1, 1): 2}),
        (
            "map_of_datetime_keys",
            MAP(DATETIME(), VARCHAR(5)),
            {datetime(2024, 1, 2, 3, 4, 5, 123456): "a", datetime(2024, 1, 2): "b"},
        ),
        ("map_of_decimal_keys", MAP(DECIMAL(10, 2), INTEGER()), {Decimal("1.25"): 1, Decimal("-0.50"): 2}),
        (
            "struct_of_map_of_date_keys",
            STRUCT(m=MAP(DATE(), ARRAY(DECIMAL(5, 2)))),
            {"m": {date(2024, 1, 2): [Decimal("1.50"), None]}},
        ),
        argnames="type_,value",
        id_="iaa",
    )
    def test_value_round_trips(self, connection, metadata, type_, value):
        table = Table(
            "collection_round_trip",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("v", type_),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )
        table.create(connection)
        connection.execute(table.insert().values(id=1, v=value))
        connection.execute(table.insert(), [{"id": 2, "v": value}, {"id": 3, "v": value}])

        eq_(connection.execute(select(table.c.v).order_by(table.c.id)).scalars().all(), [value, value, value])


class CollectionValuesInOrmRowsTest(fixtures.TestBase):
    """A legacy ``Query`` of an entity and other columns uniques its rows by each value its type calls hashable.

    A list or dict value can't be hashed, so ARRAY, MAP and STRUCT say so, as SQLAlchemy's own ARRAY and JSON do,
    and an ARRAY read ``as_tuple`` is hashable.
    """

    __backend__ = True
    __only_on__ = "starrocks"

    def test_a_query_of_an_entity_and_collection_columns_reads_its_rows(self, connection, metadata):
        Base = declarative_base(metadata=metadata)

        class Document(Base):
            __tablename__ = "orm_collection_values"
            __table_args__ = {"starrocks_duplicate_key": "id"}
            id = Column(Integer, primary_key=True, autoincrement=False)
            numbers = Column(ARRAY(INTEGER()))
            words = Column(MAP(VARCHAR(5), VARCHAR(5)))
            point = Column(STRUCT(x=INTEGER()))
            number_tuple = Column(ARRAY(INTEGER(), as_tuple=True))

        metadata.create_all(connection)
        session = Session(bind=connection)
        session.add(Document(id=1, numbers=[1, 2], words={"a": "b"}, point={"x": 1}, number_tuple=(3,)))
        session.flush()
        left_out_cast = cast(literal("[4]"), ARRAY(INTEGER())).label("cast")
        columns = (Document.numbers, Document.words, Document.point, Document.number_tuple, left_out_cast)
        rows = session.query(Document, *columns).all()
        session.close()
        eq_([tuple(row[1:]) for row in rows], [([1, 2], {"a": "b"}, {"x": 1}, (3,), [4])])
