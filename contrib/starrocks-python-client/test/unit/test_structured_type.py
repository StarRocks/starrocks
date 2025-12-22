#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from starrocks import datatype


def _sub_type_reprs(structured_type):
    return {repr(sub_type) for sub_type in structured_type.get_sub_item_types()}


def _col_spec(structured_type):
    return structured_type.get_col_spec()


class TestStructuredTypeSubItems:
    def test_array_with_scalar_items(self):
        array_type = datatype.ARRAY(datatype.INTEGER)

        assert _sub_type_reprs(array_type) == {repr(datatype.INTEGER())}

    def test_map_with_struct_and_json(self):
        map_type = datatype.MAP(
            datatype.STRING,
            datatype.ARRAY(
                datatype.STRUCT(
                    ("flag", datatype.BOOLEAN),
                    ("payload", datatype.JSON()),
                )
            ),
        )

        assert _sub_type_reprs(map_type) == {
            repr(datatype.STRING()),
            repr(
                datatype.ARRAY(
                    datatype.STRUCT(
                        ("flag", datatype.BOOLEAN),
                        ("payload", datatype.JSON()),
                    )
                )
            ),
            repr(
                datatype.STRUCT(
                    ("flag", datatype.BOOLEAN),
                    ("payload", datatype.JSON()),
                )
            ),
            repr(datatype.BOOLEAN()),
            repr(datatype.JSON()),
        }

    def test_struct_with_mixed_nested_types(self):
        struct_type = datatype.STRUCT(
            ("id", datatype.BIGINT),
            ("tags", datatype.ARRAY(datatype.STRING)),
            (
                "props",
                datatype.MAP(
                    datatype.VARCHAR(5),
                    datatype.STRUCT(
                        ("k", datatype.CHAR(3)),
                        ("v", datatype.DECIMAL(9, 3)),
                        ("extra", datatype.ARRAY(datatype.JSON())),
                    ),
                ),
            ),
        )

        assert _sub_type_reprs(struct_type) == {
            repr(datatype.BIGINT()),
            repr(datatype.ARRAY(datatype.STRING)),
            repr(datatype.STRING()),
            repr(
                datatype.MAP(
                    datatype.VARCHAR(5),
                    datatype.STRUCT(
                        ("k", datatype.CHAR(3)),
                        ("v", datatype.DECIMAL(9, 3)),
                        ("extra", datatype.ARRAY(datatype.JSON())),
                    ),
                )
            ),
            repr(datatype.VARCHAR(5)),
            repr(
                datatype.STRUCT(
                    ("k", datatype.CHAR(3)),
                    ("v", datatype.DECIMAL(9, 3)),
                    ("extra", datatype.ARRAY(datatype.JSON())),
                )
            ),
            repr(datatype.CHAR(3)),
            repr(datatype.DECIMAL(9, 3)),
            repr(datatype.ARRAY(datatype.JSON())),
            repr(datatype.JSON()),
        }


class TestStructuredTypeColSpec:
    def test_array_simple_col_spec(self):
        array_type = datatype.ARRAY(datatype.INTEGER)

        assert _col_spec(array_type) == "ARRAY<INTEGER>"

    def test_array_struct_col_spec(self):
        array_type = datatype.ARRAY(
            datatype.STRUCT(
                ("flag", datatype.BOOLEAN),
                ("payload", datatype.JSON()),
            )
        )

        assert _col_spec(array_type) == "ARRAY<STRUCT<flag BOOLEAN, payload JSON>>"

    def test_map_nested_col_spec(self):
        map_type = datatype.MAP(
            datatype.STRING,
            datatype.ARRAY(
                datatype.STRUCT(
                    ("flag", datatype.BOOLEAN),
                    ("payload", datatype.JSON()),
                )
            ),
        )

        assert (
            _col_spec(map_type)
            == "MAP<STRING, ARRAY<STRUCT<flag BOOLEAN, payload JSON>>>"
        )

    def test_struct_mixed_col_spec(self):
        struct_type = datatype.STRUCT(
            ("id", datatype.BIGINT),
            ("tags", datatype.ARRAY(datatype.STRING)),
            (
                "props",
                datatype.MAP(
                    datatype.VARCHAR(5),
                    datatype.STRUCT(
                        ("k", datatype.CHAR(3)),
                        ("v", datatype.DECIMAL(9, 3)),
                        ("extra", datatype.ARRAY(datatype.JSON())),
                    ),
                ),
            ),
        )

        assert (
            _col_spec(struct_type)
            == "STRUCT<id BIGINT, tags ARRAY<STRING>, props MAP<VARCHAR(5), STRUCT<k CHAR(3), v DECIMAL(9, 3), extra ARRAY<JSON>>>>"
        )
