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

import dataclasses
import json
import re
from typing import Any, Union

from sqlalchemy.dialects.mysql.types import DATETIME
from sqlalchemy.dialects.mysql.types import TIME
from sqlalchemy.dialects.mysql.types import TIMESTAMP
from sqlalchemy.dialects.mysql.base import _DecodingRow
from sqlalchemy.dialects.mysql.reflection import _re_compile
from sqlalchemy import log
from sqlalchemy import types as sqltypes
from sqlalchemy import util


# kw_only is added in python 3.10
# https://docs.python.org/3/library/dataclasses.html#dataclasses.dataclass
@dataclasses.dataclass(**dict(kw_only=True) if "KW_ONLY" in dataclasses.__all__ else {})
class ReflectedState(object):
    """Stores informations about table or view."""

    table_name: Union[str, None] = None
    columns: list[dict] = dataclasses.field(default_factory=list)
    table_options: dict[str, str] = dataclasses.field(default_factory=dict)
    keys: list[dict] = dataclasses.field(default_factory=list)
    fk_constraints: list[dict] = dataclasses.field(default_factory=list)
    ck_constraints: list[dict] = dataclasses.field(default_factory=list)


@log.class_logger
class StarRocksTableDefinitionParser(object):
    """Parses content of information_schema tables to get table information."""

    def __init__(self, dialect, preparer):
        self.dialect = dialect
        self.preparer = preparer
        self._re_csv_int = _re_compile(r"\d+")
        self._re_csv_str = _re_compile(r"\x27(?:\x27\x27|[^\x27])*\x27")

    def parse(
        self,
        table: _DecodingRow,
        table_config: _DecodingRow,
        columns: list[_DecodingRow],
        column_autoinc: dict[str, bool],
        charset: str,
    ) -> ReflectedState:
        return ReflectedState(
            table_name=table["TABLE_NAME"],
            columns=[self._parse_column(column=column, col_autoinc=column_autoinc) for column in columns],
            table_options=self._parse_table_options(
                table=table, table_config=table_config, columns=columns
            ),
            keys=[
                {
                    "type": self._get_key_type(columns=columns),
                    "columns": [
                        (c, None, None) for c in self._get_key_columns(columns=columns)
                    ],
                    "parser": None,
                    "name": None,
                }
            ],
        )

    def _parse_column_type(self, column: _DecodingRow) -> Any:
        """
        Parse column type from information_schema.columns table.
        It splits column type into type and arguments.
        After that it creates instance of column type.
        """
        pattern = r"^(?P<type>\w+)(?:\s*\((?P<args>.*?)\))?$"
        match = re.match(pattern, column["COLUMN_TYPE"])
        type_ = match.group("type")
        args = match.group("args")
        try:
            col_type = self.dialect.ischema_names[type_]
        except KeyError:
            util.warn(
                "Did not recognize type '%s' of column '%s'"
                % (type_, column["COLUMN_NAME"])
            )
            col_type = sqltypes.NullType

        # Column type positional arguments eg. varchar(32)
        if args is None or args == "":
            type_args = []
        elif args[0] == "'" and args[-1] == "'":
            type_args = self._re_csv_str.findall(args)
        else:
            type_args = [int(v) for v in self._re_csv_int.findall(args)]

        # Column type keyword options
        type_kw = {}

        if issubclass(col_type, (DATETIME, TIME, TIMESTAMP)):
            if type_args:
                type_kw["fsp"] = type_args.pop(0)

        if col_type.__name__ == "LARGEINT":
            type_instance = col_type()
        else:
            type_instance = col_type(*type_args, **type_kw)
        return type_instance

    def _parse_column(self, column: _DecodingRow, col_autoinc: dict) -> dict:
        """
        Parse column from information_schema.columns table.
        It returns dictionary with column information expected by sqlalchemy.
        """
        col = {
            "name": column["COLUMN_NAME"],
            "type": self._parse_column_type(column=column),
            "nullable": column["IS_NULLABLE"] == "YES",
            "default": column["COLUMN_DEFAULT"],
            "autoincrement": col_autoinc.get(column["COLUMN_NAME"], False),
            "comment": column["COLUMN_COMMENT"],
        }
        if column['GENERATION_EXPRESSION'] is not None:
            col["computed"] = {"sqltext": column["GENERATION_EXPRESSION"]}
        return col

    def _get_key_columns(self, columns: list[_DecodingRow]) -> list[str]:
        """
        Get list of key columns from information_schema.columns table.
        It returns list of column names that are part of key.
        """
        sorted_columns = sorted(columns, key=lambda col: col["ORDINAL_POSITION"])
        return [c["COLUMN_NAME"] for c in sorted_columns if c["COLUMN_KEY"]]

    def _get_key_type(self, columns: list[_DecodingRow]) -> str:
        """
        Get key type from information_schema.columns table.
        It returns string representation of key type.
        It assumes only one key type is present in columns.
        """
        key_map = {
            "PRI": "PRIMARY",
            "UNI": "UNIQUE",
            "DUP": "DUPLICATE",
            "AGG": "AGGREGATE",
        }
        key_types = list(set([c["COLUMN_KEY"] for c in columns if c["COLUMN_KEY"]]))
        if len(key_types) > 1:
            raise NotImplementedError(f"Multiple key types found: {key_types}")
        if not key_types:
            return ""
        return key_map[key_types[0]]

    def _get_key_desc(self, columns: list[_DecodingRow]) -> str:
        """
        Get key description from information_schema.columns table.
        It returns string representation of key description.
        """
        quoted_cols = [
            self.preparer.quote_identifier(col)
            for col in self._get_key_columns(columns=columns)
        ]
        return f"{self._get_key_type(columns=columns)} KEY({', '.join(quoted_cols)})"

    def _get_distribution(self, table_config: _DecodingRow) -> str:
        """
        Get distribution from information_schema.tables table.
        It returns string representation of distribution option.
        """
        distribution_cols = table_config["DISTRIBUTE_KEY"]
        distribution_str = f"({distribution_cols})" if distribution_cols else ""
        return f'{table_config["DISTRIBUTE_TYPE"]}{distribution_str}'

    def _parse_table_options(
        self,
        table: _DecodingRow,
        table_config: _DecodingRow,
        columns: list[_DecodingRow],
    ) -> dict:
        """
        Parse table options from information_schema.tables table.
        It returns dictionary with table options expected by sqlalchemy.
        """
        return {
            f"{self.dialect.name}_engine": table_config["TABLE_ENGINE"],
            f"{self.dialect.name}_key_desc": self._get_key_desc(columns),
            f"{self.dialect.name}_comment": table["TABLE_COMMENT"] if table["TABLE_COMMENT"] != 'OLAP' else None,
            f"{self.dialect.name}_partition_by": table_config["PARTITION_KEY"],
            f"{self.dialect.name}_distribution": self._get_distribution(table_config),
            f"{self.dialect.name}_order_by": table_config["SORT_KEY"],
            f"{self.dialect.name}_properties": tuple(
                json.loads(table_config["PROPERTIES"] or "{}").items()
            ),
        }
