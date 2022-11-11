#! /usr/bin/python3
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

from dataclasses import dataclass

from dbt.adapters.base.column import Column

@dataclass
class StarRocksColumn(Column):
    @property
    def quoted(self) -> str:
        return "`{}`".format(self.column)

    def __repr__(self) -> str:
        return f"<StarRocksColumn {self.name} ({self.data_type})>"

    def is_string(self) -> bool:
        return self.dtype.lower() in ["text", "character varying", "character", "varchar",
                                      # starrocks
                                      "char", "string"]

    def is_float(self):
        return self.dtype.lower() in [
            # floats
            "real",
            "float4",
            "float",
            "double precision",
            "float8",
            # starrocks
            "double",
        ]

    def is_integer(self) -> bool:
        return self.dtype.lower() in [
            # real types
            "smallint",
            "integer",
            "bigint",
            "smallserial",
            "serial",
            "bigserial",
            # aliases
            "int2",
            "int4",
            "int8",
            "serial2",
            "serial4",
            "serial8",
            # starrocks
            "largeint",
            "tinyint",
        ]
