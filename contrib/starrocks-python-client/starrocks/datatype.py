#! /usr/bin/python3
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

from datetime import date, datetime
from inspect import isclass
import re
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union

import sqlalchemy.dialects.mysql.types as mysql_types
from sqlalchemy.engine import Dialect
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import UserDefinedType


class BOOLEAN(sqltypes.BOOLEAN):
    __visit_name__ = "BOOLEAN"


class TINYINT(mysql_types.TINYINT):
    __visit_name__ = "TINYINT"


class SMALLINT(mysql_types.SMALLINT):
    __visit_name__ = "SMALLINT"


class INTEGER(mysql_types.INTEGER):
    __visit_name__ = "INTEGER"


class BIGINT(mysql_types.BIGINT):
    __visit_name__ = "BIGINT"


class LARGEINT(sqltypes.Integer):
    __visit_name__ = "LARGEINT"


class DECIMAL(mysql_types.DECIMAL):
    __visit_name__ = "DECIMAL"


class DOUBLE(mysql_types.DOUBLE):
    __visit_name__ = "DOUBLE"


class FLOAT(mysql_types.FLOAT):
    __visit_name__ = "FLOAT"


class CHAR(mysql_types.CHAR):
    __visit_name__ = "CHAR"


class VARCHAR(mysql_types.VARCHAR):
    __visit_name__ = "VARCHAR"


class STRING(mysql_types.TEXT):
    __visit_name__ = "STRING"


class BINARY(sqltypes.BINARY):
    __visit_name__ = "BINARY"


class VARBINARY(sqltypes.VARBINARY):
    __visit_name__ = "VARBINARY"


class DATETIME(mysql_types.DATETIME):
    __visit_name__ = "DATETIME"

    _reg = re.compile(r"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)\.?(\d+)?")

    def result_processor(self, dialect: Dialect, coltype: object):
        def process(value):
            if isinstance(value, str):
                m = self._reg.match(value)
                if not m:
                    raise ValueError(
                        "could not parse %r as a datetime value" % (value,)
                    )
                return datetime(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process

class DATE(sqltypes.DATE):
    __visit_name__ = "DATE"

    def literal_processor(self, dialect: Dialect) -> Callable[[date], str]:
        def process(value: date) -> str:
            return f"TO_DATE('{value}')"

        return process

    _reg = re.compile(r"(\d+)-(\d+)-(\d+)")

    def result_processor(self, dialect: Dialect, coltype: object):
        def process(value):
            if isinstance(value, str):
                m = self._reg.match(value)
                if not m:
                    raise ValueError("could not parse %r as a date value" % (value,))
                return date(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process


class HLL(sqltypes.Numeric):
    __visit_name__ = "HLL"


class BITMAP(sqltypes.Numeric):
    __visit_name__ = "BITMAP"


class PERCENTILE(sqltypes.Numeric):
    __visit_name__ = "PERCENTILE"


class StructuredType(UserDefinedType):
    @staticmethod
    def _check_subtype(subtype_obj: Union[sqltypes.TypeEngine, type]) -> sqltypes.TypeEngine:
        """
        Check if the subtype is a valid structured type.
        return: an instance of a type, rather than a type itself
        """
        if isclass(subtype_obj):
            if issubclass(subtype_obj, StructuredType):
                raise TypeError(f"'{subtype_obj.__name__}' should be an instance of StructuredType, not a class")
            return subtype_obj()
        return subtype_obj

    def get_col_spec(self, **kw) -> str:
        return "InvalidStructuredType<>"

    def get_sub_type_col_spec(self, sub_type, **kw) -> str:
        if hasattr(sub_type, 'get_col_spec'):
            return sub_type.get_col_spec(**kw)
        else:
            return str(sub_type)

    def get_sub_item_types(self) -> Set[sqltypes.TypeEngine]:
        """
        Get all the sub item types of this structured type recursively.
        Which is need for sqlacodegen to import correct types.
        """
        raise NotImplementedError("get_sub_item_types is not implemented for this pure Structuredtype")


class ARRAY(StructuredType):
    """
    Usage:
        ARRAY(item_type)

    Examples:
        ARRAY(INTEGER)
        ARRAY(ARRAY(STRING))
        ARRAY(STRUCT(name=STRING, address=MAP(STRING, ARRAY(STRING))))
    """

    __visit_name__ = "ARRAY"

    def __init__(self, item_type: sqltypes.TypeEngine, **kwargs):
        self.item_type = self._check_subtype(item_type)
        super().__init__(**kwargs)

    @property
    def python_type(self) -> Optional[Type[List[Any]]]:
        return list

    def __repr__(self):
        return f"ARRAY({repr(self.item_type)})"

    def get_col_spec(self, **kw) -> str:
        inner_type_sql = self.get_sub_type_col_spec(self.item_type)
        return f"ARRAY<{inner_type_sql}>"

    def get_sub_item_types(self) -> Set[sqltypes.TypeEngine]:
        types = {self.item_type}
        if hasattr(self.item_type, 'get_sub_item_types'):
            types.update(self.item_type.get_sub_item_types())
        return types

class MAP(StructuredType):
    """
    Usage:
        MAP(key_type, value_type)

    Examples:
        MAP(INTEGER, STRING)
        MAP(STRING, MAP(INTEGER, STRING))
        MAP(STRING, STRUCT(name=STRING, age=ARRAY(INTEGER)))
    """

    __visit_name__ = "MAP"

    def __init__(self, key_type: sqltypes.TypeEngine, value_type: sqltypes.TypeEngine, **kwargs: Any):
        self.key_type = self._check_subtype(key_type)
        self.value_type = self._check_subtype(value_type)
        super().__init__()

    @property
    def python_type(self) -> Optional[Type[Dict[Any, Any]]]:
        return dict

    def __repr__(self):
        return f"MAP({repr(self.key_type)}, {repr(self.value_type)})"

    def get_col_spec(self, **kw) -> str:
        key_type_sql = self.get_sub_type_col_spec(self.key_type, **kw)
        value_type_sql = self.get_sub_type_col_spec(self.value_type, **kw)
        return f"MAP<{key_type_sql}, {value_type_sql}>"

    def get_sub_item_types(self) -> Set[sqltypes.TypeEngine]:
        types = set[sqltypes.TypeEngine]({self.key_type, self.value_type})
        if hasattr(self.key_type, 'get_sub_item_types'):
            types.update(self.key_type.get_sub_item_types())
        if hasattr(self.value_type, 'get_sub_item_types'):
            types.update(self.value_type.get_sub_item_types())
        return types

class STRUCT(StructuredType):
    """
    Usage:
        STRUCT((name, type), (name, type), ..., name=type, ...)

    Examples:
        STRUCT((name, STRING), (age, INTEGER))
        STRUCT(name=STRING, info=STRUCT(age=INTEGER, city=STRING))
        STRUCT(name=STRING, address=MAP(STRING, ARRAY(STRING)))
"""

    __visit_name__ = "STRUCT"

    def __init__(self, *fields: Tuple[STRING, sqltypes.TypeEngine], **kwfields: sqltypes.TypeEngine):
        self.field_tuples = tuple(
            (name, self._check_subtype(type_))
            for (name, type_) in (fields + tuple(kwfields.items()))
        )
        self._field_dict = {
            name.lower(): type_ for (name, type_) in self.field_tuples
        }
        super().__init__()

    @property
    def python_type(self) -> Optional[Type[Any]]:
        return None

    def __repr__(self):
        fields = ", ".join(
            f"{name}={repr(type_)}" for name, type_ in self.field_tuples
        )
        return f"STRUCT({fields})"

    def get_col_spec(self, **kw) -> str:
        fields_sql = []
        for name, type_ in self.field_tuples:
            type_sql = self.get_sub_type_col_spec(type_, **kw)
            fields_sql.append(f"{name} {type_sql}")
        return f"STRUCT<{', '.join(fields_sql)}>"

    def get_sub_item_types(self) -> Set[sqltypes.TypeEngine]:
        types = set[sqltypes.TypeEngine]()
        for _, type_ in self.field_tuples:
            types.add(type_)
            if hasattr(type_, 'get_sub_item_types'):
                types.update(type_.get_sub_item_types())
        return types


class JSON(sqltypes.JSON):
    __visit_name__ = "JSON"
