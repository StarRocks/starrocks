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

from datetime import date
from inspect import isclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

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


class DATE(sqltypes.DATE):
    __visit_name__ = "DATE"

    def literal_processor(self, dialect: Dialect) -> Callable[[date], str]:
        def process(value: date) -> str:
            return f"TO_DATE('{value}')"

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
        if isclass(subtype_obj):
            if issubclass(subtype_obj, StructuredType):
                raise TypeError(f"Structured type '{subtype_obj}' should have subtypes")
            return subtype_obj()
        return subtype_obj


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


class JSON(sqltypes.JSON):
    __visit_name__ = "JSON"
