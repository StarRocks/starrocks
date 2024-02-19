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

import logging
from typing import Optional, List, Any, Type, Dict, Callable, Literal
from datetime import date

from sqlalchemy.engine import Dialect
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.dialects.mysql.types import TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, DOUBLE, FLOAT, CHAR, VARCHAR, DATETIME
from sqlalchemy.dialects.mysql.json import JSON

logger = logging.getLogger(__name__)


class LARGEINT(sqltypes.Integer):
    __visit_name__ = "LARGEINT"


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


class ARRAY(TypeEngine):
    __visit_name__ = "ARRAY"

    @property
    def python_type(self) -> Optional[Type[List[Any]]]:
        return list


class MAP(TypeEngine):
    __visit_name__ = "MAP"

    @property
    def python_type(self) -> Optional[Type[Dict[Any, Any]]]:
        return dict


class STRUCT(TypeEngine):
    __visit_name__ = "STRUCT"

    @property
    def python_type(self) -> Optional[Type[Any]]:
        return None
