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

import datetime
import logging
import re
from typing import Optional, List, Any, Type, Dict, Callable

from sqlalchemy.engine import Dialect
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine

logger = logging.getLogger(__name__)


class LARGEINT(sqltypes.Integer):
    __visit_name__ = "LARGEINT"


class DATE(sqltypes.DATE):
    __visit_name__ = "DATE"
    def literal_processor(self, dialect: Dialect) -> Callable[[datetime.date], str]:
        def process(value: datetime.date) -> str:
            return f"TO_DATE('{value}')"

        return process


class StarrocksDate(sqltypes.DATE):
    __visit_name__ = "DATE"

    _reg = re.compile(r"(\d+)-(\d+)-(\d+)")

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                m = self._reg.match(value)
                if not m:
                    raise ValueError("could not parse %r as a date value" % (value,))
                return datetime.date(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process


class StarrocksDateTime(sqltypes.DATETIME):
    __visit_name__ = "DATETIME"

    _reg = re.compile(r"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)\.(\d+)")

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                m = self._reg.match(value)
                if not m:
                    raise ValueError(
                        "could not parse %r as a datetime value" % (value,)
                    )
                return datetime.datetime(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process

    def literal_processor(self, dialect):
        def process(value):
            if value is not None:
                datetime_str = value.isoformat(" ", timespec="microseconds")
                return f"'{datetime_str}'"

        return process


class HLL(sqltypes.Numeric):
    __visit_name__ = "HLL"


class BITMAP(sqltypes.Numeric):
    __visit_name__ = "BITMAP"


class PERCENTILE(sqltypes.Numeric):
    __visit_name__ = "PERCENTILE"


class ARRAY(TypeEngine):
    __visit_name__ = "ARRAY"

    def __init__(self, item_type, **kwargs):
        self.item_type = item_type
        super().__init__(**kwargs)

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