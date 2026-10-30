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

from datetime import date, datetime, timedelta
from decimal import Decimal
from inspect import isclass
import json
import re
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union

from sqlalchemy import func
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

    def result_processor(self, dialect: Dialect, coltype: object):
        # The server sends LARGEINT as text.
        def process(value):
            if value is None or isinstance(value, int):
                return value
            if isinstance(value, (bytes, bytearray)):
                value = value.decode("ascii")
            return int(value)

        return process


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

    _reg = re.compile(r"(\d+)-(\d+)-(\d+)[ T](\d+):(\d+):(\d+)(?:\.(\d{1,6}))?$")

    def bind_processor(self, dialect: Dialect):
        # DATETIME has no time zone and the driver drops a bound value's UTC
        # offset, so reject an aware value instead of storing it as wall time.
        def process(value):
            if isinstance(value, datetime) and value.utcoffset() is not None:
                raise ValueError(
                    "StarRocks DATETIME has no time zone; convert %r to a naive "
                    "datetime in the intended zone" % (value,)
                )
            return value

        return process

    def result_processor(self, dialect: Dialect, coltype: object):
        def process(value):
            if isinstance(value, (bytes, bytearray)):
                value = value.decode("ascii")
            if isinstance(value, str):
                m = self._reg.match(value.strip())
                if not m:
                    raise ValueError(
                        "could not parse %r as a datetime value" % (value,)
                    )
                *parts, fraction = m.groups()
                # ".5" is 500000 microseconds, not 5.
                return datetime(*(int(x) for x in parts), int((fraction or "").ljust(6, "0")))
            else:
                return value

        return process

class TIME(mysql_types.TIME):
    """StarRocks ``TIME``, a signed duration rather than a time of day.

    StarRocks holds a TIME as a signed number of seconds and renders it as
    ``[-]HH:MM:SS`` with no day part, so the hour field is unbounded and may be
    negative: ``timediff('1000-01-02 01:01:01', '1000-01-01 01:01:01')`` is
    ``24:00:00``, and reversing the arguments gives ``-24:00:00``.

    Values are therefore returned as :class:`datetime.timedelta`, which spans
    that whole range. :class:`sqlalchemy.dialects.mysql.TIME` instead maps the
    driver's ``timedelta`` onto :class:`datetime.time` and drops
    ``timedelta.days`` in the process, turning ``24:00:00`` into ``00:00:00``
    and ``-00:00:01`` into ``23:59:59``. Past 999 hours the value never reaches
    that processor at all -- PyMySQL's parser allows at most three hour digits
    and hands back the undecoded string, which then raises ``AttributeError``.
    """

    __visit_name__ = "TIME"

    _reg = re.compile(r"(-)?(\d+):(\d{1,2}):(\d{1,2})(?:\.(\d{1,6}))?$")

    @property
    def python_type(self) -> Type[timedelta]:
        return timedelta

    def result_processor(self, dialect: Dialect, coltype: object):
        def process(value: Any) -> Optional[timedelta]:
            if value is None or isinstance(value, timedelta):
                return value
            if isinstance(value, (bytes, bytearray)):
                value = value.decode("ascii")
            m = self._reg.match(value)
            if not m:
                raise ValueError("could not parse %r as a time value" % (value,))
            sign, hours, minutes, seconds, fraction = m.groups()
            elapsed = timedelta(
                hours=int(hours),
                minutes=int(minutes),
                seconds=int(seconds),
                microseconds=int((fraction or "").ljust(6, "0")),
            )
            return -elapsed if sign else elapsed

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

    def result_processor(self, dialect: Dialect, coltype: object):
        def process(value):
            if value is None:
                return None
            if isinstance(value, (str, bytes, bytearray)):
                value = parse_complex_value(value)
            elif isinstance(value, dict):
                value = list(value.items())
            return _convert_structured(self, value, dialect)

        return process


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


_PLAIN_FIELD_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


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

    def adapt(self, cls, **kw):
        # The default constructor copy cannot see the fields (they are passed
        # positionally), so a copied or dialect-adapted STRUCT had none.
        if isclass(cls) and issubclass(cls, STRUCT):
            return cls(*self.field_tuples)
        return super().adapt(cls, **kw)

    def get_col_spec(self, **kw) -> str:
        fields_sql = []
        for name, type_ in self.field_tuples:
            type_sql = self.get_sub_type_col_spec(type_, **kw)
            if not _PLAIN_FIELD_NAME.fullmatch(name):
                name = "`" + name.replace("`", "``") + "`"
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


class VARIANT(sqltypes.TypeEngine):
    """Semi-structured type, available from StarRocks 4.1.

    Supported on Iceberg catalog tables (Iceberg format-version 3), where it
    maps to Iceberg's ``variant`` type.

    A bound value is serialised to JSON text and parsed back into a VARIANT
    server-side by ``PARSE_JSON``. That round trip is the reason this does not
    subclass :class:`sqlalchemy.types.JSON`: JSON suppresses ``bind_expression``
    on INSERT, so the text would reach the column as a ``VARCHAR`` bind and
    StarRocks would store the whole document as a VARIANT *string* — indexing
    into it then yields NULL and reading it back returns JSON text rather than
    the original structure.
    """

    __visit_name__ = "VARIANT"

    def bind_expression(self, bindparam):
        return func.parse_json(bindparam)

    def bind_processor(self, dialect: Dialect):
        def process(value: Any) -> Optional[str]:
            return None if value is None else json.dumps(value)

        return process

    def result_processor(self, dialect: Dialect, coltype: object):
        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, (bytes, bytearray)):
                value = value.decode("utf-8")
            return json.loads(value)

        return process


_ESCAPES = {'"': '"', "'": "'", "\\": "\\", "/": "/", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t"}
_NUMBER = re.compile(r"-?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")
_INTEGER = re.compile(r"-?\d+")
_BAREWORDS = {"null": None, "true": True, "false": False}


class _ComplexValueParser:
    """Parser for the server's text rendering of ARRAY/MAP/STRUCT values.

    That rendering is JSON-like but not JSON: map keys of non-string types are
    unquoted (``{1:"a"}``), JSON items inside an array are single-quoted, and
    strings may contain raw control characters.
    """

    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0

    def fail(self, message: str) -> ValueError:
        return ValueError(
            "could not parse complex value at offset %d: %s: %r" % (self.pos, message, self.text)
        )

    def skip_ws(self) -> None:
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self.pos += 1

    def expect_one_of(self, *tokens: str) -> str:
        self.skip_ws()
        for token in tokens:
            if self.text.startswith(token, self.pos):
                self.pos += len(token)
                return token
        raise self.fail("expected one of %s" % (tokens,))

    def parse(self) -> Any:
        value = self.value()
        self.skip_ws()
        if self.pos != len(self.text):
            raise self.fail("trailing characters")
        return value

    def value(self) -> Any:
        self.skip_ws()
        if self.pos >= len(self.text):
            raise self.fail("unexpected end of input")
        char = self.text[self.pos]
        if char == "[":
            self.pos += 1
            items: List[Any] = []
            self.skip_ws()
            if self.text.startswith("]", self.pos):
                self.pos += 1
                return items
            while True:
                items.append(self.value())
                if self.expect_one_of(",", "]") == "]":
                    return items
        if char == "{":
            # Pairs, not a dict: keys are converted by the declared key type first.
            self.pos += 1
            pairs: List[Tuple[Any, Any]] = []
            self.skip_ws()
            if self.text.startswith("}", self.pos):
                self.pos += 1
                return pairs
            while True:
                key = self.value()
                self.expect_one_of(":")
                pairs.append((key, self.value()))
                if self.expect_one_of(",", "}") == "}":
                    return pairs
        if char in "\"'":
            return self.string(char)
        m = _NUMBER.match(self.text, self.pos)
        if m:
            self.pos = m.end()
            token = m.group()
            return int(token) if _INTEGER.fullmatch(token) else Decimal(token)
        for word, result in _BAREWORDS.items():
            if self.text.startswith(word, self.pos):
                self.pos += len(word)
                return result
        raise self.fail("unexpected character")

    def string(self, quote: str) -> str:
        self.pos += 1
        out: List[str] = []
        while self.pos < len(self.text):
            char = self.text[self.pos]
            if char == quote:
                self.pos += 1
                return "".join(out)
            if char == "\\":
                escape = self.text[self.pos + 1:self.pos + 2]
                if escape == "u":
                    out.append(chr(int(self.text[self.pos + 2:self.pos + 6], 16)))
                    self.pos += 6
                    continue
                if escape not in _ESCAPES:
                    raise self.fail("invalid escape \\%s" % escape)
                out.append(_ESCAPES[escape])
                self.pos += 2
                continue
            out.append(char)
            self.pos += 1
        raise self.fail("unterminated string")


def parse_complex_value(text: Union[str, bytes]) -> Any:
    """Parse an ARRAY/MAP/STRUCT value as rendered by the server.

    Maps and structs are returned as lists of ``(key, value)`` pairs and numbers
    as ``int`` or ``Decimal``, so nothing is lost before the declared type
    converts them. Raises ``ValueError`` rather than returning undecoded text.
    """
    if isinstance(text, (bytes, bytearray)):
        text = text.decode("utf-8")
    return _ComplexValueParser(text).parse()


def _element_converter(type_: Any, dialect: Dialect) -> Callable[[Any], Any]:
    type_ = StructuredType._check_subtype(type_)
    if isinstance(type_, StructuredType):
        return lambda value: _convert_structured(type_, value, dialect)
    impl = type_.dialect_impl(dialect)
    if isinstance(impl, sqltypes.Boolean):
        return lambda value: value if isinstance(value, bool) else bool(value)
    if isinstance(impl, sqltypes.Integer):
        return int
    if isinstance(impl, sqltypes.Float):
        return float
    if isinstance(impl, sqltypes.Numeric):
        return (lambda value: Decimal(str(value))) if impl.asdecimal else float
    if isinstance(impl, sqltypes._Binary):
        # Binary values nested in a complex value are rendered as hex.
        return lambda value: value if isinstance(value, bytes) else bytes.fromhex(value)
    if isinstance(impl, sqltypes.String):
        return str
    processor = impl.result_processor(dialect, None)
    return processor if processor is not None else (lambda value: value)


def _nullable(convert: Callable[[Any], Any]) -> Callable[[Any], Any]:
    return lambda value: None if value is None else convert(value)


def _convert_structured(type_: StructuredType, value: Any, dialect: Dialect) -> Any:
    if value is None:
        return None
    if isinstance(type_, ARRAY):
        if not isinstance(value, list):
            raise ValueError("expected an ARRAY value, got %r" % (value,))
        item = _nullable(_element_converter(type_.item_type, dialect))
        return [item(element) for element in value]
    if not isinstance(value, list) or any(not isinstance(pair, tuple) for pair in value):
        raise ValueError("expected a %s value, got %r" % (type_.__visit_name__, value))
    if isinstance(type_, MAP):
        key = _nullable(_element_converter(type_.key_type, dialect))
        val = _nullable(_element_converter(type_.value_type, dialect))
        return {key(k): val(v) for k, v in value}
    fields = {
        name.lower(): _nullable(_element_converter(field_type, dialect))
        for name, field_type in type_.field_tuples
    }
    return {
        k: fields[k.lower()](v) if isinstance(k, str) and k.lower() in fields else v
        for k, v in value
    }
