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
from decimal import Decimal, InvalidOperation
from inspect import isclass
import json
import math
import numbers
import re
from typing import Any, Callable, Dict, Final, Iterable, List, Mapping, NoReturn, Optional, Set, Tuple, Type, Union

from sqlalchemy import exc, func, literal_column, util
import sqlalchemy.dialects.mysql.json as mysql_json
import sqlalchemy.dialects.mysql.types as mysql_types
from sqlalchemy.engine import Dialect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.visitors import InternalTraversal
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
    """StarRocks BIGINT. StarRocks has no unsigned BIGINT, so a cast to MySQL's widens to LARGEINT."""

    __visit_name__ = "BIGINT"

    def result_processor(self, dialect: Dialect, coltype: object) -> Optional[Callable[[Any], Any]]:
        """Read an unsigned BIGINT, cast to the LARGEINT the server sends as text, as ``int``."""
        if self.unsigned:
            return _int_from_text
        return super().result_processor(dialect, coltype)


class LARGEINT(sqltypes.Integer):
    """StarRocks' 128-bit integer, read as ``int``.

    The server sends a LARGEINT value as text, which the driver hands over as a string. StarRocks shows the type
    as ``largeint(40)``; that display width changes nothing, so it is accepted and dropped.
    """

    __visit_name__ = "LARGEINT"

    def __init__(self, display_width: Optional[int] = None) -> None:
        super().__init__()

    def result_processor(self, dialect: Dialect, coltype: object) -> Callable[[Any], Optional[int]]:
        return _int_from_text


def _int_from_text(value: Any) -> Optional[int]:
    """Return an integer the driver left as text, ``str`` or ``bytes``, as an ``int``; ``None`` stays ``None``."""
    if value is None or value.__class__ is int:
        return value
    return int(value)


class DECIMAL(mysql_types.DECIMAL):
    __visit_name__ = "DECIMAL"


class DOUBLE(mysql_types.DOUBLE):
    """DOUBLE returning ``float``; MySQL's default ``asdecimal=True`` would round every value to ten places."""

    __visit_name__ = "DOUBLE"

    def __init__(
        self,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        asdecimal: bool = False,
        **kw: Any,
    ) -> None:
        super().__init__(precision=precision, scale=scale, asdecimal=asdecimal, **kw)


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
    """StarRocks DATETIME, a wall time with no time zone.

    StarRocks has no time-zone-aware datetime type, so an aware ``datetime`` is stored as its wall time and its
    UTC offset is dropped. PyMySQL and asyncmy drop the offset when they bind the value, as with a plain MySQL
    ``DATETIME``, and the literal processor renders it without the offset.
    """

    __visit_name__ = "DATETIME"

    def literal_processor(self, dialect: Dialect) -> Callable[[Union[date, datetime]], str]:
        """Render ``'YYYY-MM-DD HH:MM:SS[.ffffff]'``, or ``'YYYY-MM-DD'`` for a ``date``.

        SQLAlchemy 1.4 has no DATETIME literal renderer of its own, and 2.0's keeps the UTC offset.
        """

        def render_wall_time_literal(value: Union[date, datetime]) -> str:
            return "'%s'" % _wall_time(value).isoformat().replace("T", " ")

        return render_wall_time_literal

    def result_processor(self, dialect: Dialect, coltype: object) -> Callable[[Any], Any]:
        return _datetime_from_text


def _wall_time(value: Any) -> Any:
    """Return an aware ``datetime`` without its time zone, keeping its wall time; any other value as it is."""
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.replace(tzinfo=None)
    return value


def _datetime_from_text(value: Any) -> Any:
    """Read the ``YYYY-MM-DD HH:MM:SS[.ffffff]`` text the server writes inside a collection as a ``datetime``.

    A value that is not a string, such as the ``datetime`` the driver returns for a plain column, is returned as
    it is.
    """
    if value.__class__ is not str:
        return value
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return _datetime_with_short_fraction(value)


def _datetime_with_short_fraction(text: str) -> datetime:
    """Read DATETIME text whose fraction has other than three or six digits, which Python 3.10 cannot.

    ``.5`` is half a second, 500000 microseconds.
    """
    whole, _, fraction = text.partition(".")
    if not (fraction.isascii() and fraction.isdigit() and len(fraction) <= 6):
        raise ValueError(f"could not read {text!r} as a DATETIME value")
    return datetime.fromisoformat(whole).replace(microsecond=int(fraction.ljust(6, "0")))


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
    hashable = False
    """A value reads as a list or a dict, so the ORM uniques rows holding one by identity, as for SQLAlchemy's own
    ARRAY and JSON; hashing it would raise TypeError."""

    @staticmethod
    def _check_subtype(subtype_obj: Union[sqltypes.TypeEngine, type]) -> sqltypes.TypeEngine:
        """
        Check if the subtype is a valid structured type.
        return: an instance of a type, rather than a type itself

        SQLAlchemy's generic ``ARRAY`` is returned as this dialect's ARRAY, with its ``dimensions`` and
        ``as_tuple``, so an element of that type is created, bound and read as a top-level one is, rather than
        cast from text.
        """
        if isclass(subtype_obj):
            if issubclass(subtype_obj, StructuredType):
                raise TypeError(f"'{subtype_obj.__name__}' should be an instance of StructuredType, not a class")
            return subtype_obj()
        if isinstance(subtype_obj, sqltypes.ARRAY):
            return ARRAY(subtype_obj.item_type, dimensions=subtype_obj.dimensions, as_tuple=subtype_obj.as_tuple)
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

    def result_processor(self, dialect: Dialect, coltype: object) -> Callable[[Any], Any]:
        """Build the column's reader: decode the text the server sends, then convert each element by its type.

        SQLAlchemy calls this once per column and statement, so each element's conversion is built here, not for
        every value; see :func:`decode_collection_text` for the decoding. A value that :class:`BinaryElementsAsHex`
        selected arrives inside its one-field STRUCT, and its binary elements are then hex, whatever the server
        version.
        """
        return _collection_column_reader(self, dialect)

    def bind_processor(self, dialect: Dialect) -> Callable[[Any], Optional[str]]:
        """Serialise a value as the one JSON document that :class:`StructuredBindExpression` rebuilds it from.

        The driver has no way to send a list or a dict, so the value travels as text. Each element is written as
        its declared type writes it, as text unless the element is JSON, so no number or date reaches the server
        as a JSON value it would cast inexactly; see :func:`structured_value_encoder`. SQL NULL stays NULL. A number
        is written with all its digits, and a value that is not a number for a numeric element, or a collection
        for a scalar one, raises here.

        The element type's other limits are left to the server, which checks them less than for a plain column:
        StarRocks 3.3 and 4.1 store ``"abcdef"`` in an ``ARRAY<VARCHAR(3)>`` and ``12345.678`` in an
        ``ARRAY<DECIMAL(5, 2)>`` as ``12345.68``, where a plain column of the type refuses both, and store an
        integer out of an element's range as NULL.
        """
        return self._bound_text_writer(dialect)

    def _bound_text_writer(self, dialect: Dialect) -> Callable[[Any], Optional[str]]:
        """Return the function that writes a value as the JSON document of its elements."""
        to_document = structured_value_encoder(self, dialect)

        def serialise(value: Any) -> Optional[str]:
            return None if value is None else json.dumps(to_document(value))

        return serialise

    def literal_processor(self, dialect: Dialect) -> Callable[[Optional[Any]], str]:
        """Render the JSON document ``bind_processor`` builds as an escaped SQL string literal."""
        encode = self.bind_processor(dialect)
        quote = sqltypes.String().literal_processor(dialect)

        def process(value: Optional[Any]) -> str:
            return quote(encode(value))

        return process

    def bind_expression(self, bound_document: ColumnElement) -> "StructuredBindExpression":
        """Have the server rebuild the value from its bound document, element by element."""
        return StructuredBindExpression(bound_document, self)


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
    cache_ok = True

    def __init__(
        self,
        item_type: sqltypes.TypeEngine,
        dimensions: Optional[int] = None,
        as_tuple: bool = False,
        **kwargs: Any,
    ) -> None:
        """Hold ``item_type``, inside ``dimensions - 1`` further ARRAYs when ``dimensions`` is above one.

        ``dimensions`` is how SQLAlchemy's generic ``ARRAY``, which cannot be nested, declares an array of arrays,
        and it is copied here when that type is adapted. It is not kept, so a copy of this type does not nest again.

        ``as_tuple``, as on the generic ``ARRAY``, reads a value as a tuple instead of a list, which an ORM
        mapping can hash; the arrays ``dimensions`` nests read as tuples too.
        """
        item_type = self._check_subtype(item_type)
        for _ in range((dimensions or 1) - 1):
            item_type = ARRAY(item_type, as_tuple=as_tuple)
        self.item_type = item_type
        self.as_tuple = as_tuple
        super().__init__(**kwargs)

    @property
    def python_type(self) -> Optional[Type[List[Any]]]:
        return list

    @property
    def hashable(self) -> bool:  # type: ignore[override]
        """An ARRAY declared ``as_tuple`` reads as a tuple, hashable when its elements are, as SQLAlchemy's is."""
        return self.as_tuple and getattr(self.item_type, "hashable", True)

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

    def _bound_text_writer(self, dialect: Dialect) -> Callable[[Any], Optional[str]]:
        """Write a list as its JSON document, and the text of an array, such as ``'[1,2,3]'``, as before lists.

        Text was the way to bind an array before lists were accepted, and the server cast it to the column's type.
        JSON array text is read here instead, with exact decimals, and written as a list's document is. Other text,
        such as the unquoted words of ``'[a,b]'``, and JSON array text whose elements the declared type refuses,
        is still handed to the server's cast, behind :data:`SERVER_CAST_MARK`, and stored as it was; text that is
        not an array is stored as NULL. StarRocks has no cast from text to an array holding a MAP or a STRUCT, so
        such an array takes no other text than JSON.
        """
        write_document = super()._bound_text_writer(dialect)
        server_casts_text = server_casts_text_to(self, dialect)

        def write(value: Any) -> Optional[str]:
            if not isinstance(value, str):
                return write_document(value)
            items = _json_array_or_none(value)
            if items is not None:
                try:
                    return write_document(items)
                except (TypeError, ValueError):
                    if not server_casts_text:
                        raise
            elif not server_casts_text:
                raise ValueError(
                    "expected an ARRAY of MAP or STRUCT values as a list or as the JSON text of one, got %r" % (value,)
                )
            return SERVER_CAST_MARK + value

        return write

    def column_expression(self, colexpr: ColumnElement) -> ColumnElement:
        """Select an ARRAY of binary elements, nested ARRAYs included, through :class:`BinaryElementsAsHex`."""
        return BinaryElementsAsHex(colexpr) if _holds_binary_through_arrays(self) else colexpr


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
    cache_ok = True

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
    cache_ok = True

    def __init__(self, *fields: Tuple[STRING, sqltypes.TypeEngine], **kwfields: sqltypes.TypeEngine):
        self.field_tuples = tuple(
            (name, self._check_subtype(type_))
            for (name, type_) in (fields + tuple(kwfields.items()))
        )
        self._field_dict = {
            name.lower(): type_ for (name, type_) in self.field_tuples
        }
        super().__init__()

    @util.memoized_property
    def _static_cache_key(self) -> Tuple[Any, ...]:
        """Key on the field names and types; the fields are positional, so the default key would ignore them."""
        return (
            self.__class__,
            tuple((name, type_._static_cache_key) for name, type_ in self.field_tuples),
        )

    @property
    def python_type(self) -> Optional[Type[Any]]:
        return None

    def __repr__(self):
        fields = ", ".join(
            f"{name}={repr(type_)}" for name, type_ in self.field_tuples
        )
        return f"STRUCT({fields})"

    def adapt(self, cls: Type[Any], **kw: Any) -> Any:
        """Copy the STRUCT with its fields; SQLAlchemy's constructor copy passes only keywords, and fields are positional."""
        if not (isclass(cls) and issubclass(cls, STRUCT)):
            return super().adapt(cls, **kw)
        return cls(*self.field_tuples)

    def get_col_spec(self, **kw: Any) -> str:
        """Render ``STRUCT<name type, ...>`` for a dialect without a STRUCT of its own; StarRocks' compiles it itself.

        A field name that is not a plain ASCII identifier is backquoted.
        """
        fields_sql = ", ".join(
            f"{_struct_field_name_sql(name)} {self.get_sub_type_col_spec(type_, **kw)}"
            for name, type_ in self.field_tuples
        )
        return f"STRUCT<{fields_sql}>"

    def get_sub_item_types(self) -> Set[sqltypes.TypeEngine]:
        types = set[sqltypes.TypeEngine]()
        for _, type_ in self.field_tuples:
            types.add(type_)
            if hasattr(type_, 'get_sub_item_types'):
                types.update(type_.get_sub_item_types())
        return types


def check_struct_field_name(name: str) -> None:
    """Refuse a STRUCT field name holding a backquote, which StarRocks would silently drop from the name.

    StarRocks has no escape for a backquote in a field name: it reads the doubled form MySQL uses as nothing, so
    a field ``a`b`` would be created as ``ab``.
    """
    if "`" in name:
        raise exc.CompileError(f"StarRocks cannot name a STRUCT field {name!r}: it drops the backquote from the name")


def _struct_field_name_sql(name: str) -> str:
    check_struct_field_name(name)
    return name if name.isascii() and name.isidentifier() else f"`{name}`"


def _structured_leaf_impl(type_: sqltypes.TypeEngine, dialect: Dialect) -> sqltypes.TypeEngine:
    """Return the type an element's values are stored as: its dialect type, or a TypeDecorator's underlying one."""
    impl = type_.dialect_impl(dialect)
    while isinstance(impl, sqltypes.TypeDecorator):
        impl = impl.impl.dialect_impl(dialect)
    return impl


def _structured_leaf_kind(type_: sqltypes.TypeEngine, dialect: Dialect) -> str:
    """Classify a non-structured element type by how its values travel inside the bound JSON document."""
    impl = _structured_leaf_impl(type_, dialect)
    if isinstance(impl, sqltypes.JSON):
        return "json"
    if isinstance(impl, sqltypes.Boolean):
        return "boolean"
    if isinstance(impl, (sqltypes.Integer, sqltypes.Numeric)):
        return "number"
    if isinstance(impl, sqltypes.DateTime):
        return "datetime"
    if isinstance(impl, sqltypes.Date):
        return "date"
    if isinstance(impl, sqltypes._Binary):
        return "binary"
    if isinstance(impl, sqltypes.String):
        return "string"
    return "other"


def _encode_number(value: Any, integral: bool) -> str:
    """Write a number as the exact text the element's CAST reads.

    The CAST yields NULL for text it cannot read, so an integer element gets a whole float or Decimal as an
    integer and refuses a fraction, and a float subclass such as numpy's is written as a plain float.
    """
    if isinstance(value, int):
        return str(int(value))
    number = _decimal_or_float(value)
    if not (number.is_finite() if isinstance(number, Decimal) else math.isfinite(number)):
        raise ValueError("cannot store %r in a numeric element" % (value,))
    if integral:
        if number != int(number):
            raise ValueError("cannot store %r in an integer element exactly" % (value,))
        return str(int(number))
    return format(number, "f") if isinstance(number, Decimal) else repr(float(number))


def _decimal_or_float(value: Any) -> Union[Decimal, float]:
    """Return a Decimal or float as it is, and read a string or another kind of number from its text.

    The text is what the driver sends for such a value in a plain column, numpy's ``float32`` for one, so the
    element holds what the column would. Text that is not a number, and a value that is not a number or a
    string, is refused rather than cast to NULL.
    """
    if isinstance(value, (Decimal, float)):
        return value
    if not isinstance(value, (str, numbers.Number)):
        raise TypeError("cannot store the %s %r in a numeric element" % (type(value).__name__, value))
    try:
        return Decimal(str(value))
    except InvalidOperation:
        raise ValueError("cannot store %r in a numeric element; it is not a number" % (value,)) from None


def _encode_datetime(value: Any) -> str:
    """Write a date or datetime as the text its element's CAST reads; an aware datetime is its wall time."""
    if isinstance(value, datetime):
        return _wall_time(value).isoformat(sep=" ")
    return value.isoformat() if isinstance(value, date) else str(value)


def _encode_structured_leaf(kind: str, value: Any, integral: bool = False) -> Any:
    if kind == "boolean":
        return "true" if value else "false"
    if kind == "number":
        return _encode_number(value, integral)
    if kind in ("datetime", "date"):
        return _encode_datetime(value)
    if kind == "binary":
        return bytes(value).hex()
    return value if isinstance(value, str) else str(value)


def _struct_values_in_field_order(type_: "STRUCT", value: Any) -> List[Any]:
    """Order a STRUCT value by the declared fields; a missing field is NULL and an unknown one is an error."""
    names = [name for name, _ in type_.field_tuples]
    if isinstance(value, (list, tuple)):
        if len(value) != len(names):
            raise ValueError("expected %d STRUCT fields %s, got %r" % (len(names), names, value))
        return list(value)
    if not isinstance(value, Mapping):
        raise ValueError("expected a STRUCT value as a dict, got %r" % (value,))
    by_lower_name = {name.lower(): name for name in names}
    values: Dict[str, Any] = {}
    for key, item in value.items():
        name = key if key in names else by_lower_name.get(str(key).lower())
        if name is None or name in values:
            raise ValueError("%r is not a field of %r, which has %s" % (key, type_, names))
        values[name] = item
    return [values.get(name) for name in names]


def encode_structured_value(type_: sqltypes.TypeEngine, value: Any, dialect: Dialect) -> Any:
    """Encode a Python value as the JSON-ready form the bind expression of ``type_`` decodes."""
    return structured_value_encoder(type_, dialect)(value)


def structured_value_encoder(type_: sqltypes.TypeEngine, dialect: Dialect) -> Callable[[Any], Any]:
    """Build the function that encodes a Python value of ``type_`` as the JSON-ready form its bind expression decodes.

    An ARRAY is a list of its encoded items, a MAP is ``[keys, values]``, and a STRUCT is the list of its
    fields in declared order, so no name or key is ever looked up in the document. A leaf goes through its
    type's own bind processing, as a plain column's value does, and is then a string (a number as exact text)
    unless it is JSON, which is nested as is. ``None`` stays JSON null. A TypeDecorator over an ARRAY, MAP or
    STRUCT is encoded by :func:`_decorated_collection_encoder`.
    """
    type_ = StructuredType._check_subtype(type_)
    if isinstance(type_, sqltypes.TypeDecorator):
        collection = stored_collection_type(type_, dialect)
        if collection is not None:
            return _decorated_collection_encoder(type_, collection, dialect)
    if isinstance(type_, ARRAY):
        encode_item = structured_value_encoder(type_.item_type, dialect)

        def encode_array(value: Any) -> Any:
            if value is None:
                return None
            if not isinstance(value, (list, tuple)):
                raise ValueError("expected an ARRAY value as a list, got %r" % (value,))
            return [encode_item(item) for item in value]

        return encode_array
    if isinstance(type_, MAP):
        encode_key = structured_value_encoder(type_.key_type, dialect)
        encode_value = structured_value_encoder(type_.value_type, dialect)

        def encode_map(value: Any) -> Any:
            if value is None:
                return None
            if not isinstance(value, Mapping):
                raise ValueError("expected a MAP value as a dict, got %r" % (value,))
            return [[encode_key(key) for key in value], [encode_value(item) for item in value.values()]]

        return encode_map
    if isinstance(type_, STRUCT):
        encode_fields = [structured_value_encoder(field_type, dialect) for _, field_type in type_.field_tuples]

        def encode_struct(value: Any) -> Any:
            if value is None:
                return None
            values = _struct_values_in_field_order(type_, value)
            return [encode(item) for encode, item in zip(encode_fields, values)]

        return encode_struct
    return _structured_leaf_encoder(type_, dialect)


def _decorated_collection_encoder(
    type_: sqltypes.TypeDecorator, collection: "StructuredType", dialect: Dialect
) -> Callable[[Any], Any]:
    """Build the encoder of a TypeDecorator over an ARRAY, MAP or STRUCT: its ``process_bind_param``, then the
    collection's own encoding.

    The decorator's whole bind processing would also serialise the value as the collection's bound document,
    which the element of another collection is not, so only the ``process_bind_param`` of each TypeDecorator
    down to the collection is applied, the outermost first.
    """
    process_bind_params = _decorator_bind_params(type_, dialect)
    encode_collection = structured_value_encoder(collection, dialect)

    def encode_decorated(value: Any) -> Any:
        for process_bind_param in process_bind_params:
            value = process_bind_param(value, dialect)
        return encode_collection(value)

    return encode_decorated


def _decorator_bind_params(type_: sqltypes.TypeEngine, dialect: Dialect) -> List[Callable[[Any, Dialect], Any]]:
    """Return the ``process_bind_param`` of ``type_`` and of each TypeDecorator under it that overrides it."""
    process_bind_params = []
    decorator = type_.dialect_impl(dialect)
    while isinstance(decorator, sqltypes.TypeDecorator):
        if util.method_is_overridden(decorator, sqltypes.TypeDecorator.process_bind_param):
            process_bind_params.append(decorator.process_bind_param)
        decorator = decorator.impl.dialect_impl(dialect)
    return process_bind_params


SERVER_CAST_MARK: Final[str] = "~"
"""Marks bound ARRAY text the server casts itself; a bound JSON document starts with ``[`` instead."""


def _json_array_or_none(text: str) -> Optional[List[Any]]:
    """Read text as a JSON array, keeping each decimal's digits, or return None when it is not one."""
    try:
        value = json.loads(text, parse_float=Decimal)
    except ValueError:
        return None
    return value if isinstance(value, list) else None


def server_casts_text_to(type_: "ARRAY", dialect: Dialect) -> bool:
    """Whether StarRocks casts text to this ARRAY type: it has no cast to an array holding a MAP or a STRUCT.

    An element that is a TypeDecorator over an ARRAY, MAP or STRUCT counts as that collection.
    """
    item_type = stored_collection_type(type_.item_type, dialect)
    if isinstance(item_type, ARRAY):
        return server_casts_text_to(item_type, dialect)
    return item_type is None


def stored_collection_type(type_: sqltypes.TypeEngine, dialect: Dialect) -> Optional["StructuredType"]:
    """Return the ARRAY, MAP or STRUCT that values of ``type_`` are stored as, through any TypeDecorator, or None."""
    impl = StructuredType._check_subtype(_structured_leaf_impl(type_, dialect))
    return impl if isinstance(impl, StructuredType) else None


_JSON_TO_MAP_SINCE: Final[Tuple[int, ...]] = (3, 4, 3)
"""The first StarRocks that casts JSON to MAP, from StarRocks PR 58045; 3.5.0 and 4.0.0 have it too."""

_JSON_TO_STRUCT_SINCE: Final[Tuple[int, ...]] = (3, 5, 0)
"""The first StarRocks that casts JSON to STRUCT, alone or as an ARRAY's elements, from StarRocks PR 52790."""

_JSON_CASTABLE_ELEMENT_TYPES: Final[Tuple[Type[sqltypes.TypeEngine], ...]] = (
    sqltypes.Boolean, sqltypes.Integer, sqltypes.Float, sqltypes.String, sqltypes.JSON,
)
"""The element types StarRocks casts JSON to; it has no cast of JSON to DATE, DATETIME, DECIMAL or binary."""

_JSON_PATH_FIELD_NAME: Final = re.compile(r'[^."\\\[\]]+')
"""The STRUCT field names that the JSON path ``$.<name>`` reads as one key with that name, ``$`` apart."""


def server_casts_json_to(type_: sqltypes.TypeEngine, dialect: Dialect) -> bool:
    r"""Whether the connected StarRocks casts JSON to ``type_``, an ARRAY, MAP or STRUCT.

    StarRocks casts JSON to an ARRAY of :data:`_JSON_CASTABLE_ELEMENT_TYPES` on every supported version, to a
    MAP from 3.4.3, and to a STRUCT, alone or as an ARRAY's elements, from 3.5.0, when each value is such an
    element or a collection it casts JSON to. It has no cast of JSON to an ARRAY of ARRAYs or of MAPs. A MAP's
    key is not checked: StarRocks takes only a scalar type for one, and refuses any other in the CAST itself.

    A STRUCT field is read by the JSON path ``$.<name>``, so a field whose name holds ``.``, ``"``, ``\``, ``[``
    or ``]``, or is ``$``, is read from another key, or stops the StarRocks backend before 4.0.13 and 4.1.3;
    such a STRUCT is taken as one StarRocks can't cast to. With no server version known, as when compiling
    offline, only the casts every supported version has are taken as there.
    """
    return _casts_json_to(type_, dialect, tuple(dialect.server_version_info or ()))


def _casts_json_to(type_: sqltypes.TypeEngine, dialect: Dialect, version: Tuple[int, ...]) -> bool:
    collection = stored_collection_type(type_, dialect)
    if collection is None:
        return isinstance(_structured_leaf_impl(type_, dialect), _JSON_CASTABLE_ELEMENT_TYPES)
    if isinstance(collection, ARRAY):
        item_type = stored_collection_type(collection.item_type, dialect)
        if item_type is not None and not isinstance(item_type, STRUCT):
            return False
        return _casts_json_to(collection.item_type, dialect, version)
    if isinstance(collection, MAP):
        return version >= _JSON_TO_MAP_SINCE and _casts_json_to(collection.value_type, dialect, version)
    return version >= _JSON_TO_STRUCT_SINCE and all(
        name != "$"
        and _JSON_PATH_FIELD_NAME.fullmatch(name) is not None
        and _casts_json_to(field_type, dialect, version)
        for name, field_type in collection.field_tuples
    )


def _structured_leaf_encoder(type_: sqltypes.TypeEngine, dialect: Dialect) -> Callable[[Any], Any]:
    """Build the encoder of a non-structured element, which applies its type's bind processing first.

    That processing is what a plain column's value gets, ``None`` included, so a TypeDecorator's
    ``process_bind_param`` sees every element. A JSON element is then nested as the document that processing
    serialises, which applies the engine's ``json_serializer`` too.
    """
    kind = _structured_leaf_kind(type_, dialect)
    process = type_.dialect_impl(dialect).bind_processor(dialect) or _unprocessed
    if kind == "json":
        return lambda value: _nested_json_document(process(value))
    integral = kind == "number" and isinstance(_structured_leaf_impl(type_, dialect), sqltypes.Integer)

    def encode_leaf(value: Any) -> Any:
        value = process(value)
        _refuse_collection_in_scalar_element(type_, value)
        return None if value is None else _encode_structured_leaf(kind, value, integral)

    return encode_leaf


def _unprocessed(value: Any) -> Any:
    return value


def _nested_json_document(serialized: Optional[Union[str, bytes]]) -> Any:
    """Parse a JSON element's serialised document back, to nest it in the collection's document."""
    return None if serialized is None else json.loads(serialized)


def _refuse_collection_in_scalar_element(type_: sqltypes.TypeEngine, value: Any) -> None:
    """Refuse a list, tuple or mapping bound to a scalar element, which would be stored as its repr or as NULL."""
    if isinstance(value, (list, tuple, Mapping)):
        raise TypeError(
            "cannot store the %s %r in a %r element; declare a nested ARRAY, MAP or STRUCT for it"
            % (type(value).__name__, value, type_)
        )


class StructuredBindExpression(ColumnElement):
    """SQL that builds an ARRAY, MAP or STRUCT value from the one JSON document a bind parameter carries.

    The server's own text-to-ARRAY cast is not a JSON parser and its JSON-to-DECIMAL/DATE/MAP/STRUCT casts
    are missing or version-dependent, so each element is taken out of the parsed document as JSON and cast
    on its own: ``CAST(CAST(element AS VARCHAR) AS type)``, nested through ``array_map``, ``map_from_arrays``
    and ``named_struct``. Only ``CAST(.. AS ARRAY<JSON>)``, ``parse_json`` and these functions are needed,
    which every supported StarRocks version provides. The expression is rendered at compile time because it
    depends on the declared element types.
    """

    inherit_cache = True
    _traverse_internals = [
        ("bindvalue", InternalTraversal.dp_clauseelement),
        ("type", InternalTraversal.dp_type),
    ]

    def __init__(self, bindvalue: ColumnElement, type_: StructuredType) -> None:
        self.bindvalue = bindvalue
        self.type = type_


class _StructuredExpressionRenderer:
    """Render the SQL that ``StructuredBindExpression`` compiles to; lambda variables are numbered to stay unique."""

    def __init__(self, compiler: Any) -> None:
        self.dialect = compiler.dialect
        self.type_compiler = getattr(self.dialect, "type_compiler_instance", None) or self.dialect.type_compiler
        self.quote_string = sqltypes.String().literal_processor(self.dialect)
        self.variable_count = 0

    def render(self, type_: StructuredType, render_bound: Callable[[], str]) -> str:
        """Render the value of ``type_`` from the bound text: a JSON document, or array text for the server's cast.

        ``render_bound`` renders the bind parameter once more each time it is called, in the order of the SQL.
        """
        if isinstance(type_, ARRAY) and server_casts_text_to(type_, self.dialect):
            return self.rebuild_or_cast_text(type_, render_bound)
        return self.convert(type_, f"parse_json({render_bound()})")

    def rebuild_or_cast_text(self, type_: "ARRAY", render_bound: Callable[[], str]) -> str:
        """Rebuild the array from its JSON document, or cast text bound behind :data:`SERVER_CAST_MARK` as is.

        The bound text appears three times. Taking it once under a lambda variable instead ran two to three
        times slower on the server than this, and its lambdas over a captured NULL fail on 3.3.
        """
        mark_length = len(SERVER_CAST_MARK)
        is_marked = f"left({render_bound()}, {mark_length}) = {self.quote_string(SERVER_CAST_MARK)}"
        server_cast = f"CAST(substr({render_bound()}, {mark_length + 1}) AS {self.type_compiler.process(type_)})"
        rebuilt = self.convert(type_, f"parse_json({render_bound()})")
        return f"IF({is_marked}, {server_cast}, {rebuilt})"

    def new_variable(self) -> str:
        self.variable_count += 1
        return f"e{self.variable_count}"

    def convert(self, type_: sqltypes.TypeEngine, json_sql: str) -> str:
        """Render the value of ``type_`` from ``json_sql``; a TypeDecorator over a collection is that collection."""
        type_ = stored_collection_type(type_, self.dialect) or type_
        if isinstance(type_, ARRAY):
            return self.convert_array(type_.item_type, f"CAST({json_sql} AS ARRAY<JSON>)")
        if isinstance(type_, MAP):
            return self.build_unless_null(json_sql, lambda name: self.convert_map(type_, name))
        if isinstance(type_, STRUCT):
            return self.build_unless_null(json_sql, lambda name: self.convert_struct(type_, name))
        return self.convert_leaf(type_, json_sql)

    def build_unless_null(self, json_sql: str, build: Callable[[str], str]) -> str:
        """Build a MAP or STRUCT from ``json_sql`` under a lambda variable, or NULL when it is NULL.

        ``named_struct`` of NULL fields is a struct of NULLs, not NULL; ``IF(.., NULL, named_struct(..))`` is
        refused before 3.5; and a NULL ``map_from_arrays`` result crashes the backend of 3.3 and 3.4 when it
        is a constant in ``INSERT .. VALUES``. So the single-element array holding the document is filtered
        down to nothing for a NULL document, and ``element_at`` of the empty array is a typed NULL.
        """
        name = self.new_variable()
        present = self.new_variable()
        present_only = f"array_filter({present} -> {present} IS NOT NULL, [{json_sql}])"
        return f"element_at(array_map({name} -> {build(name)}, {present_only}), 1)"

    def convert_array(self, item_type: sqltypes.TypeEngine, items_sql: str) -> str:
        item_type = StructuredType._check_subtype(item_type)
        if _is_json_leaf(item_type, self.dialect):
            return items_sql
        name = self.new_variable()
        return f"array_map({name} -> {self.convert(item_type, name)}, {items_sql})"

    def convert_map(self, type_: MAP, name: str) -> str:
        pair = f"CAST({name} AS ARRAY<JSON>)"
        keys = self.convert_array(type_.key_type, f"CAST({pair}[1] AS ARRAY<JSON>)")
        values = self.convert_array(type_.value_type, f"CAST({pair}[2] AS ARRAY<JSON>)")
        return f"map_from_arrays({keys}, {values})"

    def convert_struct(self, type_: STRUCT, name: str) -> str:
        fields = ", ".join(
            f"{self.quote_string(field_name)}, {self.convert(field_type, f'CAST({name} AS ARRAY<JSON>)[{position}]')}"
            for position, (field_name, field_type) in enumerate(type_.field_tuples, start=1)
        )
        return f"named_struct({fields})"

    def convert_leaf(self, type_: sqltypes.TypeEngine, json_sql: str) -> str:
        kind = _structured_leaf_kind(type_, self.dialect)
        if kind == "json":
            return json_sql
        text = f"CAST({json_sql} AS VARCHAR)"
        if kind == "string":
            return text
        if kind == "binary":
            return f"to_binary({text}, 'hex')"
        return f"CAST({text} AS {self.type_compiler.process(type_)})"


def _is_json_leaf(type_: sqltypes.TypeEngine, dialect: Dialect) -> bool:
    return not isinstance(type_, StructuredType) and _structured_leaf_kind(type_, dialect) == "json"


@compiles(StructuredBindExpression)
def compile_structured_bind_expression(element: StructuredBindExpression, compiler: Any, **kw: Any) -> str:
    return _StructuredExpressionRenderer(compiler).render(element.type, lambda: compiler.process(element.bindvalue, **kw))


def _check_json_path_key(element: Any) -> None:
    """Refuse a key StarRocks' JSON path cannot name, which would read the wrong value or fail on the server.

    An empty key and ``$`` read the whole document, a key holding ``[`` is read as an array index, a key holding
    ``]`` is an invalid path to StarRocks, and a key holding a double quote cannot be escaped.
    """
    if isinstance(element, int):
        return
    key = str(element)
    if key in ("", "$") or any(character in key for character in '"[]'):
        raise ValueError(
            f"StarRocks JSON paths cannot name the key {element!r}; an empty key and $ read the whole document, "
            "and a key holding [, ] or a double quote reads another value or is refused"
        )


def _json_path_step(element: Any) -> str:
    r"""Write one step of a StarRocks JSON path: ``[0]`` for an array index, ``."key"`` for an object key.

    StarRocks reads a backslash in a quoted key as the start of an escape, so ``\n`` in a key would name a
    newline, and ``\\`` names one backslash; each backslash in the key is therefore doubled.
    """
    if isinstance(element, int):
        return "[%s]" % element
    _check_json_path_key(element)
    return '."%s"' % str(element).replace("\\", "\\\\")


class JSONIndexType(mysql_json.JSONIndexType):
    """The path of one JSON index, ``$."key"`` or ``$[0]``, for a key StarRocks' path syntax can name."""

    def _format_value(self, value: Any) -> str:
        return "$" + _json_path_step(value)


class JSONPathType(mysql_json.JSONPathType):
    """The path of a JSON path index, ``$."a"[0]``, for keys StarRocks' path syntax can name."""

    def _format_value(self, value: Any) -> str:
        return "$" + "".join(_json_path_step(element) for element in value)


class JSON(sqltypes.JSON):
    """JSON column type that round-trips scalars as JSON scalars, not JSON strings.

    The server's ``parse_json('1')`` and ``CAST('1' AS JSON)`` yield the JSON *string* ``"1"``, so a bare scalar
    bound as text would read back as ``'1'``. The bound text is therefore wrapped in a one-element array and
    the element is taken out again, ``json_query(parse_json(concat('[', :value, ']')), '$[0]')``, which keeps
    the JSON type of the value. A SQL NULL bind stays SQL NULL, and a JSON null (``'null'``) is written as a JSON
    null. From StarRocks 4.0, Flat JSON reads a JSON null stored with JSON objects back as ``{}``, unless the
    table's ``flat_json.enable`` property is false (StarRocks issue 81490).
    """

    __visit_name__ = "JSON"

    def bind_expression(self, bindvalue: Any) -> Any:
        wrapped = func.concat(literal_column("'['"), bindvalue, literal_column("']'"))
        return func.json_query(func.parse_json(wrapped), literal_column("'$[0]'"))


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


def _refuse_json_constant(name: str) -> NoReturn:
    """Refuse ``NaN`` and ``Infinity``, which StarRocks never writes, so JSON reads nothing the scanner refuses."""
    raise ValueError(f"{name} is not part of StarRocks' text of a collection")


_COLLECTION_JSON: Final = json.JSONDecoder(
    parse_float=Decimal, parse_constant=_refuse_json_constant, object_pairs_hook=tuple, strict=False
)
"""Decodes a collection's text when it is JSON, to the values :func:`scan_collection_text` reads from it."""


def decode_collection_text(text: Union[str, bytes, bytearray]) -> Any:
    """Decode the text StarRocks sends for an ARRAY, MAP or STRUCT value.

    An ARRAY is a list, and a MAP or STRUCT a tuple of ``(key, value)`` pairs, from which the declared type builds
    its dict. A number is an ``int``, or a ``Decimal`` when it has a fraction or an exponent, so no digit is lost
    before the element's type reads it; ``inf``, ``-inf`` and ``nan`` are floats.

    Most values are JSON, which ``json`` decodes in C. The rest are read by :func:`scan_collection_text`: a MAP
    whose keys are not strings, as in ``{1:"a"}``, the JSON elements of an ARRAY, MAP or STRUCT, which the server
    single-quotes, and a non-finite DOUBLE. The scanner reads JSON to the same values, so a value decodes the
    same whichever of the two reads it.
    """
    if not isinstance(text, str):
        text = text.decode("utf-8")
    try:
        return _COLLECTION_JSON.decode(text)
    except ValueError:
        pass
    return scan_collection_text(text)


_COLLECTION_TOKENS: Final = re.compile(
    r"""\s*(
          [\[\]{}:,]
        | "[^"\\]*(?:\\.[^"\\]*)*"
        | -?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?
        | '[^'\\]*(?:\\.[^'\\]*)*'
        | null | true | false | -inf | inf | nan
        | \S
    )""",
    re.VERBOSE | re.DOTALL,
)
"""The tokens of a collection's text: brackets, separators, strings, numbers and words.

Any other character is a token of its own, which only a one-digit number can be, so the scanner refuses it.
The most frequent tokens are tried first.
"""

_BARE_WORDS: Final = {"null": None, "true": True, "false": False, "inf": math.inf, "-inf": -math.inf, "nan": math.nan}

_STRING_ESCAPE: Final = re.compile(r"\\(?:u([0-9a-fA-F]{4})|(.))", re.DOTALL)

_ESCAPED_CHARACTERS: Final = {
    "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "/": "/", "\\": "\\", '"': '"', "'": "'",
}

_SURROGATE: Final = re.compile("[\ud800-\udfff]")

_VALUE, _VALUE_OR_CLOSE, _KEY, _KEY_OR_CLOSE, _COLON, _SEPARATOR, _END = range(7)
"""What :func:`scan_collection_text` expects next."""


def scan_collection_text(text: str) -> Any:
    r"""Read a collection's text, JSON or not, to the values :func:`decode_collection_text` describes.

    Besides JSON, it reads keys that are not quoted, single-quoted strings with ``\'`` escapes, raw control
    characters inside strings, and the words ``inf``, ``-inf`` and ``nan``. Anything else raises ``ValueError``.

    The text is split into tokens by one regular expression, and each value is added to the ARRAY or MAP being
    read; the ones it is inside wait on a stack, each with the key it is the value of.
    """
    enclosing: List[Tuple[Optional[List[Any]], bool, Any]] = []
    items: Optional[List[Any]] = None
    is_object = False
    key: Any = None
    value: Any = None
    expect = _VALUE
    for token in _COLLECTION_TOKENS.findall(text):
        first = token[0]
        if expect == _SEPARATOR:
            if first == ",":
                expect = _KEY if is_object else _VALUE
                continue
            if first != ("}" if is_object else "]"):
                raise _unreadable(text, token)
            value = tuple(items) if is_object else items
            items, is_object, key = enclosing.pop()
        elif expect == _COLON:
            if first != ":":
                raise _unreadable(text, token)
            expect = _VALUE
            continue
        elif first == "[" or first == "{":
            if expect != _VALUE and expect != _VALUE_OR_CLOSE:
                raise _unreadable(text, token)
            enclosing.append((items, is_object, key))
            items, is_object = [], first == "{"
            expect = _KEY_OR_CLOSE if is_object else _VALUE_OR_CLOSE
            continue
        elif first == "]" or first == "}":
            if expect != (_KEY_OR_CLOSE if first == "}" else _VALUE_OR_CLOSE):
                raise _unreadable(text, token)
            value = tuple(items) if is_object else items
            items, is_object, key = enclosing.pop()
        else:
            if expect == _END:
                raise _unreadable(text, token)
            if first == '"' or first == "'":
                if len(token) == 1:
                    raise _unreadable(text, token)
                value = token[1:-1] if "\\" not in token else _unquoted(token)
            else:
                value = _scanned_number_or_word(text, token)
            if expect == _KEY or expect == _KEY_OR_CLOSE:
                key = value
                expect = _COLON
                continue
        if items is None:
            expect = _END
        else:
            items.append((key, value) if is_object else value)
            expect = _SEPARATOR
    if expect != _END:
        raise _unreadable(text, "the end of the text")
    return value


def _scanned_number_or_word(text: str, token: str) -> Any:
    """Return the value of a number or word token, refusing a character that starts no token."""
    if len(token) == 1 and not "0" <= token <= "9":
        raise _unreadable(text, token)
    if token in _BARE_WORDS:
        return _BARE_WORDS[token]
    if "." in token or "e" in token or "E" in token:
        return Decimal(token)
    return int(token)


def _unquoted(quoted: str) -> str:
    r"""Return the text of a quoted string, its escapes read as JSON reads them, with ``\'`` as well.

    An escaped UTF-16 surrogate pair is one character, as in JSON.
    """
    body = quoted[1:-1]
    if "\\" not in body:
        return body
    text = _STRING_ESCAPE.sub(_escaped_character, body)
    if _SURROGATE.search(text):
        text = text.encode("utf-16-le", "surrogatepass").decode("utf-16-le", "surrogatepass")
    return text


def _escaped_character(escape: "re.Match[str]") -> str:
    code, character = escape.groups()
    if code is not None:
        return chr(int(code, 16))
    try:
        return _ESCAPED_CHARACTERS[character]
    except KeyError:
        raise ValueError(f"could not read the escape \\{character} in a StarRocks collection value") from None


def _unreadable(text: str, where: str) -> ValueError:
    return ValueError(f"could not read the StarRocks collection value {text!r}: unexpected {where!r}")


def _nests_binary_as_hex(version: Optional[Tuple[int, ...]]) -> bool:
    """Whether the server writes a VARBINARY inside a collection as hex: from 4.0.10 and 4.1.1, and when unknown.

    StarRocks PR 71346 brought hex to those releases; 3.5 and earlier write the bytes themselves.
    """
    if not version:
        return True
    version = tuple(version)
    return (4, 0, 10) <= version < (4, 1) or version >= (4, 1, 1)


def _nested_binary_reader(dialect: Dialect) -> Callable[[Any], bytes]:
    """Read a VARBINARY element of a collection as the bytes it holds.

    A server that writes the bytes themselves sends them inside the collection's text, which the driver has
    decoded as UTF-8, so they are encoded back; bytes that are not UTF-8 make the driver fail before this,
    unless :class:`BinaryElementsAsHex` selected them as hex.
    """
    if _nests_binary_as_hex(getattr(dialect, "server_version_info", None)):
        return _read_hex_binary
    return _read_raw_binary


def _read_hex_binary(value: Union[str, bytes]) -> bytes:
    return value if isinstance(value, bytes) else bytes.fromhex(value)


def _read_raw_binary(value: Union[str, bytes]) -> bytes:
    return value if isinstance(value, bytes) else value.encode("utf-8")


_HEX_BINARY_FIELD: Final[str] = "hex"


def _holds_binary_through_arrays(type_: "ARRAY") -> bool:
    """Whether the elements of ``type_``, looking through nested ARRAYs, are binary."""
    item_type = type_.item_type
    while isinstance(item_type, ARRAY):
        item_type = item_type.item_type
    return not isinstance(item_type, StructuredType) and issubclass(item_type._type_affinity, sqltypes._Binary)


class BinaryElementsAsHex(ColumnElement):
    """A selected ARRAY of binary elements, hex-encoded by a server that would send their bytes as they are.

    StarRocks 3.5 and earlier write a VARBINARY inside a collection as its bytes, and the driver fails on
    bytes that are not UTF-8 and leaves the connection broken. On those servers the elements are hex-encoded,
    ``named_struct('hex', array_map(h1 -> hex(h1), col))``, in a one-field STRUCT whose shape tells the
    ARRAY's reader that they are hex; a ``text()`` query of the same column is still read as raw bytes. Other
    servers write hex already, and the column is selected as it is.
    """

    inherit_cache = True
    _traverse_internals = [("element", InternalTraversal.dp_clauseelement)]

    def __init__(self, element: ColumnElement) -> None:
        self.element = element
        self.type = element.type


@compiles(BinaryElementsAsHex)
def compile_binary_elements_as_hex(element: BinaryElementsAsHex, compiler: Any, **kw: Any) -> str:
    """Select the binary elements as hex, unless the server writes hex itself or the column is a left-out CAST.

    A CAST the compiler leaves out selects its operand, text or JSON, which holds no binary values to encode.
    """
    column_sql = compiler.process(element.element, **kw)
    if _nests_binary_as_hex(getattr(compiler.dialect, "server_version_info", None)):
        return column_sql
    left_out_cast_behind = getattr(compiler, "_left_out_cast_behind", None)
    if left_out_cast_behind is not None and left_out_cast_behind(element.element) is not None:
        return column_sql
    array_type = _structured_leaf_impl(element.type, compiler.dialect)
    return f"named_struct('{_HEX_BINARY_FIELD}', {_hex_encoded_elements_sql(array_type, column_sql)})"


def _hex_encoded_elements_sql(type_: "ARRAY", array_sql: str, depth: int = 1) -> str:
    variable = f"h{depth}"
    if isinstance(type_.item_type, ARRAY):
        item_sql = _hex_encoded_elements_sql(type_.item_type, variable, depth + 1)
    else:
        item_sql = f"hex({variable})"
    return f"array_map({variable} -> {item_sql}, {array_sql})"


def _collection_column_reader(type_: StructuredType, dialect: Dialect) -> Callable[[Any], Any]:
    """Build the reader of an ARRAY, MAP or STRUCT column, with every element's conversion built once.

    An ARRAY of binary elements also gets a reader for the hex that :class:`BinaryElementsAsHex` selects.
    """
    convert = _collection_converter(type_, dialect, _nested_binary_reader(dialect))
    decode = decode_collection_text if _server_text_is_json(type_, dialect) else scan_collection_text
    if not (isinstance(type_, ARRAY) and _holds_binary_through_arrays(type_)):

        def read_collection(value: Any) -> Any:
            return None if value is None else convert(_decoded_collection(value, decode))

        return read_collection
    convert_hex = _collection_converter(type_, dialect, _read_hex_binary)

    def read_binary_collection(value: Any) -> Any:
        if value is None:
            return None
        decoded = _decoded_collection(value, decode)
        if _is_hex_selection(decoded):
            return convert_hex(decoded[0][1])
        return convert(decoded)

    return read_binary_collection


def _decoded_collection(value: Any, decode: Callable[[str], Any]) -> Any:
    """Decode the server's text of a value; a list or dict the caller decoded already is used as it is."""
    if isinstance(value, str):
        return decode(value)
    if isinstance(value, (bytes, bytearray)):
        return decode(value.decode("utf-8"))
    return value


def _server_text_is_json(type_: sqltypes.TypeEngine, dialect: Dialect) -> bool:
    """Whether the server writes a value of ``type_`` as JSON, which decides only which decoder is tried first.

    The server writes a numeric or BOOLEAN MAP key unquoted, and a JSON element as a single-quoted string, so a
    collection holding either is not JSON, and is scanned without trying ``json`` and failing first. Both
    decoders read JSON to the same values, so this changes how fast a value is read, not what it reads as.
    """
    type_ = StructuredType._check_subtype(type_)
    if isinstance(type_, ARRAY):
        return _server_text_is_json(type_.item_type, dialect)
    if isinstance(type_, MAP):
        unquoted_key = _structured_leaf_kind(type_.key_type, dialect) in ("number", "boolean")
        return not unquoted_key and _server_text_is_json(type_.value_type, dialect)
    if isinstance(type_, STRUCT):
        return all(_server_text_is_json(field_type, dialect) for _, field_type in type_.field_tuples)
    return _structured_leaf_kind(type_, dialect) != "json"


def _is_hex_selection(decoded: Any) -> bool:
    """Whether a decoded ARRAY value is the one-field STRUCT that :class:`BinaryElementsAsHex` selects it in.

    An ARRAY decodes to a list, and a STRUCT to a tuple of pairs, so the shape alone tells them apart.
    """
    return decoded.__class__ is tuple and len(decoded) == 1 and decoded[0][0] == _HEX_BINARY_FIELD


class LeftOutCastResult(sqltypes.TypeEngine):
    """The type of a result column whose CAST to ARRAY, MAP or STRUCT the compiler left out.

    The server sends the operand's own text, JSON or not, rather than what it writes for the CAST's type, so
    the value is read by :func:`operand_text_reader` rather than by that type's own reader.
    """

    cache_ok = True

    def __init__(self, cast_type: sqltypes.TypeEngine) -> None:
        self.cast_type = cast_type

    def result_processor(self, dialect: Dialect, coltype: object) -> Callable[[Any], Any]:
        return operand_text_reader(self.cast_type, dialect)


def operand_text_reader(cast_type: sqltypes.TypeEngine, dialect: Dialect) -> Callable[[Any], Any]:
    """Build the reader of a value whose CAST to ``cast_type`` was left out, from the text of its operand.

    Each element is converted to its declared type as StarRocks' cast of JSON converts it, see
    :func:`_operand_text_converter`; a STRUCT keeps its declared fields only, each found by its exact name. A
    TypeDecorator's ``process_result_value`` is then applied, as on any column of its type.

    Text that is neither JSON nor StarRocks' own output, such as ``[a,b]`` or ``[NULL,1]``, which StarRocks'
    cast of text to ARRAY reads, is returned as the text itself for an ARRAY, as 1.3 returned it, rather than
    failing a fetch that 1.3 completed. Such text for a MAP or STRUCT, which StarRocks can't cast text to,
    raises.
    """
    collection = stored_collection_type(cast_type, dialect)
    convert = _collection_converter(collection, dialect, _read_raw_binary, from_operand_text=True)
    process_result_values = _decorator_result_values(cast_type, dialect)
    keeps_array_text = isinstance(collection, ARRAY)

    def converted_or_unread_text(value: Any) -> Any:
        try:
            decoded = _decoded_collection(value, decode_collection_text)
        except ValueError:
            text = value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else value
            if not (keeps_array_text and _is_bracketed_text(text)):
                raise
            return text
        return convert(decoded)

    def read_operand_text(value: Any) -> Any:
        result = None if value is None else converted_or_unread_text(value)
        for process_result_value in process_result_values:
            result = process_result_value(result, dialect)
        return result

    return read_operand_text


def _is_bracketed_text(value: Any) -> bool:
    """Whether ``value`` is text in brackets, as StarRocks' cast of text to ARRAY reads an array."""
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return stripped.startswith("[") and stripped.endswith("]")


def _decorator_result_values(type_: sqltypes.TypeEngine, dialect: Dialect) -> List[Callable[[Any, Dialect], Any]]:
    """Return the ``process_result_value`` of each TypeDecorator from ``type_`` down to what it stores, the
    innermost first, as SQLAlchemy applies them to a column's value."""
    process_result_values = []
    decorator = type_.dialect_impl(dialect)
    while isinstance(decorator, sqltypes.TypeDecorator):
        if util.method_is_overridden(decorator, sqltypes.TypeDecorator.process_result_value):
            process_result_values.append(decorator.process_result_value)
        decorator = decorator.impl.dialect_impl(dialect)
    return process_result_values[::-1]


def _collection_converter(
    type_: StructuredType, dialect: Dialect, read_binary: Callable[[Any], bytes], from_operand_text: bool = False
) -> Callable[[Any], Any]:
    """Build the function that turns a decoded value of ``type_``, or ``None``, into Python by its element types.

    ``from_operand_text`` builds the converter of :func:`operand_text_reader`.
    """
    if isinstance(type_, ARRAY):
        return _array_converter(
            _element_converter(type_.item_type, dialect, read_binary, from_operand_text), type_.as_tuple
        )
    if isinstance(type_, MAP):
        return _map_converter(
            _element_converter(type_.key_type, dialect, read_binary, from_operand_text),
            _element_converter(type_.value_type, dialect, read_binary, from_operand_text),
        )
    if from_operand_text:
        return _declared_fields_converter(type_, dialect, read_binary)
    return _struct_converter(type_, dialect, read_binary)


def _array_converter(convert_item: Optional[Callable[[Any], Any]], as_tuple: bool = False) -> Callable[[Any], Any]:
    """Build the converter of an ARRAY, which reads a list, or a tuple for an ARRAY declared ``as_tuple``."""
    if as_tuple:
        convert_list = _array_converter(convert_item)
        return lambda value: None if value is None else tuple(convert_list(value))

    def convert_array(value: Any) -> Optional[List[Any]]:
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError(f"expected an ARRAY value, got {value!r}")
        return value if convert_item is None else [convert_item(item) for item in value]

    return convert_array


def _map_converter(
    convert_key: Optional[Callable[[Any], Any]], convert_value: Optional[Callable[[Any], Any]]
) -> Callable[[Any], Any]:
    if convert_key is None and convert_value is None:
        return lambda value: None if value is None else dict(_pairs_of(value, "MAP"))
    key_of = convert_key or _unprocessed
    value_of = convert_value or _unprocessed

    def convert_map(value: Any) -> Optional[Dict[Any, Any]]:
        if value is None:
            return None
        return {key_of(key): value_of(item) for key, item in _pairs_of(value, "MAP")}

    return convert_map


def _struct_converter(type_: "STRUCT", dialect: Dialect, read_binary: Callable[[Any], bytes]) -> Callable[[Any], Any]:
    """Build the converter of a STRUCT, whose fields are found by name, the declared case first and then any case.

    A field the type does not declare is kept as it is decoded.
    """
    by_name = {name: _element_converter(field_type, dialect, read_binary) for name, field_type in type_.field_tuples}
    if all(convert is None for convert in by_name.values()):
        return lambda value: None if value is None else dict(_pairs_of(value, "STRUCT"))
    by_folded_name = {name.lower(): convert for name, convert in by_name.items()}

    def field_converter(name: Any) -> Optional[Callable[[Any], Any]]:
        if name in by_name:
            return by_name[name]
        return by_folded_name.get(name.lower()) if isinstance(name, str) else None

    def convert_struct(value: Any) -> Optional[Dict[Any, Any]]:
        if value is None:
            return None
        fields = {}
        for name, item in _pairs_of(value, "STRUCT"):
            convert = field_converter(name)
            fields[name] = item if convert is None else convert(item)
        return fields

    return convert_struct


def _declared_fields_converter(
    type_: "STRUCT", dialect: Dialect, read_binary: Callable[[Any], bytes]
) -> Callable[[Any], Any]:
    """Build the converter of a STRUCT read from an operand's text: its declared fields, each by its exact name.

    StarRocks' cast of JSON to STRUCT reads each field by its JSON path, so it drops a key the type does not
    declare, and a field whose key is missing, or in another case, is NULL.
    """
    fields = [
        (name, _element_converter(field_type, dialect, read_binary, from_operand_text=True))
        for name, field_type in type_.field_tuples
    ]

    def convert_declared_fields(value: Any) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        items = dict(_pairs_of(value, "STRUCT"))
        return {name: items.get(name) if convert is None else convert(items.get(name)) for name, convert in fields}

    return convert_declared_fields


def _pairs_of(value: Any, kind: str) -> Iterable[Tuple[Any, Any]]:
    """Return the ``(key, value)`` pairs of a decoded MAP or STRUCT, or of a dict given already decoded."""
    if value.__class__ is tuple:
        return value
    if isinstance(value, Mapping):
        return value.items()
    raise ValueError(f"expected a {kind} value, got {value!r}")


def _element_converter(
    type_: Any, dialect: Dialect, read_binary: Callable[[Any], bytes], from_operand_text: bool = False
) -> Optional[Callable[[Any], Any]]:
    """Build the reader of one element of a collection, ``None`` included, as a plain column of its type reads.

    The decoded value is first made what the driver returns for such a column, by the type underneath any
    TypeDecorator, and then goes through the declared type's own result processing: an Enum's, a
    TypeDecorator's ``process_result_value``, or PickleType's unpickling. ``None`` is returned when the decoded
    value is already the element's value, as a string is, so that it is not copied.

    ``from_operand_text`` builds the reader of :func:`operand_text_reader`, whose elements can be any JSON
    value; see :func:`_operand_text_converter`.
    """
    type_ = StructuredType._check_subtype(type_)
    if isinstance(type_, StructuredType):
        return _collection_converter(type_, dialect, read_binary, from_operand_text)
    impl = _structured_leaf_impl(type_, dialect)
    if from_operand_text and isinstance(impl, StructuredType):
        return _decorated_collection_converter(type_, impl, dialect, read_binary)
    as_driver_value = (from_operand_text and _operand_text_converter(impl)) or _driver_value_converter(
        impl, read_binary
    )
    process = type_.dialect_impl(dialect).result_processor(dialect, None)
    if as_driver_value is None:
        return process
    if process is None:
        return _skipping_none(as_driver_value)
    return lambda value: process(None if value is None else as_driver_value(value))


def _decorated_collection_converter(
    type_: sqltypes.TypeEngine, collection: StructuredType, dialect: Dialect, read_binary: Callable[[Any], bytes]
) -> Callable[[Any], Any]:
    """Build the reader of a TypeDecorator over a collection, inside a collection read from an operand's text."""
    convert = _collection_converter(collection, dialect, read_binary, from_operand_text=True)
    process_result_values = _decorator_result_values(type_, dialect)

    def convert_decorated(value: Any) -> Any:
        value = convert(value)
        for process_result_value in process_result_values:
            value = process_result_value(value, dialect)
        return value

    return convert_decorated


def _operand_text_converter(impl: sqltypes.TypeEngine) -> Optional[Callable[[Any], Any]]:
    """Build what converts an element of an operand's text to its type, where the server's own text needs none.

    The text is the operand's, such as JSON, rather than what StarRocks writes for the type, so an element can
    hold any JSON value. Each is converted as StarRocks' cast of JSON converts it: a number, ``true`` or
    ``false``, or an object or array in a string element reads as its JSON text, a JSON element as the JSON
    value, and a DATETIME with a UTC offset as its wall time, as the dialect binds one. A DECIMAL takes a number
    or its text only.
    """
    if isinstance(impl, sqltypes.JSON):
        return _json_text_of
    if isinstance(impl, sqltypes.String):
        return _text_of_operand_element
    if isinstance(impl, sqltypes.DateTime):
        return _wall_time_of_datetime_text
    if isinstance(impl, sqltypes.Numeric) and not isinstance(impl, sqltypes.Float):
        return _decimal_of_operand_element
    return None


def _text_of_operand_element(value: Any) -> str:
    return value if value.__class__ is str else _json_text_of(value)


def _decimal_of_operand_element(value: Any) -> Decimal:
    """Read a DECIMAL element, refusing a boolean or text that is not a number with ValueError."""
    if value.__class__ is not bool:
        try:
            return _as_decimal(value)
        except InvalidOperation:
            pass
    raise ValueError(f"could not read {value!r} as a DECIMAL")


def _wall_time_of_datetime_text(value: Any) -> Any:
    return _wall_time(_datetime_from_text(value))


def _json_text_of(decoded: Any) -> str:
    """Write a decoded value as the JSON text StarRocks writes for it, ``{"a": 1, "b": [1, 2]}``."""
    return json.dumps(_json_ready(decoded), ensure_ascii=False)


def _json_ready(decoded: Any) -> Any:
    """Return a decoded value as ``json.dumps`` writes it: a MAP's or STRUCT's pairs as a dict, a Decimal as a
    float, as StarRocks holds a fractional JSON number."""
    if decoded.__class__ is tuple:
        return {key: _json_ready(item) for key, item in decoded}
    if decoded.__class__ is list:
        return [_json_ready(item) for item in decoded]
    if decoded.__class__ is Decimal:
        return float(decoded)
    return decoded


def _driver_value_converter(
    impl: sqltypes.TypeEngine, read_binary: Callable[[Any], bytes]
) -> Optional[Callable[[Any], Any]]:
    """Build what turns a decoded element into the driver's value for a column of ``impl``, if they differ.

    The server writes a BOOLEAN as ``1`` or ``0``, a floating point number as a decimal and a binary value as
    text, and an integer given as a dict's string key is read too; see :func:`_boolean_from_its_spelling` for a
    BOOLEAN. A string, and the text of a date or time, already decode as the driver's value, or as the text the
    type's own result processing reads.
    """
    if isinstance(impl, sqltypes.Boolean):
        return _boolean_from_its_spelling
    if isinstance(impl, sqltypes.Integer):
        return int
    if isinstance(impl, sqltypes.Float):
        return float
    if isinstance(impl, sqltypes.Numeric):
        return _as_decimal
    if isinstance(impl, sqltypes._Binary):
        return read_binary
    return None


_BOOLEAN_SPELLINGS: Final[Mapping[str, bool]] = {"true": True, "false": False, "1": True, "0": False}
"""The text of a boolean that StarRocks' cast of a JSON string to BOOLEAN reads, in any case."""


def _boolean_from_its_spelling(value: Any) -> bool:
    """Read a BOOLEAN element: the ``1`` or ``0`` StarRocks writes, a JSON ``true`` or ``false``, or the text of
    one of these, in any case, as StarRocks' JSON cast reads it.

    Text that is not a collection StarRocks wrote, as read through a CAST the compiler left out, can hold any
    value, and Python's ``bool()`` reads every non-empty string, ``"false"`` included, as True. Any other value
    raises ValueError instead.
    """
    if value.__class__ is bool:
        return value
    if value.__class__ is int and (value == 0 or value == 1):
        return value == 1
    if value.__class__ is str:
        spelled = _BOOLEAN_SPELLINGS.get(value.lower())
        if spelled is not None:
            return spelled
    raise ValueError(f"could not read {value!r} as a BOOLEAN; StarRocks and JSON write one as 1, 0, true or false")


def _as_decimal(value: Any) -> Decimal:
    return value if value.__class__ is Decimal else Decimal(str(value))


def _skipping_none(convert: Callable[[Any], Any]) -> Callable[[Any], Any]:
    def convert_unless_none(value: Any) -> Any:
        return value if value is None else convert(value)

    return convert_unless_none
