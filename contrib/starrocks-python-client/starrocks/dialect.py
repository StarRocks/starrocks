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
from functools import cache, lru_cache, partial
import logging
import math
import re
import sys
from textwrap import dedent
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    Final,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)
import warnings

from alembic.ddl.base import (
    ColumnName,
    RenameTable,
    alter_table,
    format_column_name,
    format_server_default,
    format_table_name,
)
from alembic.ddl.mysql import (
    MySQLAlterDefault,
    MySQLChangeColumn,
    MySQLModifyColumn,
)
from alembic.operations.ops import AlterColumnOp
from alembic.util.sqla_compat import _literal_bindparam, compiles
from sqlalchemy import (
    Column,
    Table,
    exc,
    log,
    schema as sa_schema,
    text,
    util,
)
from sqlalchemy.dialects.mysql.base import (
    MySQLCompiler,
    MySQLDDLCompiler,
    MySQLIdentifierPreparer,
    MySQLTypeCompiler,
    _DecodingRow,
    colspecs as base_colspecs,
)
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
import sqlalchemy.dialects.mysql.types as mysql_types
from sqlalchemy.engine import URL, Connection, reflection
from sqlalchemy.engine.interfaces import ExecutionContext

#from sqlalchemy.engine.interfaces import ReflectedTableComment
from sqlalchemy.engine.row import Row
from sqlalchemy.sql import bindparam, operators, sqltypes
from sqlalchemy.sql.elements import (
    BinaryExpression,
    Cast,
    ClauseElement,
    ColumnClause,
    Grouping,
    Label,
    Null,
    TypeClause,
)
from sqlalchemy.sql.expression import CompoundSelect, Delete, Insert, Select, TableClause, Update

from starrocks.common import utils
from starrocks.common.consts import TableConfigKey
from starrocks.common.defaults import ReflectionMVDefaults, ReflectionTableDefaults, ReflectionViewDefaults
from starrocks.common.params import (
    ColumnAggInfoKey,
    ColumnAggInfoKeyWithPrefix,
    DialectName,
    TableInfoKey,
    TableInfoKeyWithPrefix,
    TableKind,
    TableObjectInfoKey,
)
from starrocks.common.types import (
    ColumnAggType,
    SystemRunMode,
    TableEngine,
    TableModel,
    TableType,
    ViewSecurityType,
)
from starrocks.common.utils import TableAttributeNormalizer
from starrocks.reserved_words import STARROCKS_RESERVED_WORDS
from starrocks.sql.schema import View


if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.interfaces import DBAPICursor

from . import reflection as _reflection
from .datatype import (
    ARRAY,
    BIGINT,
    BINARY,
    BITMAP,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    FLOAT,
    HLL,
    INTEGER,
    JSON,
    LARGEINT,
    MAP,
    PERCENTILE,
    SMALLINT,
    STRING,
    STRUCT,
    TIME,
    TINYINT,
    VARBINARY,
    VARCHAR,
    VARIANT,
    BinaryElementsAsHex,
    JSONIndexType,
    JSONPathType,
    LeftOutCastResult,
    check_struct_field_name,
    server_casts_json_to,
    stored_collection_type,
)
from .engine.interfaces import ReflectedMVState, ReflectedState, ReflectedViewState
from .reflection import StarRocksInspector
from .sql.ddl import (
    AlterMaterializedView,
    AlterTableColumns,
    AlterTableDistribution,
    AlterTableEngine,
    AlterTableKey,
    AlterTableOrder,
    AlterTablePartition,
    AlterTableProperties,
    AlterView,
    AlterViewSecurity,
    CreateMaterializedView,
    CreateView,
    DropMaterializedView,
    DropView,
    group_properties_for_set,
)


# Register the compiler methods
# The @compiles decorator is the public API for registering new SQL constructs.
# However, we are now using the internal `__visit_name__` attribute on the
# DDLElement classes themselves to hook into the visitor pattern, which is
# consistent with how SQLAlchemy's own constructs are implemented.
# compiles(CreateView)(StarRocksDDLCompiler.visit_create_view)
# compiles(DropView)(StarRocksDDLCompiler.visit_drop_view)
# compiles(CreateMaterializedView)(StarRocksDDLCompiler.visit_create_materialized_view)
# compiles(DropMaterializedView)(StarRocksDDLCompiler.visit_drop_materialized_view)


##############################################################################################
# NOTES - INCOMPLETE/UNFINISHED
# There are a number of items in here marked as ToDo
# In terms of table creation, the Partition, Distribution and OrderBy clauses need to be addressed from table options
# Tests `test_has_index` and `test_has_index_schema` are failing, this is because the CREATE INDEX statement appears to
# work async and only when it's finished does it appear in the table definition
# Other tests are failing, need to fix or figure out how to suppress
# Review some skipped test suite requirements
#
#
#
##############################################################################################

logger = logging.getLogger(__name__)

# starrocks supported data types
ischema_names = {
    # === Boolean ===
    "boolean": BOOLEAN,
    # === Integer ===
    "tinyint": TINYINT,
    "smallint": SMALLINT,
    "int": INTEGER,
    "integer": INTEGER,
    "bigint": BIGINT,
    "largeint": LARGEINT,
    # === Floating-point ===
    "float": FLOAT,
    "double": DOUBLE,
    # === Fixed-precision ===
    "decimal": DECIMAL,
    "decimal32": DECIMAL,
    "decimal64": DECIMAL,
    "decimal128": DECIMAL,
    "decimal256": DECIMAL,
    "decimalv2": DECIMAL,
    "numeric": DECIMAL,
    "number": DECIMAL,
    # === String ===
    "varchar": VARCHAR,
    "char": CHAR,
    "string": STRING,
    "text": STRING,
    "json": JSON,
    "variant": VARIANT,
    # === Date and time ===
    "date": DATE,
    "datetime": DATETIME,
    "timestamp": DATETIME,
    "time": TIME,
    # == binary ==
    "binary": BINARY,
    "varbinary": VARBINARY,
    # === Structural ===
    "array": ARRAY,
    "map": MAP,
    "struct": STRUCT,
    "hll": HLL,
    "percentile": PERCENTILE,
    "bitmap": BITMAP,
}

colspecs = {
    generic_type: impl_type for generic_type, impl_type in base_colspecs.items() if generic_type is not sqltypes.Enum
} | {
    mysql_types.BIGINT: BIGINT,
    sqltypes.Date: DATE,
    sqltypes.DateTime: DATETIME,
    sqltypes.DECIMAL: DECIMAL,
    sqltypes.Time: TIME,
    sqltypes.ARRAY: ARRAY,
    sqltypes.JSON: JSON,
    sqltypes.JSON.JSONIndexType: JSONIndexType,
    sqltypes.JSON.JSONPathType: JSONPathType,
}

def require_server_version(
    dialect: "StarRocksDialect",
    minimum: Tuple[int, ...],
    feature: str,
    error: Type[exc.SQLAlchemyError] = exc.CompileError,
) -> None:
    """Raise ``error`` when the connected StarRocks is older than ``minimum``, the first version with ``feature``.

    Compiling without a connection, as Alembic's offline mode does, knows no server version and checks nothing.
    """
    version = dialect.server_version_info
    if version and tuple(version) < minimum:
        raise error(
            f"{feature} needs StarRocks {'.'.join(map(str, minimum))} or later; "
            f"the server is {'.'.join(map(str, version))}"
        )


class SchemaChangeTimeoutError(exc.SQLAlchemyError):
    """A StarRocks schema change on a table was still running after ``starrocks_schema_change_timeout`` seconds.

    Not the built-in TimeoutError: from Python 3.11 that is asyncio's, which SQLAlchemy takes for a cancellation
    and invalidates the connection on.
    """


class SchemaChangeJob(NamedTuple):
    """A StarRocks schema-change job, as a row of ``SHOW ALTER TABLE COLUMN``, ``OPTIMIZE`` or ``ROLLUP`` gives it."""

    job_id: str
    state: str
    message: str


@cache
def _dialect_leaving_percents_single() -> "StarRocksDialect":
    """A StarRocks dialect with the ``named`` paramstyle, as Alembic configures one for offline SQL scripts."""
    return StarRocksDialect(paramstyle="named")


def stored_sql_text(clause: ClauseElement) -> str:
    """Compile a clause to the SQL text StarRocks keeps and shows back, such as a view definition, values inline.

    A dialect whose paramstyle does not format with ``%`` leaves each ``%`` single, as in the text StarRocks
    reflects. :meth:`StarRocksDDLCompiler.sql_text` doubles it when the text goes into DDL.

    The dialect knows no server version, so the text is the same for every server, and only the CASTs to ARRAY,
    MAP or STRUCT that every supported StarRocks runs are kept. Each CAST left out warns, since StarRocks
    computes on, and stores, the operand's own type, see :data:`STORED_SQL_TEXT_COMPILE_KWARG`.
    """
    return str(
        clause.compile(
            dialect=_dialect_leaving_percents_single(),
            compile_kwargs={"literal_binds": True, STORED_SQL_TEXT_COMPILE_KWARG: True},
        )
    )


@lru_cache(maxsize=1024)
def warn_once_of_doubled_percents(sql: str) -> None:
    """Warn that DDL text holds ``%%``, once for each distinct text rather than on every compile.

    Earlier versions of the dialect handed such text to the driver as written, so online a single ``%`` raised
    and users wrote ``%%``, which the driver turned back into ``%``. :meth:`StarRocksDDLCompiler.sql_text` now
    escapes ``%`` for the driver itself, so an existing ``%%`` reaches StarRocks as ``%%``, which changes what a
    ``date_format`` or ``str2date`` pattern means. The cache remembers the texts already warned about.

    A ``%%`` can be meant, as in the string literal ``'a%%b'``, and the dialect can't tell it from one written for
    earlier versions, so the warning says how to silence it for a ``%%`` that is meant.
    """
    shown = sql if len(sql) <= 120 else f"{sql[:117]}..."
    util.warn(
        f"DDL text {shown!r} holds '%%'. The StarRocks dialect now escapes '%' for the driver itself, so '%%' "
        "reaches StarRocks as '%%'; write a single '%' instead (earlier versions of the dialect needed '%%'). "
        "To keep a '%%' you mean, such as one in a string literal, silence this warning with "
        "warnings.filterwarnings(\"ignore\", message=\"DDL text .* holds '%%'\")"
    )


class StarRocksTypeCompiler(MySQLTypeCompiler):
    """
    Compile a datatype to StarRocks' SQL type string.
    For special types only, use the default implementation in MySQLTypeCompiler for other types.

    NOTE: StarRocks will ignore the length of Integer types, so we need to care about the
    comparison of Integer types.
    NOTE: StarRocks does not support scale of Float types.
    """

    def process(self, type_: sqltypes.TypeEngine, **kw: Any) -> str:
        """Compile a type; a float element of a CAST's target is DOUBLE where a CAST to it is, see
        :data:`CAST_TARGET_COMPILE_KWARG`."""
        if kw.get(CAST_TARGET_COMPILE_KWARG) and StarRocksSQLCompiler.casts_float_as_double(type_, self.dialect):
            return "DOUBLE"
        return super().process(type_, **kw)

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_FLOAT(self, type_, **kw):
        return "FLOAT"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_REAL(self, type_: sqltypes.REAL, **kw: Any) -> str:
        """StarRocks has no REAL; it reads the REAL of MySQL, PostgreSQL and Trino tables as its 4-byte FLOAT."""
        return "FLOAT"

    def visit_LARGEINT(self, type_, **kw):
        if getattr(type_, "display_width", None) is not None:
            return f"LARGEINT({type_.display_width})"
        return "LARGEINT"

    def visit_NVARCHAR(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_STRING(self, type_, **kw):
        return "STRING"

    def visit_DATETIME(self, type_: sqltypes.DateTime, **kw: Any) -> str:
        """StarRocks DATETIME has no precision argument; ``DATETIME(6)`` is a syntax error."""
        return "DATETIME"

    def visit_TIMESTAMP(self, type_: sqltypes.TIMESTAMP, **kw: Any) -> str:
        """TIMESTAMP is not a StarRocks type keyword; DATETIME is the equivalent."""
        return "DATETIME"

    def visit_enum(self, type_: sqltypes.Enum, **kw: Any) -> str:
        """StarRocks has no ENUM type, so sa.Enum is always stored as VARCHAR.

        VARCHAR(n) counts bytes in StarRocks, so n is at least the longest value's UTF-8 length, not its character
        count, and at least the ``length`` the Enum asks for.
        """
        longest_value_bytes = max((len(value.encode("utf-8")) for value in type_.enums), default=None)
        if longest_value_bytes is not None and type_.length:
            longest_value_bytes = max(longest_value_bytes, type_.length)
        return self.visit_VARCHAR(sqltypes.String(longest_value_bytes), **kw)

    def visit_NCHAR(self, type_: sqltypes.NCHAR, **kw: Any) -> str:
        """StarRocks has no NATIONAL CHAR; its CHAR is UTF-8 already, as NVARCHAR's VARCHAR is."""
        return self.visit_CHAR(type_, **kw)

    def visit_CHAR(self, type_: sqltypes.CHAR, **kw: Any) -> str:
        """Render ``CHAR(n)`` or ``CHAR`` alone, as :meth:`visit_VARCHAR` renders VARCHAR.

        MySQL's compiler adds NATIONAL for ``mysql.NCHAR`` and ``CHAR(national=True)``, and COLLATE or BINARY for
        a collation, all of which StarRocks refuses in a column definition.
        """
        return "CHAR" if type_.length is None else f"CHAR({type_.length})"

    def visit_TEXT(self, type_: sqltypes.Text, **kw: Any) -> str:
        """StarRocks stores TEXT as varchar(65533), which is what STRING means."""
        return "STRING"

    def visit_VARCHAR(self, type_, **kw):
        # StarRocks supports unbounded strings via STRING. When no length is
        # provided (e.g. from SQLAlchemy String() without length), render
        # STRING instead of requiring a VARCHAR length.
        if getattr(type_, "length", None) is None or type_.length == 65533:
            return "STRING"
        return f"VARCHAR({type_.length})"

    def visit_BINARY(self, type_: sqltypes.BINARY, **kw: Any) -> str:
        return self._binary_with_length("BINARY", type_)

    def visit_VARBINARY(self, type_: sqltypes.VARBINARY, **kw: Any) -> str:
        return self._binary_with_length("VARBINARY", type_)

    @staticmethod
    def _binary_with_length(keyword: str, type_: sqltypes.TypeEngine) -> str:
        """Render a binary keyword with the declared length; StarRocks stores BINARY(n) as varbinary(n)."""
        length = getattr(type_, "length", None)
        return keyword if length is None else f"{keyword}({length})"

    def visit_ARRAY(self, type_: Union[ARRAY, sqltypes.ARRAY], **kw: Any) -> str:
        """Render ``ARRAY<item>``; SQLAlchemy's generic ARRAY is adapted first, so its ``dimensions`` nest."""
        if not isinstance(type_, ARRAY):
            type_ = type_.dialect_impl(self.dialect)
        inner_type_sql = self.process(type_.item_type, **kw)
        return f"ARRAY<{inner_type_sql}>"

    def visit_MAP(self, type_: MAP, **kw):
        # logger.debug("visit_MAP: type_: %r, kw: %s", type_, kw)
        key_type_sql = self.process(type_.key_type, **kw)
        value_type_sql = self.process(type_.value_type, **kw)
        return f"MAP<{key_type_sql}, {value_type_sql}>"

    def visit_STRUCT(self, type_: STRUCT, **kw: Any) -> str:
        """Render ``STRUCT<name type, ...>``, with each field name quoted by the identifier preparer.

        The preparer quotes a field name as it quotes a column name: a name StarRocks reserves, such as ``key``,
        or one that is not a plain lowercase identifier, is backquoted, and a ``%`` is doubled for a driver that
        formats parameters with ``%``. A name holding a backquote is refused, see
        :func:`~starrocks.datatype.check_struct_field_name`.
        """
        preparer = self.dialect.identifier_preparer
        fields_sql = []
        for name, field_type in type_.field_tuples:
            check_struct_field_name(name)
            fields_sql.append(f"{preparer.quote(name)} {self.process(field_type, **kw)}")
        return f"STRUCT<{', '.join(fields_sql)}>"

    def visit_HLL(self, type_, **kw):
        return "HLL"

    def visit_BITMAP(self, type_, **kw):
        return "BITMAP"

    def visit_PERCENTILE(self, type_: PERCENTILE, **kw: Any) -> str:
        return "PERCENTILE"

    def visit_VARIANT(self, type_, **kw):
        require_server_version(self.dialect, (4, 1), "The VARIANT type")
        return "VARIANT"

    def visit_BLOB(self, type_: sqltypes.LargeBinary, **kw: Any) -> str:
        """StarRocks has no BLOB; a LargeBinary is a BINARY of its declared length."""
        return self._binary_with_length("BINARY", type_)


CAST_TARGET_COMPILE_KWARG: Final[str] = "starrocks_cast_target"
"""The type compiler keyword that marks the type compiled as a CAST's target, an ARRAY, MAP or STRUCT.

Each float element in it is then DOUBLE where a CAST to the element's type would be, see
:meth:`StarRocksSQLCompiler.casts_float_as_double`; a column of the type is still created with FLOAT.
"""

ResultMapAppender = Callable[[Any, Any, Tuple[Any, ...], sqltypes.TypeEngine], None]
"""The ``add_to_result_map`` callable through which the compiler records each column of a statement's result."""

STORED_SQL_TEXT_COMPILE_KWARG: Final[str] = "starrocks_stored_sql_text"
"""The compile keyword that marks SQL compiled to be stored, such as a view's definition, rather than run.

Such SQL is compiled with no server version, and every CAST it leaves out warns, see
:meth:`StarRocksSQLCompiler._warn_unless_read_as_result`.
"""


class StarRocksSQLCompiler(MySQLCompiler):
    render_table_with_column_in_update_from = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Set up what :meth:`visit_select` and :meth:`visit_label` track, before the statement compiles.

        SQLAlchemy compiles the statement in the constructor, so this comes before it.
        """
        self._selects_compiled_as_from: List[bool] = []
        self._labelled_select_columns: List[ClauseElement] = []
        super().__init__(*args, **kwargs)

    def visit_sysdate_func(self, fn: Any, **kw: Any) -> str:
        """StarRocks has no SYSDATE(); NOW() is its current-timestamp function, and takes the same precision."""
        return "now" + self.function_argspec(fn, **kw)

    def visit_delete(self, delete_stmt: Delete, **kw: Any) -> str:
        result: str = super().visit_delete(delete_stmt, **kw)
        compile_state: Any = delete_stmt._compile_state_factory(
            delete_stmt, self, **kw
        )
        delete_stmt = compile_state.statement
        table: str = self.delete_table_clause(
            delete_stmt, delete_stmt.table, False
        )
        if not delete_stmt._where_criteria:
            return "TRUNCATE TABLE " + table
        return result

    _rows_paged_by_dialect: bool = False
    """Whether an executemany of this INSERT is one the driver would send a row at a time; see :meth:`visit_insert`."""

    def visit_insert(self, insert_stmt: Insert, **kw: Any) -> str:
        """Render an INSERT, and note whether the dialect sends the rows of an executemany of it.

        The driver sends such an INSERT once per row when its VALUES row holds more than bare placeholders, as
        the bind expression of a JSON, ARRAY, MAP or STRUCT column does; :meth:`StarRocksDialect.do_executemany`
        then sends the rows itself.
        """
        toplevel = not self.stack
        text = self._render_overwrite_and_label(insert_stmt, super().visit_insert(insert_stmt, **kw))
        if toplevel and self.for_executemany:
            self._rows_paged_by_dialect = self.dialect._driver_sends_row_by_row(text)
        return text

    def _render_overwrite_and_label(self, insert_stmt: Insert, text: str) -> str:
        """Render ``INSERT OVERWRITE`` and ``WITH LABEL`` when the ``starrocks_overwrite`` and ``starrocks_label`` options ask.

        StarRocks takes the label straight after the table, ``INSERT INTO t WITH LABEL l (a, b) VALUES ...``, and
        replaces ``INTO`` with ``OVERWRITE``. The base compiler renders the whole statement, so the head is rewritten
        around the table name it just emitted.
        """
        overwrite = bool(insert_stmt.dialect_options[self.dialect.name]["overwrite"])
        label = insert_stmt.dialect_options[self.dialect.name]["label"]
        if not overwrite and label is None:
            return text
        into_table = "INTO " + self.preparer.format_table(insert_stmt.table)
        head, found, tail = text.partition(into_table)
        if not found:
            raise exc.CompileError(f"Cannot apply starrocks_overwrite or starrocks_label to: {text}")
        keyword = "OVERWRITE" if overwrite else "INTO"
        label_clause = "" if label is None else " WITH LABEL " + self._quoted_insert_label(label)
        return f"{head}{keyword} {into_table[len('INTO '):]}{label_clause}{tail}"

    _INSERT_LABEL: Final = re.compile(r"[-A-Za-z0-9_]{1,128}")
    """The labels StarRocks takes: ``^[-\\w]{1,128}$`` in its frontend, where ``\\w`` is ASCII."""

    def _quoted_insert_label(self, label: Any) -> str:
        """Quote ``label``, refusing one that StarRocks would refuse or record under another name.

        StarRocks refuses a label outside :attr:`_INSERT_LABEL`, and drops a backquote, which the identifier
        quoting doubles, so ``lbl`q`` would be recorded as ``lblq`` with no error.
        """
        if not isinstance(label, str) or self._INSERT_LABEL.fullmatch(label) is None:
            raise exc.CompileError(
                f"starrocks_label must be 1 to 128 ASCII letters, digits, '_' or '-', as StarRocks requires, "
                f"not {label!r}"
            )
        return self.preparer.quote(label)

    _JSON_ACCESS_OPERATORS: Final[Tuple[Any, ...]] = (operators.json_getitem_op, operators.json_path_getitem_op)

    def _render_json_extract_from_binary(self, binary: Any, operator: Any, **kw: Any) -> str:
        """Render a JSON index or path access with ``json_query`` or the ``get_json_*`` function for its type.

        StarRocks has no ``JSON_EXTRACT``. ``json_query`` returns JSON, with a JSON null kept as JSON null, and
        the ``get_json_*`` functions return the SQL type of the accessor (``as_integer()`` and the like): a
        missing key or a JSON null is SQL NULL, and a value of another type is converted as StarRocks casts it,
        so the JSON string ``"5"`` reads as 5 and ``1.5`` as 1.

        A chained access such as ``j["arr"][1]`` is one call with its paths joined at run time: StarRocks returns
        NULL when ``json_query`` indexes into an array an inner ``json_query`` returned.

        ``as_numeric(precision, scale)`` is rendered by :meth:`_render_json_decimal_access`.
        """
        decimal_type = self._exact_decimal_access_type(binary.type)
        if decimal_type is not None:
            return self._render_json_decimal_access(binary, decimal_type, **kw)
        return self._render_json_access(self._json_access_function(binary.type), binary, **kw)

    def _render_json_access(self, function: str, binary: Any, **kw: Any) -> str:
        """Render ``function(document, path)`` for a JSON access, a chain's paths joined into one."""
        document, paths = self._json_document_and_paths(binary, **kw)
        path = paths[0] if len(paths) == 1 else "concat(%s)" % ", ".join(
            [paths[0], *(f"substring({later}, 2)" for later in paths[1:])]
        )
        return f"{function}({document}, {path})"

    def _render_json_decimal_access(self, binary: Any, decimal_type: str, **kw: Any) -> str:
        """Render ``as_numeric(p, s)``: the value's text cast to the DECIMAL, which keeps every digit StarRocks stores.

        StarRocks stores a JSON number with a fraction as a DOUBLE, so ``99998969694839.983485848`` inside an
        object or array is stored, and read, as ``99998969694839.98``. An integer such as
        ``12345678901234567890``, and a number written as a JSON string, keep all their digits. Read through
        ``get_json_double``, a value past 2**53, or with more than about 16 digits, would be rounded.
        The text of a JSON ``true`` or ``false`` is not a number, so when the text cast is NULL the value is cast
        through a DOUBLE as well, which reads a boolean as 1 or 0, as ``get_json_double`` does, and anything else
        it cannot hold as NULL. That second read is ``json_query``, not ``get_json_double``: StarRocks 4.1.6
        refuses an expression combining ``get_json_string`` and ``get_json_double`` of one key, with "only found
        column statistics".
        """
        exact = self._render_json_access("get_json_string", binary, **kw)
        whole = self._render_json_access("json_query", binary, **kw)
        return f"COALESCE(CAST({exact} AS {decimal_type}), CAST(CAST({whole} AS DOUBLE) AS {decimal_type}))"

    @staticmethod
    def _exact_decimal_access_type(access_type: sqltypes.TypeEngine) -> Optional[str]:
        """Return ``DECIMAL(p, s)`` for an ``as_numeric(p, s)`` access, and ``None`` for any other."""
        if access_type._type_affinity is not sqltypes.Numeric:
            return None
        precision, scale = access_type.precision, access_type.scale
        if precision is None or scale is None:
            return None
        return f"DECIMAL({precision}, {scale})"

    def _json_document_and_paths(self, binary: Any, **kw: Any) -> Tuple[str, List[str]]:
        """Return the JSON document a chain of accesses starts from, and the path of each access in order.

        SQLAlchemy groups the inner access when the chain mixes an index with a path, as ``j[("a", 2)][1]``
        does, so the grouping is looked through to find it.
        """
        left = self._ungrouped(binary.left)
        if isinstance(left, BinaryExpression) and left.operator in self._JSON_ACCESS_OPERATORS:
            document, paths = self._json_document_and_paths(left, **kw)
        else:
            document, paths = self.process(binary.left, **kw), []
        return document, [*paths, self.process(binary.right, **kw)]

    @staticmethod
    def _ungrouped(element: Any) -> Any:
        while isinstance(element, Grouping):
            element = element.element
        return element

    def _json_access_function(self, access_type: sqltypes.TypeEngine) -> str:
        affinity = access_type._type_affinity
        if affinity is sqltypes.JSON:
            return "json_query"
        if affinity is sqltypes.Integer:
            return "get_json_int"
        if affinity is sqltypes.Boolean:
            return "get_json_bool"
        if affinity is sqltypes.String:
            return "get_json_string"
        if issubclass(affinity, sqltypes.Numeric):
            return "get_json_double"
        return "json_query"

    def update_tables_clause(
        self,
        update_stmt: Update,
        from_table: Any,
        extra_froms: list[Any],
        **kw: Any,
    ) -> str:
        """Render only the updated table; StarRocks takes the other tables from ``FROM``, not a comma list."""
        kw["asfrom"] = True
        return from_table._compiler_dispatch(self, **kw)

    def update_from_clause(
        self,
        update_stmt: Update,
        from_table: Any,
        extra_froms: list[Any],
        from_hints: Any,
        **kw: Any,
    ) -> str:
        """Render ``FROM b, c`` for the extra tables, as ``UPDATE a SET ... FROM b, c WHERE ...``."""
        kw["asfrom"] = True
        return "FROM " + ", ".join(
            t._compiler_dispatch(self, fromhints=from_hints, **kw) for t in extra_froms
        )

    def delete_extra_from_clause(
        self,
        delete_stmt: Delete,
        from_table: Any,
        extra_froms: list[Any],
        from_hints: Any,
        **kw: Any,
    ) -> str:
        """Render ``USING b, c`` for the extra tables only; StarRocks rejects the deleted table repeated in it."""
        kw["asfrom"] = True
        return "USING " + ", ".join(
            t._compiler_dispatch(self, fromhints=from_hints, **kw) for t in extra_froms
        )

    _LIMIT_FOR_OFFSET_ALONE: Final[int] = 2**62 - 1

    def limit_clause(self, select: Select, **kw: Any) -> str:
        """Render ``LIMIT <limit>`` and ``LIMIT <limit> OFFSET <offset>``, the forms StarRocks takes.

        StarRocks has no OFFSET without a LIMIT, so an offset alone gets a LIMIT of 2**62 - 1. StarRocks refuses
        a limit and offset whose sum overflows a signed 64-bit integer, which leaves room for any offset below 2**62.
        """
        text = ""
        if select._limit_clause is not None:
            text += "\n LIMIT " + self.process(select._limit_clause, **kw)
        elif select._offset_clause is not None:
            text += f"\n LIMIT {self._LIMIT_FOR_OFFSET_ALONE}"
        if select._offset_clause is not None:
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text

    def visit_cast(self, cast: Any, **kw: Any) -> str:
        """Render a cast of a JSON expression to a string type as ``json_string``, which yields the JSON text.

        ``CAST(<JSON> AS VARCHAR)`` on StarRocks drops the quotes of a JSON string and turns a JSON null into SQL
        NULL, whereas SQLAlchemy documents the cast as the JSON text (``"value"``, ``null``). ``json_string``
        returns that text, and SQL NULL for a missing value.

        A CAST to ARRAY, MAP or STRUCT that StarRocks can't run is rendered as its operand alone, see
        :meth:`_leaves_out_collection_cast`, with a warning unless the CAST is a column of the statement's
        result, see :meth:`_warn_unless_read_as_result`.
        """
        casts_json_to_string = (
            cast.clause.type._type_affinity is sqltypes.JSON
            and cast.typeclause.type._type_affinity is sqltypes.String
        )
        if casts_json_to_string:
            return f"json_string({self.process(cast.clause, **kw)})"
        if self._leaves_out_collection_cast(cast):
            self._warn_unless_read_as_result(cast, cast, kw)
            return self.process(cast.clause.self_group(), **kw)
        return super().visit_cast(cast, **kw)

    def _leaves_out_collection_cast(self, cast: Any) -> bool:
        """Whether ``cast`` is a CAST to ARRAY, MAP or STRUCT that StarRocks can't run on its operand.

        Such a CAST is left out, as MySQL's compiler left out every CAST to these types, and the operand's text is
        read as the type by the type's result processing. That covers:

        - text: StarRocks refuses text to MAP and STRUCT, and its text cast to ARRAY is not a JSON parser, so it
          turns quoted dates, escaped quotes and fractions into NULL, where the reader reads them exactly;
        - an operand with no type, such as ``literal_column()``, ``text()`` or a typeless ``column()``, which can
          be text;
        - JSON, where :func:`server_casts_json_to` finds no such cast in the connected StarRocks.

        A collection operand, ``null()``, and any other operand keep the CAST.
        """
        if stored_collection_type(cast.typeclause.type, self.dialect) is None or isinstance(cast.clause, Null):
            return False
        operand_type = cast.clause.type
        if operand_type._isnull:
            return True
        operand_affinity = operand_type._type_affinity
        if isinstance(operand_affinity, type) and issubclass(operand_affinity, sqltypes.String):
            return True
        return operand_affinity is sqltypes.JSON and not server_casts_json_to(cast.typeclause.type, self.dialect)

    def visit_select(self, select_stmt: Select, asfrom: bool = False, **kw: Any) -> str:
        """Compile a SELECT, noting whether it is a FROM item, whose columns the enclosing query reads."""
        self._selects_compiled_as_from.append(asfrom)
        try:
            return super().visit_select(select_stmt, asfrom=asfrom, **kw)
        finally:
            self._selects_compiled_as_from.pop()

    def visit_label(
        self,
        label: Label,
        within_columns_clause: bool = False,
        within_label_clause: bool = False,
        render_label_as_label: Optional[Label] = None,
        **kw: Any,
    ) -> str:
        """Compile a label, noting the expression it names while it is a column of a SELECT.

        A label rendered by its name alone, as ORDER BY or GROUP BY refer to a column, is computed on, so a CAST
        left out behind it warns, see :meth:`_warn_unless_read_as_result`. A column of the result that is a
        left-out CAST is read from its operand's text, see :meth:`_result_map_reading_operand_text`.
        """
        label_kw = dict(
            within_columns_clause=within_columns_clause,
            within_label_clause=within_label_clause,
            render_label_as_label=render_label_as_label,
        )
        if render_label_as_label is label:
            left_out_cast = self._left_out_cast_behind(label)
            if left_out_cast is not None and not kw.get(STORED_SQL_TEXT_COMPILE_KWARG):
                self._warn_of_left_out_cast(left_out_cast, stored=False)
        elif within_columns_clause and not within_label_clause:
            column = self._select_column_expression(label.element)
            kw["add_to_result_map"] = self._result_map_reading_operand_text(column, kw.get("add_to_result_map"))
            self._labelled_select_columns.append(column)
            try:
                return super().visit_label(label, **label_kw, **kw)
            finally:
                self._labelled_select_columns.pop()
        return super().visit_label(label, **label_kw, **kw)

    def visit_column(self, column: ColumnClause, **kw: Any) -> str:
        """Compile a column; one of a subquery that a CAST left out warns where the query computes on it.

        See :meth:`_warn_unless_read_as_result`. As a column of the result, it is read from the operand's text,
        see :meth:`_result_map_reading_operand_text`.
        """
        if column._proxies and not kw.get(STORED_SQL_TEXT_COMPILE_KWARG):
            left_out_cast = self._left_out_cast_behind(column)
            if left_out_cast is not None:
                self._warn_unless_read_as_result(column, left_out_cast, kw)
                if self._is_select_column(column, kw):
                    kw["add_to_result_map"] = self._result_map_reading_operand_text(column, kw.get("add_to_result_map"))
        return super().visit_column(column, **kw)

    def _result_map_reading_operand_text(
        self, column: ClauseElement, add_to_result_map: Optional[ResultMapAppender]
    ) -> Optional[ResultMapAppender]:
        """Return ``add_to_result_map``, made to read ``column`` from its operand's text when it is a column of
        the statement's result that is a left-out CAST, or a subquery column that is one.

        The server sends the operand's text, so the column is read by
        :class:`~starrocks.datatype.LeftOutCastResult` rather than by the CAST type's own reader.
        """
        if (
            add_to_result_map is None
            or self._left_out_cast_behind(column) is None
            or self._how_select_columns_are_read() != "result"
        ):
            return add_to_result_map

        def add_column_read_from_operand_text(
            keyname: Any, name: Any, objects: Tuple[Any, ...], type_: sqltypes.TypeEngine
        ) -> None:
            add_to_result_map(keyname, name, objects, LeftOutCastResult(type_))

        return add_column_read_from_operand_text

    def _warn_unless_read_as_result(self, element: ClauseElement, left_out_cast: Cast, kw: Dict[str, Any]) -> None:
        """Warn of ``left_out_cast`` unless ``element`` is a column of the statement's result, read as its type.

        The type's result processing reads the value from the operand's text, which is right only where the
        dialect reads the value: a column of the result, or a column of a subquery that the enclosing query
        reads as it is, which is then checked where the enclosing query names it. Anywhere else StarRocks
        computes on the operand's own type: in a comparison, ORDER BY, GROUP BY or DISTINCT, a function's
        argument, a subquery the query computes on, and every stored definition, such as a view's or a
        materialized view's, see :data:`STORED_SQL_TEXT_COMPILE_KWARG`.
        """
        stored = bool(kw.get(STORED_SQL_TEXT_COMPILE_KWARG))
        if not stored and self._is_select_column(element, kw) and self._how_select_columns_are_read() != "computed":
            return
        self._warn_of_left_out_cast(left_out_cast, stored)

    def _left_out_cast_behind(self, element: ClauseElement) -> Optional[Cast]:
        """Return the left-out CAST that ``element`` is, labels and groupings apart, or that a subquery column
        selects, through every subquery between them; otherwise None."""
        element = self._select_column_expression(element)
        if isinstance(element, Cast):
            return element if self._leaves_out_collection_cast(element) else None
        if isinstance(element, ColumnClause) and stored_collection_type(element.type, self.dialect) is not None:
            for proxied in element._proxies:
                left_out_cast = self._left_out_cast_behind(proxied)
                if left_out_cast is not None:
                    return left_out_cast
        return None

    @staticmethod
    def _select_column_expression(element: ClauseElement) -> ClauseElement:
        """Return the expression a SELECT column computes, through labels, groupings and the hex selection of
        binary elements, :class:`~starrocks.datatype.BinaryElementsAsHex`."""
        while isinstance(element, (Label, Grouping, BinaryElementsAsHex)):
            element = element.element
        return element

    def _is_select_column(self, element: ClauseElement, kw: Dict[str, Any]) -> bool:
        """Whether ``element`` is itself a column of the SELECT being compiled, rather than part of one.

        A labelled column is the one :meth:`visit_label` noted; an unlabelled one is compiled straight from the
        columns clause, and is one of the SELECT's selected columns.
        """
        if self._labelled_select_columns and self._labelled_select_columns[-1] is element:
            return True
        if not kw.get("within_columns_clause") or kw.get("within_label_clause") or not self.stack:
            return False
        select = self.stack[-1]["selectable"]
        return isinstance(select, Select) and any(column is element for column in select.selected_columns)

    def _how_select_columns_are_read(self) -> str:
        """How the columns of the SELECT being compiled are read: ``"result"``, ``"from"`` or ``"computed"``.

        They are the statement's ``"result"`` for the top SELECT, and for each SELECT of a top UNION ALL. A FROM
        item's columns are read by the enclosing query, ``"from"``, and :meth:`visit_column` checks each where
        that query names it. They are ``"computed"`` where the SELECT compares them, under DISTINCT, UNION,
        INTERSECT or EXCEPT, and in every other subquery, such as a scalar one, or the SELECT of an INSERT.
        """
        select = self.stack[-1]["selectable"]
        if getattr(select, "_distinct", False) or getattr(select, "_distinct_on", ()):
            return "computed"
        depth = len(self.stack) - 1
        while depth and isinstance(self.stack[depth - 1]["selectable"], CompoundSelect):
            if self.compound_keywords[self.stack[depth - 1]["selectable"].keyword] != "UNION ALL":
                return "computed"
            depth -= 1
        if not depth:
            return "result"
        return "from" if self._selects_compiled_as_from and self._selects_compiled_as_from[-1] else "computed"

    def _warn_of_left_out_cast(self, left_out_cast: Cast, stored: bool) -> None:
        """Warn that StarRocks computes on the operand's own type where ``left_out_cast`` is left out."""
        target = self.dialect.type_compiler.process(stored_collection_type(left_out_cast.typeclause.type, self.dialect))
        if stored:
            consequence = (
                f"In a view, materialized view or other stored definition, which compiles with no server version, "
                f"StarRocks computes on the operand's own type, not on {target}, and the definition's column has "
                f"that type."
            )
        else:
            consequence = (
                f"Outside a column of the statement's result, as here, StarRocks computes on the operand's own "
                f"type, not on {target}."
            )
        util.warn(
            f"The CAST to {target} is left out, since {self._why_collection_cast_is_left_out(left_out_cast)}. "
            f"{consequence}"
        )

    def _why_collection_cast_is_left_out(self, left_out_cast: Cast) -> str:
        """Say why :meth:`_leaves_out_collection_cast` leaves out ``left_out_cast``."""
        operand_type = left_out_cast.clause.type
        if operand_type._isnull:
            return "the operand has no type, and may be text"
        if operand_type._type_affinity is not sqltypes.JSON:
            return "StarRocks has no CAST of text to it that reads the text as JSON"
        version = self.dialect.server_version_info
        if not version:
            return "no server version is known, and not every supported StarRocks casts JSON to it"
        return f"StarRocks {'.'.join(map(str, version))} can't cast JSON to it"

    def visit_typeclause(
        self,
        typeclause: TypeClause,
        type_: Optional[sqltypes.TypeEngine] = None,
        **kw: Any,
    ) -> str:
        """Render a CAST target; booleans, integers and ARRAY, MAP and STRUCT are cast to StarRocks' own types.

        MySQL casts integers to ``SIGNED INTEGER``, which StarRocks reads as INT on 3.3 and 3.5 and as BIGINT on
        4.1, so a BIGINT or LARGEINT cast lost its range on one version or the other. See
        :meth:`_integer_cast_target` for the integer types, and :meth:`casts_float_as_double` for the float
        types, an ARRAY's, MAP's or STRUCT's elements among them, see :data:`CAST_TARGET_COMPILE_KWARG`.
        """
        if type_ is None:
            if self.casts_float_as_double(typeclause.type, self.dialect):
                return "DOUBLE"
            type_ = typeclause.type.dialect_impl(self.dialect)
        if isinstance(type_, sqltypes.Integer):
            return self._integer_cast_target(type_)
        if isinstance(type_, self._CAST_TARGETS_NAMED_AS_DECLARED):
            return self.dialect.type_compiler.process(type_, **{CAST_TARGET_COMPILE_KWARG: True})
        return super().visit_typeclause(typeclause, type_, **kw)

    _CAST_TARGETS_NAMED_AS_DECLARED: Final[Tuple[Type[sqltypes.TypeEngine], ...]] = (
        sqltypes.Boolean, ARRAY, MAP, STRUCT,
    )
    """The CAST targets MySQL has none for, which StarRocks casts to by the name its own DDL gives the type."""

    _SINGLE_PRECISION_DIGITS: Final[int] = 24
    """The largest ``FLOAT(p)`` precision MySQL stores in 4 bytes; a larger one is DOUBLE."""

    _FLOAT_VISIT_NAMES: Final[Tuple[str, ...]] = ("float", "FLOAT")
    """The generic ``Float``, and the ``FLOAT`` of SQLAlchemy, MySQL and StarRocks; REAL and DOUBLE are apart."""

    @classmethod
    def casts_float_as_double(cls, declared_type: sqltypes.TypeEngine, dialect: "StarRocksDialect") -> bool:
        """Whether a CAST to ``declared_type``, a float type, is sent as DOUBLE.

        StarRocks' FLOAT is 4 bytes, so ``CAST(123456789 AS FLOAT)`` reads 123456790. A float type with a
        precision above 24, such as ``Float(53)``, ``sa.FLOAT(53)`` or ``mysql.FLOAT(53)``, is cast to DOUBLE, as
        MySQL reads ``FLOAT(p)``; so is the generic ``Float`` with no precision, which SQLAlchemy maps to Python's
        8-byte float. ``FLOAT()``, ``FLOAT(24)`` and below, and ``REAL`` are cast to FLOAT, and so is
        ``mysql.FLOAT(M, D)``, which MySQL stores in 4 bytes whatever its M. A TypeDecorator is cast as the type
        it loads for the dialect, and a type with a variant for StarRocks, from ``with_variant()``, as the
        variant. A TypeDecorator that loads ``dialect.type_descriptor(sa.Float())`` loads MySQL's ``FLOAT``,
        which can't be told from a declared ``mysql.FLOAT()``, so it is cast to FLOAT.
        """
        declared_type = cls._declared_type_for(declared_type, dialect)
        if declared_type.__visit_name__ not in cls._FLOAT_VISIT_NAMES:
            return False
        precision = getattr(declared_type, "precision", None)
        if precision is None:
            return declared_type.__visit_name__ == "float"
        return precision > cls._SINGLE_PRECISION_DIGITS and getattr(declared_type, "scale", None) is None

    @staticmethod
    def _declared_type_for(declared_type: sqltypes.TypeEngine, dialect: "StarRocksDialect") -> sqltypes.TypeEngine:
        """Return the type declared for the dialect: through each TypeDecorator, and the variant for StarRocks.

        SQLAlchemy 1.4's ``with_variant()`` wraps the type in a TypeDecorator; 2.0's keeps the variants on the
        type itself.
        """
        while True:
            variant = getattr(declared_type, "_variant_mapping", {}).get(dialect.name)
            if variant is not None:
                declared_type = variant
            elif isinstance(declared_type, sqltypes.TypeDecorator):
                declared_type = declared_type.load_dialect_impl(dialect)
            else:
                return declared_type

    _INTEGER_CAST_TARGETS: Final[Tuple[Tuple[Type[sqltypes.Integer], str, str], ...]] = (
        (mysql_types.TINYINT, "TINYINT", "SMALLINT"),
        (sqltypes.SmallInteger, "SMALLINT", "INT"),
        (mysql_types.MEDIUMINT, "INT", "INT"),
        (sqltypes.INTEGER, "INT", "BIGINT"),
        (sqltypes.BigInteger, "BIGINT", "LARGEINT"),
        (LARGEINT, "LARGEINT", "LARGEINT"),
    )
    """Each integer type with the StarRocks type a cast to it names, signed and unsigned."""

    _GENERIC_INTEGER_CAST_TARGET: Final[str] = "BIGINT"
    """The generic ``Integer`` names no width, so it is cast to the 64 bits ``SIGNED INTEGER`` has on 4.1.

    A value that fits 64 bits, such as an id or an epoch in milliseconds, is therefore not turned into NULL.
    """

    def _integer_cast_target(self, type_: sqltypes.Integer) -> str:
        """Name the StarRocks integer type of the same width, without a display width, UNSIGNED or ZEROFILL.

        StarRocks has no unsigned integers and refuses those keywords, and MEDIUMINT, in a CAST. An unsigned type
        is widened to the next signed type that holds its whole range, and MEDIUMINT is INT.
        """
        unsigned = bool(getattr(type_, "unsigned", False))
        for integer_type, signed_target, unsigned_target in self._INTEGER_CAST_TARGETS:
            if isinstance(type_, integer_type):
                return unsigned_target if unsigned else signed_target
        return self._GENERIC_INTEGER_CAST_TARGET

    def visit_insert_into_files(self, insert_into, **kw):
        return (
            f"INSERT INTO {self.process(insert_into.target, **kw)}\n"
            f" {self.process(insert_into.from_, **kw)}"
        )

    def visit_files_target(self, files, **kw):
        target_items = []
        target_items.append(self.process(files.storage, **kw))
        target_items.append(self.process(files.format, **kw))
        if files.options is not None:
            target_items.append(self.process(files.options, **kw))
        files_str = ",\n".join(target_items)
        return f"FILES(\n{files_str}\n)"

    def _key_value_format(self, items: dict):
        return ',\n'.join([
            f'{repr(k)} = {repr(v)}'
            for k, v in items.items()
        ])

    def visit_cloud_storage(self, storage, **kw):
        return self._key_value_format(storage.options)

    def visit_files_format(self, files_format, **kw):
        return self._key_value_format(files_format.options)

    def visit_files_options(self, files_options, **kw):
        return self._key_value_format(files_options.options)

    def visit_insert_from_files(self, insert_from, **kw):
        target = (
            self.preparer.format_table(insert_from.target)
            if isinstance(insert_from.target, (TableClause,))
            else self.process(insert_from.target, **kw)
        )

        if isinstance(insert_from.columns, str):
            select_str = insert_from.columns
        else:
            select_str = ",".join(
                [
                    self.process(col, **kw)
                    for col in insert_from.columns
                ]
            )

        return (
            f"INSERT INTO {target}\n"
            f" SELECT {select_str}\n"
            f" FROM {self.process(insert_from.from_, **kw)}"
        )

    def visit_files_source(self, files, **kw):
        source_items = []
        source_items.append(self.process(files.storage, **kw))
        source_items.append(self.process(files.format, **kw))
        if files.options is not None:
            source_items.append(self.process(files.options, **kw))
        files_str = ",\n".join(source_items)
        return f"FILES(\n{files_str}\n)"

class StarRocksDDLCompiler(MySQLDDLCompiler):
    def __init__(self, *args, **kwargs):
        self.indent: Final[str] = "    "
        super().__init__(*args, **kwargs)

    def visit_alter_column(self, alter: AlterColumnOp, **kw):
        """
        Compile ALTER COLUMN statements.

        StarRocks does not support altering the aggregation type of a column.
        This method checks for such attempts and raises a NotImplementedError.
        """
        # The `starrocks_agg_type` is added to kwargs only when a change is detected
        # by the comparator. See `compare_starrocks_column_agg_type`.
        if ColumnAggInfoKeyWithPrefix.AGG_TYPE in alter.kwargs:
            raise NotImplementedError(
                f"StarRocks does not support changing the aggregation type of column '{alter.column_name}'."
            )
        return super().visit_alter_column(alter, **kw)

    def visit_create_table(self, create: sa_schema.CreateTable, **kw: Any) -> str:
        table: sa_schema.Table = create.element
        table_kind: str = table.info.get(TableObjectInfoKey.TABLE_KIND, TableKind.TABLE).upper()

        if table_kind == TableKind.VIEW:
            return self._compile_create_view_from_table(table, create, **kw)
        elif table_kind == TableKind.MATERIALIZED_VIEW:
            return self._compile_create_mv_from_table(table, create, **kw)
        else:
            return self._compile_create_table_original(table, create, **kw)

    INDEX_TYPES: Final[Tuple[str, ...]] = ("BITMAP", "GIN", "NGRAMBF", "VECTOR")

    @staticmethod
    def quoted_property(value: Any) -> str:
        """Quote a PROPERTIES key or value as the double-quoted string StarRocks reads back unchanged.

        StarRocks unescapes the string once, as MySQL does, so each backslash is doubled before double quotes are
        escaped; otherwise ``\\d+`` is stored as ``d+`` and a ``"`` ends the string early.
        """
        return '"%s"' % str(value).replace("\\", "\\\\").replace('"', '\\"')

    def quoted_property_pair(self, key: Any, value: Any, separator: str = " = ") -> str:
        """Quote a ``"key" = "value"`` PROPERTIES pair, refusing a missing value rather than writing "None"."""
        if value is None:
            raise exc.CompileError(f"Property {key!r} has no value; StarRocks properties are strings")
        return self.sql_text(f"{self.quoted_property(key)}{separator}{self.quoted_property(value)}")

    _COLUMN_LIST: Final[re.Pattern[str]] = re.compile(
        r"\s*(?:`(?:[^`]|``)+`|\w+)(?:\s*,\s*(?:`(?:[^`]|``)+`|\w+))*\s*"
    )

    @classmethod
    def _partition_clause(cls, partition_by: Any) -> str:
        """Return a PARTITION BY clause, with a bare column or list of columns, such as ``k, s``, in parentheses.

        StarRocks 3.3 refuses ``PARTITION BY k`` and ``PARTITION BY k, s`` and takes ``PARTITION BY (k)`` and
        ``PARTITION BY (k, s)``, which later versions take too. Reflection gives the column or list without its
        parentheses. A list of expressions is left as written: 3.3 refuses it either way, and 3.5 refuses it in
        parentheses.
        """
        text = str(partition_by)
        return f"({text.strip()})" if cls._COLUMN_LIST.fullmatch(text) else text

    def sql_text(self, text: Any) -> str:
        """Return SQL given as text, such as a view definition or a PARTITION BY clause, ready to splice into DDL.

        SQLAlchemy's ``post_process_text``, which its ``DDL`` and ``text()`` constructs use too, doubles each
        ``%``, as in ``str2date(s, '%Y-%m-%d')``, for a driver that formats parameters into the statement with
        ``%``. A reflected clause object gives its text.

        Text that holds ``%%``, as the dialect needed before it escaped ``%`` itself, now reaches StarRocks as
        ``%%``, so it warns once per text; see :func:`warn_once_of_doubled_percents`.
        """
        sql = str(text)
        if "%%" in sql:
            warn_once_of_doubled_percents(sql)
        return self.sql_compiler.post_process_text(sql)

    def visit_create_index(self, create: sa_schema.CreateIndex, **kw: Any) -> str:  # type: ignore[override]
        """Compile ``CREATE INDEX name ON table (columns) [USING <type> [(properties)]] [COMMENT '...']``.

        ``starrocks_using`` names the type, ``starrocks_properties`` its properties (for example NGRAMBF's
        ``gram_num``) and ``starrocks_comment`` the comment.

        StarRocks indexes are BITMAP, GIN, NGRAMBF or VECTOR indexes on one plain column. It has no unique,
        multi-column or expression indexes and no ``IF NOT EXISTS``, so those raise here rather than at the
        server, and the index type is a keyword, never a quoted identifier.
        """
        index = create.element
        self._verify_index_table(index)
        if index.unique:
            raise exc.CompileError(
                f"StarRocks has no unique indexes (index {index.name!r}); use a PRIMARY KEY or UNIQUE KEY table"
            )
        if create.if_not_exists:
            raise exc.CompileError(f"StarRocks has no CREATE INDEX IF NOT EXISTS (index {index.name!r})")
        if len(index.expressions) != 1:
            raise exc.CompileError(f"A StarRocks index covers one column; index {index.name!r} lists several")
        self._refuse_mysql_index_options(index)
        columns = ", ".join(self._indexed_column_name(index, expression) for expression in index.expressions)
        text = "CREATE INDEX %s ON %s (%s)" % (
            self._prepared_index_name(index),
            self.preparer.format_table(index.table),
            columns,
        )
        options = index.dialect_options[DialectName]
        if options["using"] is not None:
            text += " USING " + self._index_type_keyword(index, options["using"])
        if options["properties"]:
            text += " " + self._index_property_list(index, options["using"], options["properties"])
        if options["comment"] is not None:
            text += " COMMENT " + self.sql_compiler.render_literal_value(options["comment"], sqltypes.String())
        return text

    _MYSQL_INDEX_OPTIONS: Final[Dict[str, str]] = {
        "length": "MySQL's key length, as in col(5); a StarRocks index covers the whole column",
        "prefix": "MySQL's index kind before INDEX, such as FULLTEXT; name the type with starrocks_using",
        "with_parser": "MySQL's WITH PARSER; a GIN index takes its parser as the 'parser' of starrocks_properties",
    }

    def _refuse_mysql_index_options(self, index: sa_schema.Index) -> None:
        """Refuse the index options the dialect takes from MySQL's, which StarRocks' CREATE INDEX has no syntax for.

        ``starrocks_length``, ``starrocks_prefix`` and ``starrocks_with_parser`` are MySQL's ``col(n)``,
        ``CREATE FULLTEXT INDEX`` and ``WITH PARSER``, which StarRocks 3.3 to 4.1 refuse as syntax errors, so they
        raise here rather than being left out of the statement. So does ``mysql_using``, whose quoted
        ``USING`` StarRocks refuses: the type goes in ``starrocks_using``.
        """
        if index.dialect_options["mysql"]["using"] is not None:
            raise exc.CompileError(
                f"StarRocks' CREATE INDEX has no mysql_using (index {index.name!r}): name the index type with "
                "starrocks_using"
            )
        options = index.dialect_options[DialectName]
        for option, meaning in self._MYSQL_INDEX_OPTIONS.items():
            if options[option] is not None:
                raise exc.CompileError(
                    f"StarRocks' CREATE INDEX has no starrocks_{option} (index {index.name!r}): it is {meaning}"
                )

    def visit_drop_index(self, drop: sa_schema.DropIndex, **kw: Any) -> str:  # type: ignore[override]
        """Compile ``DROP INDEX name ON table``; StarRocks has no ``IF EXISTS`` for it, so that raises here."""
        if getattr(drop, "if_exists", False):
            raise exc.CompileError(f"StarRocks has no DROP INDEX IF EXISTS (index {drop.element.name!r})")
        return super().visit_drop_index(drop, **kw)

    def _index_property_list(self, index: sa_schema.Index, index_type: Optional[str], properties: Dict[str, Any]) -> str:
        """Render ``("key" = "value", ...)``, which StarRocks accepts only after an index type such as NGRAMBF."""
        if index_type is None:
            raise exc.CompileError(
                f"Index {index.name!r} has starrocks_properties but no starrocks_using; StarRocks takes index "
                "properties only after an index type"
            )
        return "(%s)" % ", ".join(
            self.quoted_property_pair(key, value) for key, value in properties.items()
        )

    def visit_add_constraint(self, create: sa_schema.AddConstraint, **kw: Any) -> str:
        """Refuse ``ADD CONSTRAINT``, which StarRocks does not have, rather than send it to the server."""
        raise exc.CompileError(self._unholdable_constraint_message(create.element, "add"))

    def visit_drop_constraint(self, drop: sa_schema.DropConstraint, **kw: Any) -> str:
        """Refuse ``DROP CONSTRAINT``: a StarRocks table has no constraint to drop."""
        raise exc.CompileError(self._unholdable_constraint_message(drop.element, "drop"))

    @staticmethod
    def _unholdable_constraint_message(constraint: sa_schema.Constraint, action: str) -> str:
        """Explain why StarRocks cannot add or drop a constraint, and what it offers instead."""
        name = f" {constraint.name!r}" if constraint.name else ""
        if isinstance(constraint, sa_schema.PrimaryKeyConstraint):
            return (
                f"StarRocks cannot {action} primary key{name}: a table's key is fixed when the table is created, "
                "with starrocks_primary_key or another starrocks_*_key"
            )
        if isinstance(constraint, sa_schema.UniqueConstraint):
            return (
                f"StarRocks cannot {action} unique constraint{name}: use a PRIMARY KEY or UNIQUE KEY table, "
                "or the unique_constraints table property"
            )
        if isinstance(constraint, sa_schema.ForeignKeyConstraint):
            return (
                f"StarRocks cannot {action} foreign key{name}: the foreign_key_constraints table property "
                "records foreign keys for the optimizer"
            )
        return f"StarRocks cannot {action} constraint{name}: its tables hold no constraints besides their key"

    def _indexed_column_name(self, index: sa_schema.Index, expression: Any) -> str:
        """Return the quoted name of an indexed column, refusing expressions, which StarRocks cannot index."""
        if not isinstance(expression, sa_schema.Column):
            raise exc.CompileError(f"StarRocks indexes plain columns only; index {index.name!r} has {expression}")
        return self.preparer.format_column(expression)

    def _index_type_keyword(self, index: sa_schema.Index, index_type: str) -> str:
        """Return the index type as the upper-case keyword StarRocks expects, refusing unknown types."""
        keyword = index_type.upper()
        if keyword not in self.INDEX_TYPES:
            raise exc.CompileError(
                f"Unknown StarRocks index type {index_type!r} for index {index.name!r}; "
                f"use one of {', '.join(self.INDEX_TYPES)}"
            )
        return keyword

    def _compile_create_table_original(
            self,
            table: sa_schema.Table,
            create: sa_schema.CreateTable,
            **kw: Any) -> str:
        preparer = self.preparer

        text = "\nCREATE "
        if table._prefixes:
            text += " ".join(table._prefixes) + " "

        text += "TABLE "
        if create.if_not_exists:
            text += "IF NOT EXISTS "

        text += preparer.format_table(table) + " "

        # StarRocks-specific validation for all key types
        self._validate_key_definitions(table)

        if create_table_suffix := self.create_table_suffix(table):
            text += create_table_suffix + " "

        text += "(\n"

        column_text_list = list()
        # if only one primary key, specify it along with the column
        primary_keys: List[str] = list()
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(create_column)
                # logger.debug("column desc for column: %r is '%s'", column.name, processed)
                if processed is not None:
                    column_text_list.append(f"{self.indent}{processed}")
                if column.primary_key:
                    primary_keys.append(column.name)
            except exc.CompileError as ce:
                raise exc.CompileError(
                    "(in table %r, column %r): %s"
                    % (self._get_simple_full_table_name(table.name, table.schema), column.name, ce.args[0])
                ) from ce
        text += ", \n".join(column_text_list)

        # N.B. Primary Key is specified in post_create_table
        #  Indexes are created by SQLA after the creation of the table using CREATE INDEX
        # const = self.create_table_constraints(
        #     table,
        #     _include_foreign_key_constraints=create.include_foreign_key_constraints,  # noqa
        # )
        # if const:
        #     text += separator + self.indent + const

        text += "\n)\n%s\n" % self.post_create_table(table, primary_keys=primary_keys, **kw)
        logger.debug("create table text for table: %s, schema: %s, text: %s", table.name, table.schema, text)

        return text

    def _get_simple_full_table_name(self, table_name: str, schema: Optional[str] = None) -> str:
        """Directly get the full table name without adding any quotes."""
        return f"{schema}.{table_name}" if schema else table_name

    def _validate_key_definitions(self, table: sa_schema.Table) -> None:
        """
        Validates key definitions for all StarRocks table types.

        This performs two checks:
        1. (All key types) Ensures that all columns specified in the table's key
           (e.g., `starrocks_primary_key`) are actually defined in the table.
        2. (AGGREGATE KEY only) Enforces StarRocks' strict column ordering rules.

        Args:
            table: The SQLAlchemy Table object to validate.

        Raises:
            CompileError: If any validation rule is violated.
        """
        key_kwarg_map = TableInfoKeyWithPrefix.KEY_KWARG_MAP

        key_str = None
        key_type = None

        for kwarg, name in key_kwarg_map.items():
            if kwarg in table.kwargs:
                key_str = table.kwargs[kwarg]
                key_type = name
                break  # Found the key, no need to check for others

        if not key_str:
            self._warn_of_primary_key_left_to_duplicate_key(table)
            return

        key_column_names = [k.strip() for k in key_str.split(',')]
        table_column_names = {c.name for c in table.columns}

        # 1. Generic Check: Ensure all key columns exist in the table definition.
        missing_keys = set(key_column_names) - table_column_names
        if missing_keys:
            raise exc.CompileError(
                f"Columns specified in {key_type} ('{key_str}') not found in table: {', '.join(missing_keys)}"
            )

        # 2. Specific Check: For AGGREGATE KEY tables, validate column order.
        if key_type == TableType.AGGREGATE_KEY:
            self._validate_aggregate_key_order(table, key_column_names)

    def _warn_of_primary_key_left_to_duplicate_key(self, table: sa_schema.Table) -> None:
        """Warn when a table's implied primary key cannot make it a PRIMARY KEY table.

        With ``starrocks_infer_primary_key`` on, a table that names no ``starrocks_*_key``, and that StarRocks
        can't hold as a PRIMARY KEY table on its primary key columns (see
        :func:`starrocks.common.utils.primary_key_table_obstacle`), is created with no key clause, as earlier
        versions of the dialect did, so StarRocks makes its default DUPLICATE KEY table. With it off, every such
        table is, as in earlier versions, so none gets a warning. A table of another engine, such as an external
        MySQL table, has no key at all, so it gets no warning either.
        """
        key_columns = utils.implied_primary_key_columns(table)
        if not key_columns or not utils.infers_primary_keys(self.dialect) or not utils.table_engine_is_olap(table):
            return
        obstacle = utils.primary_key_table_obstacle(table, key_columns, self.dialect)
        if obstacle is None:
            return
        util.warn(
            f"Table {table.name!r} has the primary key ({', '.join(key_columns)}) and names no starrocks_*_key, but "
            f"it can't be made a PRIMARY KEY table: {obstacle}. It is created with no key clause, as earlier "
            "versions of the dialect did, so StarRocks makes a DUPLICATE KEY table. "
            f"{utils.primary_key_table_remedy(table, key_columns, self.dialect)}"
        )

    def _validate_aggregate_key_order(self, table: sa_schema.Table, key_column_names: List[str]) -> None:
        """
        Validates column order for AGGREGATE KEY tables.

        In StarRocks, for an AGGREGATE KEY table:
        1. All key columns must be defined before any value (aggregate) columns.
        2. The order of key columns in the table definition must match the order
           specified in the `starrocks_aggregate_key` argument.

        Args:
            table: The SQLAlchemy Table object to validate.
            key_column_names: The list of key column names from the kwarg.
        """
        key_cols_from_table = []

        # Separate table columns into key and value lists
        for col in table.columns:
            # Note: value columns are any columns not in the key list.
            if col.name in key_column_names:
                key_cols_from_table.append(col.name)

        # Rule 2: The order of key columns in the table definition must match
        if key_cols_from_table != key_column_names:
            raise exc.CompileError(
                "For AGGREGATE KEY tables, the order of key columns in the table definition "
                f"must match the order in starrocks_aggregate_key. "
                f"Expected order: {key_column_names}, Actual order: {key_cols_from_table}"
            )

        # Rule 1: All key columns must be defined before any value columns.
        last_key_col_index = -1
        col_list = list(table.columns)
        for i, col in enumerate(col_list):
            if col.name in key_column_names:
                last_key_col_index = i

        for i in range(last_key_col_index):
            if col_list[i].name not in key_column_names:
                raise exc.CompileError(
                    "For AGGREGATE KEY tables, all key columns must be defined before any value columns. "
                    f"Value column '{col_list[i].name}' appears before "
                    f"key column '{col_list[last_key_col_index].name}'."
                )

    def post_create_table(self, table: sa_schema.Table, **kw: Any) -> str:
        """
        Appends StarRocks-specific clauses to a CREATE TABLE or CREATE MATERIALIZED VIEW statement.

        This method compiles StarRocks-specific table options provided as `starrocks_`
        kwargs on the `Table` object. It is responsible for constructing clauses
        that appear after the column definitions, such as:
        - `ENGINE`
        - `KEY`: `PRIMARY KEY`, `DUPLICATE KEY`, `AGGREGATE KEY`, `UNIQUE KEY`
        - `COMMENT`
        - `PARTITION BY`
        - `DISTRIBUTED BY`
        - `ORDER BY`
        - `PROPERTIES`
        - `REFRESH`  // for MV
        This method is called after the main CREATE TABLE statement is formed and
        appends clauses like PARTITION BY, DISTRIBUTED BY, PROPERTIES, etc.

        Args:
            table: The `sqlalchemy.schema.Table` object being compiled.
            **kw: Additional keyword arguments from the compiler.
                primary_keys: The list of primary key column names. We need to check it with a table's KEY attribute.

        Returns:
            A string containing all the compiled table-level DDL clauses.
        """
        import warnings
        table_opts: List[str] = []

        opts = utils.extract_dialect_options_as_case_insensitive(table)
        logger.debug("table original opts for table: %s, schema: %s, opts: %r", table.name, table.schema, opts)

        # ENGINE
        engine = opts.get(TableInfoKey.ENGINE)
        if engine and engine.upper() != TableEngine.OLAP:
            table_opts.append(f'ENGINE={engine}')

        # Key / Table Type (Primary key, Duplicate key, Aggregate key, Unique key)
        primary_keys = kw.get("primary_keys", [])
        key_desc = self._get_create_table_key_desc(primary_keys, opts)
        if key_desc is None and primary_keys:
            key_desc = self._inferred_primary_key_desc(table)
        if key_desc:
            table_opts.append(key_desc)

        # Comment
        table_comment = table.comment
        if table_comment is None:
            # For backward compatibility, check for 'starrocks_comment'
            if starrocks_comment := opts.get(TableInfoKey.COMMENT):
                warnings.warn(
                    f"The 'starrocks_comment' dialect argument is deprecated for table '{table.name}'. "
                    "Please use the standard 'comment' argument on the Table object instead.",
                    DeprecationWarning,
                    stacklevel=3,
                )
                table_comment = starrocks_comment

        if table_comment is not None:
            comment = self.sql_compiler.render_literal_value(table_comment, sqltypes.String())
            table_opts.append(f"COMMENT {comment}")

        # Partition
        if partition_by := opts.get(TableInfoKey.PARTITION_BY):
            table_opts.append(f'PARTITION BY {self.sql_text(self._partition_clause(partition_by))}')

        # Distribution
        distributed_by = opts.get(TableInfoKey.DISTRIBUTED_BY)
        if distributed_by is None:
            # For backward compatibility, check for 'distribution'
            if starrocks_distribution := opts.get("DISTRIBUTION"):
                warnings.warn(
                    f"The 'starrocks_distribution' dialect argument is deprecated for table '{table.name}'. "
                    "Please use 'starrocks_distributed_by' instead.",
                    DeprecationWarning,
                    stacklevel=3,
                )
                distributed_by = starrocks_distribution

        if distributed_by:
            table_opts.append(f'DISTRIBUTED BY {self.sql_text(distributed_by)}')

        # Order By
        if order_by := opts.get(TableInfoKey.ORDER_BY):
            order_by = TableAttributeNormalizer.remove_outer_parentheses(self._sort_key_text(order_by))
            table_opts.append(f'ORDER BY ({self.sql_text(order_by)})')

        # Handle MV-specific REFRESH clause
        if refresh := opts.get(TableInfoKey.REFRESH):
            table_opts.append(f"REFRESH {self.sql_text(self._refresh_scheme(refresh))}")

        # Properties
        properties = opts.get(TableInfoKey.PROPERTIES)
        props_dict = {}
        if properties is not None:
            if isinstance(properties, (dict, list, tuple)):
                props_dict = dict(properties)
            else:
                raise exc.CompileError(
                    f"Unsupported type for PROPERTIES: {type(properties)}"
                )
        # if self.dialect.test_replication_num and "replication_num" not in props_dict:
        #     props_dict.setdefault("replication_num", str(self.dialect.test_replication_num))
        if props_dict:
            props = ",\n".join(
                f"{self.indent}{self.quoted_property_pair(k, v, separator='=')}" for k, v in props_dict.items()
            )
            table_opts.append(f"PROPERTIES(\n{props}\n)")

        logger.debug("table opts for table: %s, schema: %s, processed opts: %r", table.name, table.schema, table_opts)

        return "\n".join(table_opts)

    _SORT_ITEM_WRITTEN_OUT: Final[re.Pattern[str]] = re.compile(r"[\s()`]")

    def _sort_key_text(self, order_by: Any) -> str:
        """Return a sort key as ORDER BY text, quoting each bare column name of a list as StarRocks needs.

        A list or tuple names the sort columns, so a name StarRocks reserves, such as ``text``, is backquoted. An
        item holding a space, a parenthesis or a backquote, such as ``last_active DESC`` or a name already in
        backquotes, is left as written, as the string form is.
        """
        if not isinstance(order_by, (list, tuple)):
            return str(order_by)
        return ", ".join(
            str(item) if self._SORT_ITEM_WRITTEN_OUT.search(str(item)) else self.preparer.quote(str(item))
            for item in order_by
        )

    def _get_create_table_key_desc(self, primary_keys: List[str], opts: Dict[str, Any]) -> Optional[str]:
        """Visit create table key description.
        Args:
            primary_keys: The list of primary key column names. We need to check it with a talbe's KEY attribute.
            opts: The table options.

        Returns:
            The table key description. like "PRIMARY KEY (id, name)"
        """
        # Key / Table Type (Primary key, Duplicate key, Aggregate key, Unique key)
        key_type = None
        key_desc = None
        for tbl_kind_key_str, table_type in TableInfoKey.KEY_KWARG_MAP.items():
            kwarg_upper = tbl_kind_key_str.upper()
            if kwarg_upper in opts:
                if key_type:
                    raise exc.CompileError(f"Multiple key types found: {tbl_kind_key_str}, first_key_type: {key_type}")
                key_type = table_type
                key_columns_str: str = TableAttributeNormalizer.remove_outer_parentheses(opts[kwarg_upper])
                logger.debug("get table key info: key_type: %s, key_columns: %s", key_type, key_columns_str)
                # check if the key columns are valid
                if primary_keys:
                    key_columns_set = set(k.strip().strip('`') for k in key_columns_str.split(','))
                    primary_keys_set = set(k.strip().strip('`') for k in primary_keys)
                    logger.debug("check constraint keys. primary_key_set: %s, key_columns_set: [%s]", primary_keys, key_columns_str)
                    if primary_keys_set != key_columns_set:
                        raise exc.CompileError(f"Primary key columns doesn't equal to the table KEY columns. "
                                            f"primary_keys: {primary_keys}, SR's key_columns: ({key_columns_str})")
                key_desc = f"{key_type} ({key_columns_str})"
        return key_desc

    def _inferred_primary_key_desc(self, table: sa_schema.Table) -> Optional[str]:
        """Return the PRIMARY KEY clause of a table that names no key model but implies one, or None.

        See :func:`starrocks.common.utils.inferred_primary_key_columns` for when a primary key becomes the table's
        PRIMARY KEY.
        """
        key_columns = utils.inferred_primary_key_columns(table, self.dialect)
        if key_columns is None:
            return None
        return f"{TableType.PRIMARY_KEY} ({', '.join(self.preparer.quote(name) for name in key_columns)})"


    def _has_column_info_key(self, column: sa_schema.Column, key: str) -> bool:
        """Check if column has a specific info key (case-insensitive)."""
        return any(k.lower() == key.lower() for k in column.info.keys())

    def _get_column_info_value(self, column: sa_schema.Column, key: str, default: Any = None) -> Any:
        """Get column info value by key (case-insensitive)."""
        for k, v in column.info.items():
            if k.lower() == key.lower():
                return v
        return default

    def get_column_specification(self, column: sa_schema.Column, **kw: Any) -> str:
        """Builds column DDL for StarRocks, handling StarRocks-specific features.

        This method extends the base MySQL compiler to support:
        - **KEY specifier**: For AGGREGATE KEY tables, key columns can be marked
          with `Column(..., starrocks_is_agg_key=True)`. The compiler validates that
          a column is not both a key and an aggregate.
        - **Aggregate Functions**: For AGGREGATE KEY tables, value columns can have
          an aggregate function (e.g., 'SUM', 'REPLACE', or a state function such as
          'sum(BIGINT)') given as `Column(..., starrocks_agg_type='...')`.
        - **AUTO_INCREMENT**: Automatically renders `AUTO_INCREMENT` for columns
          with `autoincrement=True`. It also ensures these columns are `BIGINT`
          and `NOT NULL` as required by StarRocks.
        - **Generated Columns**: Compiles `sqlalchemy.Computed` constructs into
          StarRocks' `AS (...)` syntax.

        Args:
            column: The `sqlalchemy.schema.Column` object to process.
            **kw: Additional keyword arguments from the compiler.

        Returns:
            The full DDL string for the column definition.
        """
        # Name, type, others of a column for the output colspec
        _, idx_type = 0, 1

        # Get name and type first
        colspec: List[str] = [
            self.preparer.format_column(column),
            self.dialect.type_compiler.process(
                column.type, type_expression=column
            ),
        ]

        # Get and set column-level aggregate information
        if agg_info := self._get_column_agg_info(column):
            # logger.debug("agg info for column: %r is '%s'", column.name, agg_info)
            colspec.append(agg_info)

        # NULL or NOT NULL.
        if not column.nullable:
            colspec.append("NOT NULL")
        # else: omit explicit NULL (default)

        # AUTO_INCREMENT or default value or computed column
        # AUTO_INCREMENT columns must be NOT NULL
        if (
            column.table is not None
            and (
                # NOTE: we should not use column.table._autoincrement_column here.
                # because the column will be auto-incremented when it's set with `primary_key=True`.
                # column is column.table._autoincrement_column or
                column.autoincrement is True
            )
            and (
                column.server_default is None
                or isinstance(column.server_default, sa_schema.Identity)
            )
            and not (
                self.dialect.supports_sequences
                and isinstance(column.default, sa_schema.Sequence)
                and not column.default.optional
            )
        ):
            colspec[idx_type] = "BIGINT"  # AUTO_INCREMENT column must be BIGINT
            if column.nullable:
                colspec.append("NOT NULL")
            colspec.append("AUTO_INCREMENT")
        else:
            default = self.get_column_default_string(column)
            if default == "AUTO_INCREMENT":
                colspec[1] = "BIGINT"
                if column.nullable:
                    colspec.append("NOT NULL")
                colspec.append("AUTO_INCREMENT")

            elif default is not None:
                colspec.append("DEFAULT " + default)

        # Computed
        if column.computed is not None:
            colspec.append(self.process(column.computed))

        # Comment
        if column.comment is not None:
            literal = self.sql_compiler.render_literal_value(
                column.comment, sqltypes.String()
            )
            colspec.append("COMMENT " + literal)

        column_spec_str = " ".join(colspec)
        logger.debug("column spec for column: %s is [%s]", column.name, column_spec_str)
        return column_spec_str

    def get_column_spec_for_alter_column(self,
            name: str,
            nullable: Optional[bool],
            default: Optional[Any],
            type_: sqltypes.TypeEngine,
            autoincrement: Optional[bool],
            comment: Optional[Any],
            key_or_aggregate: Optional[str] = None,
             **kw: Any) -> str:
        """Builds ALTER COLUMN DDL for StarRocks, handling StarRocks-specific features.

        This method extends the base MySQL compiler to support:
        - **KEY specifier**: For AGGREGATE KEY tables, key columns can be marked
          with `Column(..., starrocks_is_agg_key=True)`. The compiler validates that
          a column is not both a key and an aggregate.
        - **Aggregate Functions**: For AGGREGATE KEY tables, value columns can have
          an aggregate function (e.g., 'SUM', 'REPLACE', or a state function such as
          'sum(BIGINT)') given as `Column(..., starrocks_agg_type='...')`.
        - **AUTO_INCREMENT**: Automatically renders `AUTO_INCREMENT` for columns
          with `autoincrement=True`. It also ensures these columns are `BIGINT`
          and `NOT NULL` as required by StarRocks.
        - **Generated Columns**: Compiles `sqlalchemy.Computed` constructs into
          StarRocks' `AS (...)` syntax.

        Args:
            column: The `sqlalchemy.schema.Column` object to process.
            key_or_aggregate: ``KEY`` for a key column, or a value column's aggregate type, which StarRocks wants
                after the type of a MODIFY COLUMN. The Alembic impl reads it from the database.
            **kw: Additional keyword arguments from the compiler.

        Returns:
            The DDL string for the alter column definition, without name.
        """
        colspec: List[str] = []

        # type if set
        if type_:
            type_: str = self.dialect.type_compiler.process(type_)
            colspec.append(type_)
            if key_or_aggregate:
                colspec.append(key_or_aggregate)

        # NULL or NOT NULL.
        if nullable is not None:
            colspec.append("NULL" if nullable else "NOT NULL")

        if autoincrement:
            raise exc.CompileError(
                f"StarRocks cannot set AUTO_INCREMENT in MODIFY COLUMN (column {name!r}); "
                "op.alter_column leaves an unchanged AUTO_INCREMENT out. From 4.1.3, StarRocks takes a MODIFY "
                "COLUMN of an AUTO_INCREMENT column only when it changes nothing but the comment, and keeps the "
                "AUTO_INCREMENT; earlier versions refuse any MODIFY COLUMN of an AUTO_INCREMENT column"
            )
        # DEFAULT: include if provided (even if 0 or empty string), but not when False
        elif default is not False and default is not None:
            default = format_server_default(self, default)
            if default == "AUTO_INCREMENT":
                colspec.append("AUTO_INCREMENT")
            elif default is not None:
                colspec.append("DEFAULT " + default)

        # Computed is not supported in ALTER COLUMN
        # if computed is not None:
        #     colspec.append(self.process(computed))

        # Comment
        if comment:
            literal = self.sql_compiler.render_literal_value(
                comment, sqltypes.String()
            )
            colspec.append("COMMENT " + literal)

        return " ".join(colspec)

    def _get_table_agg_info(self, table: sa_schema.Table) -> Optional[bool]:
        """
        Get aggregate information for a table from its dialect_option.
        It will use cache to store, in `table.info`.
        Args:
            table: The `sqlalchemy.schema.Table` object to process.

        Returns:
            whether it's AGGREGATE table or not (None if unknown).
        """
        # In CREATE TABLE, table.dialect_options will contain StarRocks options.
        # In ALTER TABLE ADD/MODIFY COLUMN, Alembic/SQLAlchemy often provides a lightweight
        # Table placeholder without dialect options; in that case we treat the table type
        # as unknown and avoid raising on presence of KEY/agg markers so users can specify
        # them explicitly in ADD/MODIFY statements.
        if TableInfoKeyWithPrefix.AGGREGATE_KEY in table.info:
            is_agg_table: Optional[bool] = table.info.get(TableInfoKeyWithPrefix.AGGREGATE_KEY)
            # logger.debug("Cached is_agg_table for table: %s is '%s'", table.name, is_agg_table)
        else:
            # logger.debug("is_agg_table is not stored in cache for table: %s.", table.name)
            try:
                # Remove items with value being None, because the `defaults` has all the keys.
                dialect_options = utils.extract_dialect_options_as_case_insensitive(table)
                logger.debug("Extract dialect options to get aggr info fot table: %s, options=%s", table.name, dialect_options)
                if dialect_options:
                    is_agg_table = TableInfoKey.AGGREGATE_KEY in dialect_options
                    table.info[TableInfoKeyWithPrefix.AGGREGATE_KEY] = is_agg_table
                    # logger.debug("Cache is_agg_table for table: %s to '%s'", table.name, is_agg_table)
                else:
                    # Unknown table options in ALTER context; leave as None
                    is_agg_table = None
            except Exception:
                # No dialect options available; leave as unknown
                is_agg_table = None

        return is_agg_table

    def _get_column_agg_info(self, column: sa_schema.Column) -> Union[str, None]:
        """Get aggregate information for a column.
        Args:
            column: The `sqlalchemy.schema.Column` object to process.

        Returns:
            The aggregate information for the column (`KEY` or `agg_type`, such as `SUM`, or None).
        """
        is_agg_table = self._get_table_agg_info(column.table)

        # check agg key/type in the column
        opt_dict = utils.extract_dialect_options_as_case_insensitive(column)
        has_is_agg_key = ColumnAggInfoKey.IS_AGG_KEY in opt_dict
        has_agg_type = ColumnAggInfoKey.AGG_TYPE in opt_dict

        # If we can determine the table is NOT AGGREGATE KEY, disallow column-level
        # KEY/agg markers. If unknown (ALTER context), allow rendering markers.
        if is_agg_table is False and (has_is_agg_key or has_agg_type):
            raise exc.CompileError(
                "Column-level KEY/aggregate markers are only valid for AGGREGATE KEY tables; "
                "declare starrocks_aggregate_key at table level first."
            )

        # Disallow specifying both KEY and agg_type simultaneously.
        if has_is_agg_key and has_agg_type:
            raise exc.CompileError(
                f"Column '{column.name}' cannot be both KEY and aggregated "
                f"(has {ColumnAggInfoKey.AGG_TYPE})."
            )

        if has_is_agg_key:
            return ColumnAggType.KEY
        elif has_agg_type:
            agg_val = str(opt_dict[ColumnAggInfoKey.AGG_TYPE])
            if not ColumnAggType.is_valid(agg_val):
                raise exc.CompileError(
                    f"Unsupported aggregate type for column '{column.name}': {agg_val}"
                )
            return agg_val
        return None

    def visit_computed_column(self, generated: sa_schema.Computed, **kw: Any) -> str:
        text = "AS (%s)" % self.sql_compiler.process(
            generated.sqltext, include_table=False, literal_binds=True
        )
        return text

    def visit_set_table_comment(self, create: sa_schema.SetTableComment, **kw: Any) -> str:
        return "ALTER TABLE %s COMMENT=%s" % (
            self.preparer.format_table(create.element),
            self.sql_compiler.render_literal_value(
                create.element.comment, sqltypes.String()
            ),
        )

    def visit_drop_table_comment(self, drop: sa_schema.DropTableComment, **kw: Any) -> str:
        return "ALTER TABLE %s COMMENT=''" % (
            self.preparer.format_table(drop.element)
        )

    def visit_alter_view(self, alter: AlterView, **kw: Any) -> str:
        view = alter.element
        text = f"ALTER VIEW {self.preparer.format_table(view)}\n"

        if view.columns:
            text += self._get_view_column_clauses(view)

        text += f"AS\n{self.sql_text(view.definition)}"

        logger.debug("Compiled SQL for AlterView: \n%s", text)
        return text

    def visit_alter_view_security(self, alter: AlterViewSecurity, **kw: Any) -> str:
        """Compile ALTER VIEW ... SET SECURITY, the only way to change a view's security."""
        keyword = self._view_security_keyword(alter.element, alter.security)
        return f"ALTER VIEW {self.preparer.format_table(alter.element)} SET SECURITY {keyword}"

    def _get_view_column_clauses(self, view: View) -> str:
        """
        Helper method to format column clauses for a CREATE VIEW statement.

        StarRocks VIEW columns only support name and comment.

        Args:
            view: View object with columns

        Returns:
            Formatted column clauses string, e.g., " (\n    id,\n    name COMMENT 'User name'\n)"
        """
        column_clauses: List[str] = []
        for col in view.columns:
            col_name = self.preparer.quote(col.name)
            if col.comment:
                comment = self.sql_compiler.render_literal_value(
                    col.comment, sqltypes.String()
                )
                column_clauses.append(f'{self.indent}{col_name} COMMENT {comment}')
            else:
                column_clauses.append(f'{self.indent}{col_name}')
        return " (\n%s\n)" % ",\n".join(column_clauses)

    def _compile_create_view_from_table(self, table: Table, create: CreateView, **kw: Any) -> str:
        """
        Helper to compile CREATE VIEW from a CreateView DDL element or CreateTable for View.

        Args:
            create: CreateView DDL element or CreateTable element where table_kind='VIEW'
                the table may be not a View object, so we treat it as a normal Table.
            **kw: Additional compilation kwargs

        Returns:
            Compiled SQL string for CREATE VIEW statement
        """
        preparer = self.preparer

        view_name = preparer.format_table(table)
        definition = table.info.get(TableObjectInfoKey.DEFINITION)
        if not definition:
            raise exc.CompileError("View definition is required")
        security = utils.get_dialect_option(table, TableInfoKey.SECURITY)

        or_replace_clause = "OR REPLACE " if getattr(create, "or_replace", False) else ""
        if_not_exists_clause = "IF NOT EXISTS " if create.if_not_exists else ""

        text = f"CREATE {or_replace_clause}VIEW {if_not_exists_clause}{view_name}"

        # Add column definitions if present (only name and comment are supported)
        if table.columns:
            text += self._get_view_column_clauses(table)

        if table.comment:
            text += "\nCOMMENT " + self.sql_compiler.render_literal_value(table.comment, sqltypes.String())
        if security:
            text += "\nSECURITY " + self._view_security_keyword(table, security)
        text += f"\nAS\n{self.sql_text(definition)}"

        logger.debug("Compiled SQL for CreateView: \n%s", text)
        return text

    @staticmethod
    def _warn_of_synchronous_mv(mv: Table) -> None:
        """Log a warning that StarRocks will build a MaterializedView as a synchronous materialized view.

        Without a REFRESH or DISTRIBUTED BY clause, StarRocks creates a synchronous materialized view, a rollup of
        its base table that ``information_schema`` does not list, so reflection and autogenerate can't see it, and
        autogenerate proposes creating it again. Earlier versions of the dialect created one the same way, and a
        migration may mean to, so it goes ahead; ``starrocks_refresh`` asks for an asynchronous one instead. The
        warning goes to the dialect's logger, like its other warnings about what StarRocks will do.
        """
        opts = utils.extract_dialect_options_as_case_insensitive(mv)
        if not opts.get(TableInfoKey.REFRESH) and not opts.get(TableInfoKey.DISTRIBUTED_BY):
            logger.warning(
                "Materialized view %r has neither starrocks_refresh nor starrocks_distributed_by, so StarRocks "
                "builds it as a synchronous materialized view, which reflection and autogenerate can't see; give it "
                "starrocks_refresh, for example 'ASYNC' or 'MANUAL', for an asynchronous one",
                mv.name,
            )

    _SCHEDULE_REFRESH = re.compile(r"^(\s*(?:(?:IMMEDIATE|DEFERRED)\s+)?)SCHEDULE\b", re.IGNORECASE)

    @classmethod
    def _refresh_scheme(cls, refresh: Any) -> str:
        """Return a materialized view's refresh scheme with ``SCHEDULE`` spelled ``ASYNC``.

        ``REFRESH SCHEDULE [START(...)] EVERY(...)`` is the 4.1.1 synonym of ``REFRESH ASYNC [START(...)]
        EVERY(...)``, which every version knows; reflection reads either back as ASYNC.
        """
        return cls._SCHEDULE_REFRESH.sub(r"\1ASYNC", str(refresh), count=1)

    def _view_security_keyword(self, view: Table, security: str) -> str:
        """Return the view's SECURITY keyword, refusing anything but the NONE and INVOKER that StarRocks has.

        StarRocks takes a view's SECURITY from 3.4.1; 3.4.0 and older reject the clause as a syntax error.
        """
        require_server_version(self.dialect, (3, 4, 1), f"View SECURITY (view {view.name!r})")
        keyword = str(security).upper()
        if keyword not in ViewSecurityType.ALLOWED_ITEMS - {ViewSecurityType.EMPTY}:
            raise exc.CompileError(
                f"StarRocks views have SECURITY NONE or INVOKER, not {security!r} (view {view.name!r})"
            )
        return keyword

    def visit_create_view(self, create: CreateView, **kw: Any) -> str:
        """
        CREATE VIEW is handled by `visit_create_table` dispatcher
        based on `table.info['table_type']`.
        But, this is still needed for alembic's `CreateViewOp`.
        """
        return self._compile_create_view_from_table(create.element, create, **kw)

    def visit_drop_table(self, drop: sa_schema.DropTable, **kw: Any) -> str:
        """Drop a View or MaterializedView held in a MetaData with its own DROP statement.

        ``MetaData.drop_all()`` issues a ``DropTable`` for every member, and StarRocks rejects ``DROP TABLE`` for a
        view or a materialized view.
        """
        table_kind = drop.element.info.get(TableObjectInfoKey.TABLE_KIND, TableKind.TABLE).upper()
        if table_kind == TableKind.VIEW:
            return self.visit_drop_view(drop, **kw)
        if table_kind == TableKind.MATERIALIZED_VIEW:
            return self.visit_drop_materialized_view(drop, **kw)
        return super().visit_drop_table(drop, **kw)

    def visit_drop_view(self, drop: DropView, **kw: Any) -> str:
        view = drop.element
        text = "DROP VIEW "
        if drop.if_exists:
            text += "IF EXISTS "
        text += self.preparer.format_table(view)
        return text

    def visit_alter_materialized_view(self, alter: AlterMaterializedView, **kw: Any) -> str:
        """
        Compile ALTER MATERIALIZED VIEW statement.
        Each attribute will be generated as a separate statement.

        Only supports altering mutable attributes:
        - refresh: ALTER MATERIALIZED VIEW ... REFRESH <new_scheme>
        - properties: ALTER MATERIALIZED VIEW ... SET ("<key>" = "<value>")
        """
        preparer = self.preparer

        # Format MV name with schema
        if alter.schema:
            mv_name = f"{preparer.quote(alter.schema)}.{preparer.quote(alter.mv_name)}"
        else:
            mv_name = preparer.quote(alter.mv_name)

        statements = []

        # ALTER REFRESH
        if alter.refresh is not None:
            statements.append(
                f"ALTER MATERIALIZED VIEW {mv_name} REFRESH {self.sql_text(self._refresh_scheme(alter.refresh))}"
            )

        # ALTER PROPERTIES
        if alter.properties is not None:
            props_str = ", ".join(
                self.quoted_property_pair(k, v) for k, v in alter.properties.items()
            )
            statements.append(f"ALTER MATERIALIZED VIEW {mv_name} SET ({props_str})")

        if not statements:
            raise exc.CompileError("ALTER MATERIALIZED VIEW requires at least one mutable attribute (refresh or properties)")

        # Return statements joined by semicolon
        return ";\n".join(statements)

    def _compile_create_mv_from_table(self, table: Table, create: CreateMaterializedView, **kw: Any) -> str:
        """
        Helper to compile CREATE MATERIALIZED VIEW from a CreateMaterializedView DDL element or CreateTable for MV.

        Args:
            create: CreateMaterializedView DDL element or CreateTable element where table_kind='MATERIALIZED_VIEW'
            **kw: Additional compilation kwargs

        Returns:
            Compiled SQL string for CREATE MATERIALIZED VIEW statement
        """
        preparer = self.preparer

        mv_name = preparer.format_table(table)
        definition = table.info.get(TableObjectInfoKey.DEFINITION)
        if not definition:
            raise ValueError(f"Materialized view '{table.name}' requires a definition.")
        self._warn_of_synchronous_mv(table)

        if getattr(create, "or_replace", False):
            raise exc.CompileError(
                f"StarRocks has no CREATE OR REPLACE MATERIALIZED VIEW (materialized view {table.name!r}); "
                "drop it and create it again"
            )
        if_not_exists_clause = "IF NOT EXISTS " if getattr(create, 'if_not_exists', False) else ""

        text = f"CREATE MATERIALIZED VIEW {if_not_exists_clause}{mv_name}"

        # Add column definitions if present (only name and comment are supported, same as View)
        if table.columns:
            text += self._get_view_column_clauses(table)

        clauses = []

        # Handle common clauses (COMMENT, PARTITION BY, etc.)
        common_clauses: str = self.post_create_table(table, **kw)
        clauses.append(common_clauses)

        if clauses:
            text += "\n" + "\n".join(clauses)

        text += f"\nAS\n{self.sql_text(definition)}"

        logger.debug("Compiled SQL for CreateMaterializedView: \n%s", text)
        return text

    def visit_create_materialized_view(self, create: CreateMaterializedView, **kw: Any) -> str:
        """
        The dispatch for creating a materialized view is from `CreateTable` DDL element
        with `table_kind='MATERIALIZED_VIEW'`.
        But, this is still needed for alembic's `CreateMaterializedViewOp`.
        """
        return self._compile_create_mv_from_table(create.element, create, **kw)

    def visit_drop_materialized_view(self, drop: DropMaterializedView, **kw: Any) -> str:
        mv = drop.element
        text = "DROP MATERIALIZED VIEW "
        if drop.if_exists:
            text += "IF EXISTS "
        text += self.preparer.format_table(mv)
        return text

    # Visit methods ordered according to StarRocks grammar:
    # engine → key → partition → distribution → order by → properties

    def visit_alter_table_engine(self, alter: AlterTableEngine, **kw: Any) -> str:
        """Compile ALTER TABLE ENGINE DDL for StarRocks.
        Not supported in StarRocks.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        return f"ALTER TABLE {table_name} ENGINE = {alter.engine}"

    def visit_alter_table_key(self, alter: AlterTableKey, **kw: Any) -> str:
        """Compile ALTER TABLE KEY DDL for StarRocks.
        Not supported in StarRocks yet.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        return f"ALTER TABLE {table_name} {alter.key_type} KEY ({alter.key_columns})"

    def visit_alter_table_partition(self, alter: AlterTablePartition, **kw: Any) -> str:
        """Compile ALTER TABLE PARTITION BY DDL for StarRocks.
        Not supported in StarRocks yet.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        return f"ALTER TABLE {table_name} PARTITION BY {self.sql_text(alter.partition_by)}"

    def visit_alter_table_columns(self, alter: AlterTableColumns, **kw: Any) -> str:
        """Compile a combined ALTER TABLE ADD/DROP COLUMN DDL for StarRocks.

        Renders every column change as a clause of a single ALTER TABLE
        statement so StarRocks submits them as one schema-change job:

            ALTER TABLE t ADD COLUMN a INT, DROP COLUMN c, ADD COLUMN b INT
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)

        clauses: List[str] = []
        for column in alter.adds:
            clauses.append(f"ADD COLUMN {self.get_column_specification(column, **kw)}")
        for column_name in alter.drops:
            clauses.append(f"DROP COLUMN {self.preparer.quote(column_name)}")

        if not clauses:
            raise exc.CompileError(
                f"ALTER TABLE {table_name} has no column changes to apply."
            )

        # notice users about such a time consuming operation
        from_db_clause = f"FROM {alter.schema} " if alter.schema else ""
        show_clause = f"SHOW ALTER TABLE COLUMN {from_db_clause}WHERE TableName='{alter.table_name}'"
        logger.info(f"You probably should use ({show_clause}) to check the execution status "
                    f"of the column schema change before doing another ALTER TABLE statement.")

        return f"ALTER TABLE {table_name} " + ", ".join(clauses)

    def visit_alter_table_distribution(self, alter: AlterTableDistribution, **kw: Any) -> str:
        """Compile ALTER TABLE DISTRIBUTED BY DDL for StarRocks."""
        # TODO:
        table_name = format_table_name(self, alter.table_name, alter.schema)
        distribution_clause = f"DISTRIBUTED BY {self.sql_text(alter.distribution_method)}"
        if alter.buckets is not None:
            distribution_clause += f" BUCKETS {alter.buckets}"

        # notice users about such a time consuming operation
        from_db_clause = f"FROM {alter.schema} " if alter.schema else ""
        show_clause = f"SHOW ALTER TABLE OPTIMIZE {from_db_clause}WHERE TableName='{alter.table_name}'"
        logger.info(f"You probably should use ({show_clause}) to check the execution status "
                    f"of altering DISTRIBUTION before doing another ALTER TABLE statement.")

        return f"ALTER TABLE {table_name} {distribution_clause}"

    def visit_alter_table_order(self, alter: AlterTableOrder, **kw: Any) -> str:
        """Compile ALTER TABLE ORDER BY DDL for StarRocks; a list of columns is quoted as :meth:`_sort_key_text` says."""

        table_name = format_table_name(self, alter.table_name, alter.schema)
        order_by = self._sort_key_text(alter.order_by)

        from_db_clause = f"FROM {alter.schema} " if alter.schema else ""
        show_clause = f"SHOW ALTER TABLE COLUMN {from_db_clause}WHERE TableName='{alter.table_name}'"
        logger.info(f"You probably should use ({show_clause}) to check the execution status "
                    f"of altering ORDER before doing another ALTER TABLE statement.")

        return f"ALTER TABLE {table_name} ORDER BY ({self.sql_text(order_by)})"

    def visit_alter_table_properties(self, alter: AlterTableProperties, **kw: Any) -> str:
        """Compile ALTER TABLE SET (...) DDL for StarRocks.

        Note:
            SR supports one property in an ALTER TABLE SET statement, except for the ones sharing the
            ``dynamic_partition.`` or ``binlog_`` prefix. So, we will generate a SET statement for each
            group of properties from `group_properties_for_set`.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)

        multi_set_statement = "; ".join(
            f"ALTER TABLE {table_name} SET ("
            + ", ".join(self.quoted_property_pair(k, v) for k, v in group.items())
            + ")"
            for group in group_properties_for_set(alter.properties)
        )
        logger.debug("Compiled SQL for AlterTableProperties: \n%s", multi_set_statement)
        return multi_set_statement


class StarRocksIdentifierPreparer(MySQLIdentifierPreparer):
    """Quote an identifier only where StarRocks needs it, a reserved word of StarRocks or of MySQL included.

    StarRocks reserves words MySQL does not, such as ``files``, ``text`` and ``json``, and refuses them unquoted,
    so a table or column of that name needs backquotes. MySQL's words are the MySQL preparer's own, which every
    SQLAlchemy release has; ``sqlalchemy.dialects.mysql.reserved_words`` exists only from 1.4.27.
    """

    reserved_words = MySQLIdentifierPreparer.reserved_words | STARROCKS_RESERVED_WORDS

    def __init__(self, dialect, **kwargs):
        kwargs.pop("server_ansiquotes", None)
        super().__init__(dialect, server_ansiquotes=False, **kwargs)

    # We don't force to use quote for identifier. we can uncomment this if needed.
    # def _requires_quotes(self, ident):
    #     return True


@log.class_logger
class StarRocksDialect(MySQLDialect_pymysql):
    # Dialect name
    name: Final[str] = "starrocks"

    # Supported/Permitted StarRocks's dialect construct arguments for Table and Column
    # Supports both lower and upper case variants for the arguments (easier for users' usages).
    construct_arguments = [
        (Table, {variant: None for k in TableInfoKey.ALL for variant in (k.lower(), k.upper())}),
        (Column, {variant: None for k in ColumnAggInfoKey.ALL for variant in (k.lower(), k.upper())}),
        (Insert, {"overwrite": False, "label": None}),
        (sa_schema.PrimaryKeyConstraint, {"using": None}),
        (
            sa_schema.Index,
            {
                "using": None,
                "properties": None,
                "comment": None,
                "length": None,
                "prefix": None,
                "with_parser": None,
            },
        ),
    ]

    # Caching
    # Warnings are generated by SQLAlchemy if this flag is not explicitly set
    # and tests are needed before being enabled
    supports_statement_cache = True
    supports_native_enum = False
    _support_float_cast = True
    supports_empty_insert = False

    ischema_names = ischema_names
    colspecs = colspecs

    _PRELOADED_STATES_KEY = "starrocks_preloaded_table_states"
    _MULTI_REFLECT_CHUNK_SIZE = 500
    inspector = StarRocksInspector

    statement_compiler = StarRocksSQLCompiler
    ddl_compiler = StarRocksDDLCompiler
    type_compiler = StarRocksTypeCompiler
    preparer = StarRocksIdentifierPreparer

    # Used to get the partition info from the SHOW CREATE TABLE statement
    # Use regex to find the PARTITION BY clause. It can be multi-line.
    # _PARTITION_BY_PATTERN = re.compile(r"PARTITION BY\s+(.*?)(?:\s*DISTRIBUTED BY|\s*ORDER BY|\s*PROPERTIES|AS|;|$)", re.DOTALL | re.IGNORECASE)

    def __init__(
        self,
        starrocks_schema_change_timeout: Optional[Union[float, str]] = 300.0,
        starrocks_wait_for_schema_change: Union[bool, str] = True,
        starrocks_infer_primary_key: Union[bool, str] = False,
        **kwargs: Any,
    ) -> None:
        """Take the dialect's arguments to ``create_engine``, besides MySQL's.

        Args:
            starrocks_schema_change_timeout: The longest wait, in seconds, for a running schema change before a
                statement that changes its table, and for the job of a CREATE INDEX or DROP INDEX after it: see
                :meth:`wait_while_schema_change_runs`. ``None`` or ``0`` waits without a limit. The default, 300,
                is StarRocks' default ``query_timeout``.
            starrocks_wait_for_schema_change: ``False`` sends each statement at once, without waiting, as earlier
                versions of the dialect did; StarRocks then refuses a change to a table whose schema change is
                still running.
            starrocks_infer_primary_key: ``True`` makes a table whose ``primary_key=True`` columns name no
                ``starrocks_*_key`` a PRIMARY KEY table on them, where StarRocks can hold one: see
                :func:`starrocks.common.utils.inferred_primary_key_columns`. ``False``, the default, sends no key
                clause for such a table, as earlier versions of the dialect did, so StarRocks makes a DUPLICATE KEY
                table. Alembic's offline mode takes it in ``context.configure(dialect_opts=...)``.

        Each accepts the text an ``engine_from_config`` file gives, and the URL's query string can give any of them:
        see :meth:`_take_dialect_options_from`.
        """
        super(StarRocksDialect, self).__init__(**kwargs)
        self._run_a_deprecated_engine_level_as_autocommit()
        self.run_mode: Optional[str] = None
        self.schema_change_timeout = self._schema_change_timeout_seconds(starrocks_schema_change_timeout)
        self.wait_for_schema_change = util.asbool(starrocks_wait_for_schema_change)
        self.infer_primary_key = util.asbool(starrocks_infer_primary_key)

    _ENGINE_ISOLATION_LEVEL_ATTRIBUTES: Final[Tuple[str, ...]] = ("_on_connect_isolation_level", "isolation_level")
    """Where SQLAlchemy keeps the ``isolation_level`` given to ``create_engine``: 2.0, then 1.4's MySQL dialect."""

    def _run_a_deprecated_engine_level_as_autocommit(self) -> None:
        """Replace a deprecated ``isolation_level`` given to ``create_engine`` with ``AUTOCOMMIT``, warning once.

        The level runs as ``AUTOCOMMIT``, see :meth:`set_isolation_level`, and connections report it so. When the
        pool takes back a connection whose level was changed, as by Alembic's ``autocommit_block()``, SQLAlchemy
        2.0 asserts that the engine's level is the one connections report, unless it is ``AUTOCOMMIT``. With the
        deprecated level kept, the assertion failed, and the connection never went back to the pool.
        """
        replaced = False
        for attribute in self._ENGINE_ISOLATION_LEVEL_ATTRIBUTES:
            level = getattr(self, attribute, None)
            normalized_level = level.replace("_", " ").upper() if isinstance(level, str) else None
            if normalized_level in self._DEPRECATED_ISOLATION_LEVELS:
                if not replaced:
                    self._warn_of_a_deprecated_isolation_level(normalized_level)
                setattr(self, attribute, "AUTOCOMMIT")
                replaced = True

    @staticmethod
    def _schema_change_timeout_seconds(value: Optional[Union[float, str]]) -> Optional[float]:
        """Return the schema-change timeout in seconds, or None for no limit, which ``None``, ``0`` and ``"None"`` give.

        Text that is not a number, ``nan`` and a negative number raise ArgumentError; ``inf`` waits without limit.
        """
        if value is None or (isinstance(value, str) and value.strip().lower() in {"", "none"}):
            return None
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            seconds = math.nan
        if not seconds >= 0:
            raise exc.ArgumentError(f"starrocks_schema_change_timeout must be 0 or more seconds, or None; got {value!r}")
        return seconds or None

    def create_connect_args(
        self, url: URL, _translate_args: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[Any], Dict[str, Any]]:
        """Build the driver's connect arguments, with the driver's autocommit on unless they set it.

        A URL's ``autocommit`` query value reaches the driver as text, as it always has, and the driver takes any
        text as on. Whatever the arguments say, :meth:`on_connect` turns autocommit on for each new connection.
        The dialect's own options in the query string are taken out first: see :meth:`_take_dialect_options_from`.
        """
        url = self._take_dialect_options_from(url)
        _, connect_args = super(StarRocksDialect, self).create_connect_args(url, _translate_args)
        connect_args.setdefault("autocommit", True)
        logger.debug("connect_args: %s", connect_args)
        return [], connect_args

    _URL_QUERY_OPTIONS: Final[Tuple[str, ...]] = (
        "starrocks_schema_change_timeout",
        "starrocks_wait_for_schema_change",
        "starrocks_infer_primary_key",
    )

    def _take_dialect_options_from(self, url: URL) -> URL:
        """Set the dialect's options that the URL's query string gives, and return the URL without them.

        ``alembic.ini``'s ``sqlalchemy.url`` is a natural place for them, and the driver, which gets the rest of the
        query string, refuses keyword arguments it doesn't know. A value in the URL takes the place of the
        ``create_engine`` keyword's, and a name given twice takes its last value. Alembic's offline mode makes the
        dialect without reading the query string, so it takes them in ``context.configure(dialect_opts=...)``.
        """
        values = {name: url.query[name] for name in self._URL_QUERY_OPTIONS if name in url.query}
        values = {name: value[-1] if isinstance(value, tuple) else value for name, value in values.items()}
        if "starrocks_schema_change_timeout" in values:
            self.schema_change_timeout = self._schema_change_timeout_seconds(values["starrocks_schema_change_timeout"])
        if "starrocks_wait_for_schema_change" in values:
            self.wait_for_schema_change = util.asbool(values["starrocks_wait_for_schema_change"])
        if "starrocks_infer_primary_key" in values:
            self.infer_primary_key = util.asbool(values["starrocks_infer_primary_key"])
        return url.difference_update_query(self._URL_QUERY_OPTIONS) if values else url

    def on_connect(self) -> Callable[[Any], None]:
        """Return the hook SQLAlchemy runs on each new DBAPI connection: the driver's autocommit on, then MySQL's.

        StarRocks commits every statement on its own outside an explicit transaction, so connections start in
        ``AUTOCOMMIT``, the isolation level that describes that; see :meth:`set_isolation_level`. The dialect
        reads the driver's autocommit mode as the connection's level, and PyMySQL and asyncmy start with it off,
        so a connection that ``creator``, ``async_creator`` or ``connect_args={"autocommit": False}`` made would
        otherwise run as ``READ COMMITTED``. SQLAlchemy sets the engine's ``isolation_level`` once autocommit is
        on, so ``isolation_level="READ COMMITTED"`` is what turns StarRocks transactions on.
        """
        set_up_as_mysql = super().on_connect()

        def start_in_autocommit(dbapi_connection: Any) -> None:
            dbapi_connection.autocommit(True)
            if set_up_as_mysql is not None:
                set_up_as_mysql(dbapi_connection)

        return start_in_autocommit

    def initialize(self, connection: Connection) -> None:
        """Read the server's version and run mode, and refuse a ``READ COMMITTED`` first connection StarRocks can't serve.

        SQLAlchemy sets the first connection's isolation level before the server version is known, so
        :meth:`set_isolation_level` can't refuse it there before 3.5; see :meth:`_require_sql_transactions`.
        """
        super().initialize(connection)
        self._init_run_mode(connection)
        if not self._driver_autocommits(connection.connection):
            self._require_sql_transactions(connection.connection)

    def _init_run_mode(self, connection: Connection) -> None:
        if self.run_mode is None:
            self.run_mode = self._get_run_mode(connection)
            logger.debug("system run mode: %s" % self.run_mode)

    _SCHEMA_CHANGE_JOB_KINDS: Final[Tuple[str, ...]] = ("COLUMN", "OPTIMIZE", "ROLLUP")
    """The SHOW ALTER TABLE kinds whose running job blocks other changes to its table. ROLLUP lists the jobs of
    ``ALTER TABLE ... ADD ROLLUP`` and of synchronous materialized views."""
    _SCHEMA_CHANGE_ENDED_STATES: Final[frozenset[str]] = frozenset({"FINISHED", "CANCELLED"})
    _SCHEMA_CHANGE_POLL_SECONDS: Final[float] = 1.0
    _SCHEMA_CHANGE_POLLS_BEFORE_WARNING: Final[int] = 5
    _SCHEMA_CHANGE_POLLS_BETWEEN_WARNINGS: Final[int] = 60
    _ROLLUP_TABLE_STATE_REFUSAL: Final[str] = "A materialized view or rollup index is being created on the table"
    """How StarRocks refuses a change to a table it holds in its ROLLUP state."""
    _ROLLUP_FINISH_GAP_FIRST_PAUSE_SECONDS: Final[float] = 0.05

    def do_execute(
        self,
        cursor: "DBAPICursor",
        statement: str,
        parameters: Any,
        context: Optional[ExecutionContext] = None,
    ) -> None:
        """Execute a statement once the connection's transaction and the table it alters are ready for it.

        See :meth:`_match_transaction_to` for the transaction. StarRocks builds an index or a rollup, changes most
        columns and redistributes a table in an asynchronous job, and rejects every other change to that table until
        the job ends: "A schema change operation is in progress" for a COLUMN or OPTIMIZE job, and "A materialized
        view or rollup index is being created on the table" for a ROLLUP job. ``create_all`` creates a table's
        indexes one after another, and a migration alters a table one statement at a time, so each ALTER TABLE,
        CREATE INDEX and DROP INDEX compiled from a construct that names its table first waits for the job before
        it, and is resent while StarRocks still holds the table for a ROLLUP job that has ended: see
        :meth:`_table_altered_by` and :meth:`_send_through_the_rollup_finish_gap`. A CREATE INDEX or DROP INDEX then
        waits for its own job: see :meth:`_wait_for_index_job`. Text doesn't wait.
        """
        waited_since = time.monotonic()
        jobs_before = self._prepare_connection_for(cursor, statement, context)
        self._send_through_the_rollup_finish_gap(
            cursor, jobs_before, waited_since, partial(super().do_execute, cursor, statement, parameters, context)
        )
        self._read_every_ddl_result(cursor, context)
        self._wait_for_index_job(statement, context, jobs_before)
        self._track_transaction_control(statement, context)

    def do_executemany(
        self,
        cursor: "DBAPICursor",
        statement: str,
        parameters: Any,
        context: Optional[ExecutionContext] = None,
    ) -> None:
        """Execute a statement for many parameter sets, prepared as :meth:`do_execute` prepares one.

        The driver folds the parameter sets of an INSERT into multi-row INSERTs only when its VALUES row is bare
        placeholders, and otherwise sends one INSERT per row. Each is a load transaction and a tablet version of its
        own in StarRocks: a few hundred rows take a minute, a failure leaves the rows before it stored, and past
        about a thousand rows StarRocks refuses the next with "too many versions". The VALUES row of a JSON, ARRAY,
        MAP or STRUCT column holds its bind expression, so the dialect sends those rows itself, as the driver
        would bare placeholders; see :meth:`_values_pages`. Rows of bare placeholders go to the driver as before,
        unless there are more than one INSERT takes, which the driver would send in one statement all the same.
        ``INSERT OVERWRITE`` and ``INSERT ... WITH LABEL`` are sent the same way, whatever their VALUES row holds,
        and only in one statement; see :meth:`_refuse_rows_apart`.
        """
        keeps_rows_together = self._keeps_rows_together(statement, context)
        pages = None
        if (
            keeps_rows_together
            or self._pages_rows_of(context)
            or self._driver_passes_the_row_limit(statement, parameters)
        ):
            pages = self._values_pages(cursor, statement, parameters)
        if keeps_rows_together:
            self._refuse_rows_apart(pages)
        self._prepare_connection_for(cursor, statement, context)
        if pages is None:
            super().do_executemany(cursor, statement, parameters, context)
        else:
            self._execute_pages(cursor, pages)
        self._track_transaction_control(statement, context)

    @staticmethod
    def _pages_rows_of(context: Optional[ExecutionContext]) -> bool:
        """Whether the INSERT being run is one whose rows the dialect sends; the compiler decided, once per statement."""
        return context is not None and context.isinsert and getattr(context.compiled, "_rows_paged_by_dialect", False)

    _VALUES_ROWS_PER_STATEMENT: Final[int] = 10000
    """The most rows StarRocks takes in one INSERT ... VALUES: its ``expr_children_limit``, 10,000 by default."""

    @classmethod
    def _driver_passes_the_row_limit(cls, statement: str, parameters: Any) -> bool:
        """Whether the driver would put more rows in one INSERT than StarRocks takes.

        The driver folds rows of bare placeholders into statements of up to ``max_stmt_length`` bytes, whatever
        their number, and StarRocks refuses an INSERT of more than 10,000 rows. Only such an executemany of more
        rows than that is looked at, so a smaller one goes to the driver untouched.
        """
        if len(parameters) <= cls._VALUES_ROWS_PER_STATEMENT:
            return False
        split = cls._split_off_values_row(statement)
        return split is not None and bool(cls._BARE_PLACEHOLDER_ROW.fullmatch(split[1]))

    _DRIVER_MAX_STMT_LENGTH: Final[int] = 1024000
    """The longest statement, in bytes, PyMySQL and asyncmy build from an executemany, when a cursor has no limit."""

    _BARE_PLACEHOLDER_ROW: Final[re.Pattern[str]] = re.compile(
        r"\(\s*(?:%s|%\([^()%]*\)s)(?:\s*,\s*(?:%s|%\([^()%]*\)s))*\s*\)"
    )

    @classmethod
    def _driver_sends_row_by_row(cls, statement: str) -> bool:
        """Whether the driver would send an executemany of this INSERT once per row.

        The driver folds the rows into one statement only when the one VALUES row is bare placeholders. No row of
        a multi-row INSERT can read the rows before it, since StarRocks takes no subquery in VALUES.
        """
        split = cls._split_off_values_row(statement)
        return split is not None and not cls._BARE_PLACEHOLDER_ROW.fullmatch(split[1])

    @classmethod
    def _split_off_values_row(cls, statement: str) -> Optional[Tuple[str, str]]:
        """Split an INSERT into the text up to its one VALUES row, and the row with its parentheses.

        Return None unless the statement ends with exactly one parenthesised row after a VALUES keyword outside
        parentheses, with nothing after it but whitespace and a semicolon.
        """
        depth = 0
        after_values = False
        row_start: Optional[int] = None
        for token in cls._STATEMENT_TOKEN.finditer(statement):
            text = token.group(0)
            if text.startswith(("/*", "--", "#")):
                continue
            if row_start is None and depth == 0 and after_values and text == "(":
                row_start = token.start()
            depth += (text == "(") - (text == ")")
            if row_start is not None and depth == 0:
                rest = statement[token.end():]
                return (statement[:row_start], statement[row_start:token.end()]) if not rest.strip("; \t\r\n") else None
            after_values = depth == 0 and text.upper() == "VALUES"
        return None

    def _driver_cursor(self, cursor: "DBAPICursor") -> Any:
        """Return the driver's own cursor behind ``cursor``, which is that cursor for PyMySQL."""
        return cursor

    def _execute_pages(self, cursor: "DBAPICursor", pages: List[str]) -> None:
        """Run each multi-row INSERT of an executemany, and count the rows of all of them as its rowcount."""
        stored = 0
        for page in pages:
            cursor.execute(page)
            stored += cursor.rowcount
        self._driver_cursor(cursor).rowcount = stored

    def _values_pages(self, cursor: "DBAPICursor", statement: str, parameters: Any) -> Optional[List[str]]:
        """Render the rows of an executemany of ``statement`` into the multi-row INSERTs that carry them.

        Each row is rendered by the driver's own escaping, ``cursor.mogrify``, and a page holds as many rows as
        StarRocks takes in one INSERT and the driver puts in one statement, ``max_stmt_length`` bytes. A page is
        one load transaction: it is stored whole or not at all. Return None, for the driver to run the
        executemany as before, when ``statement`` has no one VALUES row to repeat, its text before the row holds
        a placeholder, or the driver's cursor can't render a row.
        """
        split = self._split_off_values_row(statement)
        driver_cursor = self._driver_cursor(cursor)
        render_row = getattr(driver_cursor, "mogrify", None)
        if split is None or render_row is None:
            return None
        head, row = split
        try:
            head = head % ()
        except (TypeError, ValueError, KeyError):
            return None
        max_length = getattr(driver_cursor, "max_stmt_length", self._DRIVER_MAX_STMT_LENGTH)
        pages: List[str] = []
        page: List[str] = []
        page_length = 0
        for parameter_set in parameters:
            values = render_row(row, parameter_set)
            length = self._utf8_length(values)
            if page and (len(page) == self._VALUES_ROWS_PER_STATEMENT or page_length + 1 + length > max_length):
                pages.append(head + ",".join(page))
                page = []
            page_length = (page_length + 1 if page else self._utf8_length(head)) + length
            page.append(values)
        if page:
            pages.append(head + ",".join(page))
        return pages

    @staticmethod
    def _utf8_length(text: str) -> int:
        """Return the bytes ``text`` takes in UTF-8, the encoding of every StarRocks connection; ASCII is quick."""
        return len(text) if text.isascii() else len(text.encode("utf-8", "surrogateescape"))

    def do_execute_no_params(
        self, cursor: "DBAPICursor", statement: str, context: Optional[ExecutionContext] = None
    ) -> None:
        """Execute a statement without parameters, readied and waited for as :meth:`do_execute` does."""
        waited_since = time.monotonic()
        jobs_before = self._prepare_connection_for(cursor, statement, context)
        self._send_through_the_rollup_finish_gap(
            cursor, jobs_before, waited_since, partial(super().do_execute_no_params, cursor, statement, context)
        )
        self._read_every_ddl_result(cursor, context)
        self._wait_for_index_job(statement, context, jobs_before)
        self._track_transaction_control(statement, context)

    def _send_through_the_rollup_finish_gap(
        self,
        cursor: "DBAPICursor",
        jobs_before: Dict[str, Optional[SchemaChangeJob]],
        waited_since: float,
        send: Callable[[], None],
    ) -> None:
        """Send a statement, and resend it while StarRocks still holds its table for a ROLLUP job seen ended.

        StarRocks ends a ROLLUP job, which builds an ``ADD ROLLUP`` or a synchronous materialized view, in an order
        of its own. ``RollupJobV2.runRunningJob`` sets the job FINISHED, which SHOW ALTER TABLE ROLLUP shows at
        once, and writes it to the edit log. Only after that does ``MaterializedViewHandler.runAlterJobV2`` call
        ``onJobDone``, which sets the table's state back to NORMAL; a CANCELLED job ends the same way, and a comment
        there names the gap. Until then, for up to about 60 ms, StarRocks refuses each ALTER TABLE, CREATE INDEX and
        DROP INDEX on the table with "A materialized view or rollup index is being created on the table", from its
        table-state check, before the statement has changed anything. COLUMN and OPTIMIZE jobs set the table's state
        back before they show FINISHED, so they leave no gap.

        So that refusal alone is resent, and only when the wait before the statement saw a ROLLUP job on its table:
        after 50 ms, then after pauses that double up to the poll interval, until ``starrocks_schema_change_timeout``
        has passed since the wait began. ``jobs_before`` is what the wait saw, empty for a statement it didn't
        cover; any other error raises as it is.
        """
        rollup = jobs_before.get("ROLLUP")
        deadline = None if self.schema_change_timeout is None else waited_since + self.schema_change_timeout
        pause = self._ROLLUP_FINISH_GAP_FIRST_PAUSE_SECONDS
        while True:
            try:
                send()
                return
            except self.dbapi.Error as error:
                in_the_gap = rollup is not None and self._ROLLUP_TABLE_STATE_REFUSAL in str(error)
                if not in_the_gap or (deadline is not None and time.monotonic() + pause > deadline):
                    raise
            logger.info(
                "StarRocks still holds the table for its ROLLUP job %s, which has ended; resending the statement "
                "in %d ms.",
                rollup.job_id, pause * 1000,
            )
            self._pause_between_polls(cursor, pause)
            pause = min(pause * 2, self._SCHEMA_CHANGE_POLL_SECONDS)

    @staticmethod
    def _read_every_ddl_result(cursor: "DBAPICursor", context: Optional[ExecutionContext]) -> None:
        """Read the results of every statement a DDL construct compiled to, so a later statement's error raises.

        ALTER MATERIALIZED VIEW with a refresh and properties, and ALTER TABLE SET with properties StarRocks takes
        one at a time, compile to several statements. The driver reads a later statement's result only when the
        cursor closes, and logs its error there instead of raising it.
        """
        if context is not None and context.isddl:
            while cursor.nextset():
                pass

    def _wait_for_index_job(
        self, statement: str, context: Optional[ExecutionContext], jobs_before: Dict[str, Optional[SchemaChangeJob]]
    ) -> None:
        """Wait for the job a CREATE INDEX or DROP INDEX construct started, so the index shows as changed on return.

        StarRocks builds and drops a secondary index in a schema-change job, and SHOW INDEX shows the change only
        once the job has ended, so an inspector or an autogenerate run straight after ``create_all`` missed the
        last index. The wait is :meth:`wait_while_schema_change_runs`, on a cursor of its own so the statement's
        result stays as it is. When it times out the statement has run and its job goes on, so a warning is
        given instead of the error. ``jobs_before`` are the jobs the wait before the statement saw on the table;
        see :meth:`_log_a_cancelled_index_job`.
        """
        if context is None or not context.isddl:
            return
        ddl = context.compiled.statement
        if not isinstance(ddl, (sa_schema.CreateIndex, sa_schema.DropIndex)):
            return
        altered = self._table_altered_by(statement, context)
        if altered is None:
            return
        cursor = context.root_connection.connection.cursor()
        try:
            jobs_after = self.wait_while_schema_change_runs(cursor, *altered)
        except SchemaChangeTimeoutError as timeout:
            util.warn(f"{timeout} The statement has run, and StarRocks goes on with the index's job: {statement}")
            return
        finally:
            cursor.close()
        table_name, schema = altered
        self._log_a_cancelled_index_job(ddl, ".".join(filter(None, (schema, table_name))), jobs_before, jobs_after)

    @staticmethod
    def _log_a_cancelled_index_job(
        ddl: Union[sa_schema.CreateIndex, sa_schema.DropIndex],
        table: str,
        jobs_before: Dict[str, Optional[SchemaChangeJob]],
        jobs_after: Dict[str, Optional[SchemaChangeJob]],
    ) -> None:
        """Log a warning when StarRocks cancelled the job of a CREATE INDEX or DROP INDEX, which then did nothing.

        The table's newest COLUMN job is the statement's own only when it differs from the newest one before the
        statement, so an older cancelled job is not taken for it.
        """
        job = jobs_after.get("COLUMN")
        if job is None or job.state != "CANCELLED":
            return
        earlier = jobs_before.get("COLUMN")
        if earlier is not None and earlier.job_id == job.job_id:
            return
        outcome = "was not built" if isinstance(ddl, sa_schema.CreateIndex) else "was not dropped"
        logger.warning(
            "StarRocks cancelled the COLUMN job %s of index %r on table %r, so the index %s: %s",
            job.job_id, ddl.element.name, table, outcome, job.message or "no reason given",
        )

    def _prepare_connection_for(
        self, cursor: "DBAPICursor", statement: str, context: Optional[ExecutionContext]
    ) -> Dict[str, Optional[SchemaChangeJob]]:
        """Ready the transaction and the table for the statement.

        Return the jobs the wait for a running schema change saw on the table, as
        :meth:`wait_while_schema_change_runs` does.
        """
        self._match_transaction_to(cursor, statement, context)
        return self._wait_for_schema_change_blocking(cursor, statement, context)

    def _keeps_rows_together(self, statement: str, context: Optional[ExecutionContext]) -> bool:
        """Whether an executemany of ``statement`` is an ``INSERT OVERWRITE`` or ``INSERT ... WITH LABEL``.

        An ``insert()`` says so in its dialect options, and text in its INSERT keywords.
        """
        if context is not None and context.isinsert:
            options = context.compiled.statement.dialect_options[self.name]
            return bool(options["overwrite"]) or options["label"] is not None
        keyword = self._statement_keyword_token(statement)
        return keyword is not None and bool(self._INSERT_KEEPING_ROWS_TOGETHER.match(statement, keyword.start()))

    def _refuse_rows_apart(self, pages: Optional[List[str]]) -> None:
        """Refuse an ``INSERT OVERWRITE`` or ``INSERT ... WITH LABEL`` whose rows can't all go in one statement.

        Each further overwrite would replace the rows the statement before it wrote, and each further statement
        would reuse a label whose load has already committed.
        """
        needs_one = (
            "INSERT OVERWRITE and INSERT ... WITH LABEL (starrocks_overwrite and starrocks_label) need all the rows "
            "in one statement"
        )
        if pages is None:
            raise exc.InvalidRequestError(
                f"{needs_one}, but the driver would send this executemany once per row: it needs one VALUES row of "
                "values to repeat, and no placeholder before it; pass the rows to insert().values([...]) instead"
            )
        if len(pages) > 1:
            raise exc.InvalidRequestError(
                f"{needs_one}, but these rows need {len(pages)}: one INSERT takes at most "
                f"{self._VALUES_ROWS_PER_STATEMENT:,} rows, and the driver's max_stmt_length bytes; send fewer rows "
                "at a time"
            )

    _ISOLATION_LEVELS: Final[Tuple[str, ...]] = ("AUTOCOMMIT", "READ COMMITTED")
    _DEPRECATED_ISOLATION_LEVELS: Final[Tuple[str, ...]] = ("READ UNCOMMITTED", "REPEATABLE READ", "SERIALIZABLE")
    _SQL_TRANSACTIONS_VARIABLE: Final[str] = "enable_sql_transaction"
    _TRANSACTIONS_SINCE: Final[Tuple[int, ...]] = (3, 5)
    _READ_COMMITTED_CONNECTION: Final[str] = "A READ COMMITTED connection (StarRocks transactions)"
    _TRANSACTION_OPEN: Final[str] = "starrocks_transaction_open"
    _DML_KEYWORDS: Final[frozenset[str]] = frozenset({"INSERT", "UPDATE", "DELETE"})
    _DDL_KEYWORDS: Final[frozenset[str]] = frozenset(
        {"CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "GRANT", "REVOKE", "REFRESH"}
    )
    _KEYWORDS_RUN_IN_TRANSACTIONS: Final[frozenset[str]] = frozenset({
        "SELECT", "WITH", "(", "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "TRACE", "SET",
        "INSERT", "UPDATE", "DELETE", "MERGE", "BEGIN", "START", "COMMIT", "ROLLBACK",
    })
    """The first keywords of the statements StarRocks runs inside a transaction: queries, SHOW, SET, DML and
    transaction control. DESC, DESCRIBE, EXPLAIN and TRACE begin a query, a DML statement or a SHOW."""
    _LEADING_COMMENTS: Final[re.Pattern[str]] = re.compile(r"(?:\s+|/\*.*?\*/|(?:--|#)[^\n]*(?:\n|$))*", re.DOTALL)
    _STATEMENT_TOKEN: Final[re.Pattern[str]] = re.compile(
        r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|`(?:[^`]|``)*`|/\*.*?\*/|(?:--|#)[^\n]*|\w+|[()]|[^\w\s()]",
        re.DOTALL,
    )
    _TRANSACTION_START: Final[re.Pattern[str]] = re.compile(r"(?:BEGIN|START\s+TRANSACTION)\b", re.IGNORECASE)
    _TRANSACTION_END: Final[re.Pattern[str]] = re.compile(
        r"(?:COMMIT|ROLLBACK)\b(?!\s+(?:WORK\s+)?TO\b)", re.IGNORECASE
    )
    _GAP: Final[str] = r"(?:\s|/\*.*?\*/|(?:--|#)[^\n]*(?:\n|$))"
    _IDENTIFIER: Final[str] = r"(`(?:[^`]|``)+`|\w+)"
    _QUALIFIED_NAME: Final[str] = (
        rf"{_IDENTIFIER}(?:{_GAP}*\.{_GAP}*{_IDENTIFIER})?(?:{_GAP}*\.{_GAP}*{_IDENTIFIER})?"
    )
    _DELETE_TARGET: Final[re.Pattern[str]] = re.compile(
        rf"DELETE\b{_GAP}*FROM\b{_GAP}*{_QUALIFIED_NAME}", re.IGNORECASE | re.DOTALL
    )
    _INSERT_INTO_FILES: Final[re.Pattern[str]] = re.compile(
        rf"INSERT\b{_GAP}*INTO\b{_GAP}*FILES{_GAP}*\(", re.IGNORECASE | re.DOTALL
    )
    _INSERT_OVERWRITE: Final[re.Pattern[str]] = re.compile(rf"INSERT\b{_GAP}*OVERWRITE\b", re.IGNORECASE | re.DOTALL)
    _INSERT_KEEPING_ROWS_TOGETHER: Final[re.Pattern[str]] = re.compile(
        rf"INSERT\b{_GAP}*(?:OVERWRITE\b|INTO\b{_GAP}*{_QUALIFIED_NAME}{_GAP}*"
        rf"(?:(?:TEMPORARY{_GAP}+)?PARTITION{_GAP}*\([^)]*\){_GAP}*)?WITH{_GAP}+LABEL\b)",
        re.IGNORECASE | re.DOTALL,
    )
    _KEYED_MODELS_WITHOUT_PRIMARY_KEY: Final[frozenset[str]] = frozenset({
        TableModel.DUP_KEYS, TableModel.DUP_KEYS2, TableModel.AGG_KEYS, TableModel.AGG_KEYS2,
        TableModel.UNQ_KEYS, TableModel.UNQ_KEYS2,
    })

    def get_isolation_level_values(self, dbapi_conn: Any) -> Tuple[str, ...]:
        """Return ``AUTOCOMMIT``, how StarRocks runs statements by default, ``READ COMMITTED``, its transactions,
        and the other MySQL levels, which are deprecated and run as ``AUTOCOMMIT``.
        """
        return self._ISOLATION_LEVELS + self._DEPRECATED_ISOLATION_LEVELS

    def get_isolation_level(self, dbapi_connection: Any) -> str:
        """Return the level the driver's autocommit mode stands for; StarRocks reports REPEATABLE-READ regardless."""
        return "AUTOCOMMIT" if self._driver_autocommits(dbapi_connection) else "READ COMMITTED"

    def set_isolation_level(self, dbapi_connection: Any, level: str) -> None:
        """Turn the driver's autocommit on for ``AUTOCOMMIT`` and off for ``READ COMMITTED``.

        StarRocks ignores ``SET AUTOCOMMIT`` and the session isolation level, and keeps committing each statement
        on its own until a ``BEGIN WORK``. Off, the driver's autocommit tells :meth:`_match_transaction_to` to open
        one for the connection's first INSERT, UPDATE or DELETE. ``READ COMMITTED`` is refused where StarRocks
        would not run the transaction, so a migration fails before its first statement rather than part way
        through: see :meth:`_require_sql_transactions`.

        ``READ UNCOMMITTED``, ``REPEATABLE READ`` and ``SERIALIZABLE`` run as ``AUTOCOMMIT``, with a FutureWarning.
        Earlier versions of the dialect accepted them, and StarRocks committed each statement under them as it
        does under ``AUTOCOMMIT``. Given to ``create_engine``, such a level is replaced when the engine is made:
        see :meth:`_run_a_deprecated_engine_level_as_autocommit`.
        """
        level = level.replace("_", " ").upper()
        levels = self.get_isolation_level_values(dbapi_connection)
        if level not in levels:
            raise exc.ArgumentError(
                f"Invalid value {level!r} for isolation_level. "
                f"Valid isolation levels for {self.name!r} are {', '.join(levels)}"
            )
        if level in self._DEPRECATED_ISOLATION_LEVELS:
            self._warn_of_a_deprecated_isolation_level(level)
        elif level == "READ COMMITTED":
            self._require_sql_transactions(dbapi_connection)
        self._dbapi_connection_of(dbapi_connection).autocommit(level != "READ COMMITTED")

    def set_engine_execution_options(self, engine: "Engine", opts: Mapping[str, Any]) -> None:
        """Replace a deprecated ``isolation_level`` among an engine's execution options with ``AUTOCOMMIT``, warning
        once.

        ``create_engine(execution_options=...)`` and ``Engine.execution_options()`` give their options here, once,
        and SQLAlchemy then sets the level on each Connection the engine makes, so a deprecated one warned at each.
        """
        level = opts.get("isolation_level")
        normalized_level = level.replace("_", " ").upper() if isinstance(level, str) else None
        if normalized_level in self._DEPRECATED_ISOLATION_LEVELS:
            self._warn_of_a_deprecated_isolation_level(normalized_level)
            opts = {**opts, "isolation_level": "AUTOCOMMIT"}
        super().set_engine_execution_options(engine, opts)

    @classmethod
    def _warn_of_a_deprecated_isolation_level(cls, level: str) -> None:
        """Warn with a FutureWarning, which Python shows by default, unlike a DeprecationWarning, at the line that
        gave the level: see :meth:`_stacklevel_of_the_callers_line`.
        """
        warnings.warn(
            f"The isolation level {level!r} is deprecated for StarRocks, and runs as AUTOCOMMIT, as it did before; "
            "use 'AUTOCOMMIT', or 'READ COMMITTED' for StarRocks transactions",
            FutureWarning,
            stacklevel=cls._stacklevel_of_the_callers_line(),
        )

    _LIBRARY_MODULE: Final[re.Pattern[str]] = re.compile(r"(?:starrocks|sqlalchemy|alembic)(?:\.|$)")

    @classmethod
    def _stacklevel_of_the_callers_line(cls) -> int:
        """Return the ``stacklevel`` at which the method calling this one warns of the first line outside the
        dialect, SQLAlchemy and Alembic.

        A level reaches the dialect through a different depth of SQLAlchemy's frames for each way of giving it, and
        Python shows a FutureWarning once for each line it names, so a fixed ``stacklevel`` named a line of the
        dialect or of SQLAlchemy, and hid every call after the first.
        """
        frame = sys._getframe(1)
        stacklevel = 1
        while frame.f_back is not None and cls._LIBRARY_MODULE.match(frame.f_globals.get("__name__", "")):
            frame = frame.f_back
            stacklevel += 1
        return stacklevel

    @staticmethod
    def _dbapi_connection_of(connection: Any) -> Any:
        """Return the DBAPI connection behind a pooled connection, which SQLAlchemy 1.4 passes as well."""
        return getattr(connection, "dbapi_connection", connection)

    def _driver_connection_of(self, connection: Any) -> Any:
        """Return the driver's own connection behind a pooled or a DBAPI connection.

        SQLAlchemy has ``Dialect.get_driver_connection`` only from 1.4.24. Before that, a dialect without it is
        pymysql's, whose DBAPI connection is the driver's own; the asyncmy dialect defines the method itself.
        """
        dbapi_connection = self._dbapi_connection_of(connection)
        get_driver_connection = getattr(self, "get_driver_connection", None)
        return dbapi_connection if get_driver_connection is None else get_driver_connection(dbapi_connection)

    def _driver_autocommits(self, connection: Any) -> bool:
        """Return whether the driver connection is in autocommit mode; None means the server's default, which is on."""
        mode = self._driver_connection_of(connection).autocommit_mode
        return mode is None or bool(mode)

    def do_commit(self, dbapi_connection: Any) -> None:
        """Commit, and forget the StarRocks transaction the dialect saw opened on the connection."""
        self._forget_transaction(dbapi_connection)
        super().do_commit(dbapi_connection)

    def do_rollback(self, dbapi_connection: Any) -> None:
        """Roll back, and forget the StarRocks transaction the dialect saw opened on the connection.

        The rollback goes to StarRocks even when the dialect saw no transaction open, so the pool's rollback on
        checkin also ends one opened through a raw DBAPI cursor.
        """
        self._forget_transaction(dbapi_connection)
        super().do_rollback(dbapi_connection)

    def _forget_transaction(self, connection: Any) -> None:
        info = self._pool_info(connection)
        if info is not None:
            info.pop(self._TRANSACTION_OPEN, None)

    @staticmethod
    def _pool_info(connection: Any) -> Optional[Dict[str, Any]]:
        """Return a pooled connection's ``info`` dictionary, which lives as long as its DBAPI connection.

        A DBAPI connection has none, and the connection SQLAlchemy initializes the dialect with raises
        NotImplementedError for it; both return None.
        """
        try:
            info = getattr(connection, "info", None)
        except NotImplementedError:
            return None
        return info if isinstance(info, dict) else None

    @classmethod
    def _statement_keyword(cls, statement: str) -> str:
        """Return the statement's keyword, upper case: see :meth:`_statement_keyword_token`."""
        token = cls._statement_keyword_token(statement)
        return token.group(0).upper() if token else ""

    @classmethod
    def _statement_keyword_token(cls, statement: str) -> Optional[re.Match[str]]:
        """Return the token of the statement's first keyword, past leading comments.

        For ``WITH``, it is the keyword of the statement the common table expressions belong to: the first
        SELECT, INSERT, UPDATE or DELETE outside parentheses.
        """
        tokens = cls._STATEMENT_TOKEN.finditer(statement, cls._LEADING_COMMENTS.match(statement).end())
        first = next(tokens, None)
        if first is None or first.group(0).upper() != "WITH":
            return first
        depth = 0
        for token in tokens:
            text = token.group(0)
            if text == "(":
                depth += 1
            elif text == ")":
                depth -= 1
            elif depth == 0 and text.upper() in cls._DML_KEYWORDS | {"SELECT"}:
                return token
        return first

    def _match_transaction_to(
        self, cursor: "DBAPICursor", statement: str, context: Optional[ExecutionContext]
    ) -> None:
        """Open or commit the connection's StarRocks transaction as ``statement`` needs.

        - On a ``READ COMMITTED`` connection the first INSERT, UPDATE or DELETE opens a transaction with BEGIN WORK,
          which the SQLAlchemy commit or rollback ends. Reads open none.
        - StarRocks refuses DDL inside a transaction, so DDL first commits the open one, as MySQL does.
        - StarRocks runs only queries, SHOW, DESCRIBE, SET, DML and transaction control inside a transaction, so
          with one open any other statement, such as ANALYZE, USE or KILL, is refused before it is sent, and the
          transaction stays open.
        - A BEGIN or START TRANSACTION sent as text is refused where StarRocks accepts it and ignores it: see
          :meth:`_require_sql_transactions`.
        - Text holding several statements is refused on a ``READ COMMITTED`` connection or with a transaction
          open: see :meth:`_refuse_several_statements_in_transaction`.

        A statement compiled from a construct is classified by the construct, and text by its first keyword. A
        Connection that has lost its level is warned of first: see :meth:`_warn_of_a_lost_isolation_level`.
        """
        if context is None:
            return
        info = self._pool_info(context.root_connection.connection)
        if info is None:
            return
        self._warn_of_a_lost_isolation_level(context, info)
        self._refuse_several_statements_in_transaction(statement, context, info)
        keyword = self._statement_keyword(statement)
        if context.isinsert or context.isupdate or context.isdelete or keyword in self._DML_KEYWORDS:
            self._open_transaction_for_dml(cursor, statement, keyword, context, info)
        elif info.get(self._TRANSACTION_OPEN) and (context.isddl or keyword in self._DDL_KEYWORDS):
            cursor.execute("COMMIT")
            del info[self._TRANSACTION_OPEN]
        elif info.get(self._TRANSACTION_OPEN) and keyword and keyword not in self._KEYWORDS_RUN_IN_TRANSACTIONS:
            raise exc.InvalidRequestError(
                "StarRocks runs only queries, SHOW, DESCRIBE, EXPLAIN, SET, INSERT, UPDATE, DELETE, COMMIT and "
                f"ROLLBACK inside a transaction, so this {keyword} statement can't run in the open one; commit "
                "first, or run it on an AUTOCOMMIT connection"
            )
        elif self._TRANSACTION_START.match(statement, self._LEADING_COMMENTS.match(statement).end()):
            self._require_sql_transactions(context.root_connection.connection, "A transaction (BEGIN)")

    _LOST_ISOLATION_LEVEL_WARNED: Final[str] = "starrocks_lost_isolation_level_warned"

    def _warn_of_a_lost_isolation_level(self, context: ExecutionContext, info: Dict[str, Any]) -> None:
        """Warn, once for each DBAPI connection, when it doesn't run at the level its Connection was given.

        SQLAlchemy sets a level given to ``Connection.execution_options()``, ``Engine.execution_options()`` or
        ``create_engine(execution_options=...)`` on the DBAPI connection the Connection has at the time. After a
        disconnect or ``invalidate()``, the same Connection goes on with a new DBAPI connection, at the engine's
        level, since SQLAlchemy doesn't set the Connection's on it, and tells the dialect of neither. MySQL's
        default level still runs transactions; StarRocks' default, ``AUTOCOMMIT``, commits each statement at once.
        So a ``READ COMMITTED`` Connection would go on without transactions, and its rollbacks undo nothing, and an
        ``AUTOCOMMIT`` Connection on a ``READ COMMITTED`` engine would hold its DML in a transaction, which a rollback
        or a close without a commit undoes. A level given to ``create_engine(isolation_level=...)`` is set on every
        new DBAPI connection.
        """
        requested = context.execution_options.get("isolation_level")
        if not isinstance(requested, str) or info.get(self._LOST_ISOLATION_LEVEL_WARNED):
            return
        transactional = requested.replace("_", " ").upper() == "READ COMMITTED"
        if transactional != self._driver_autocommits(context.root_connection.connection):
            return
        info[self._LOST_ISOLATION_LEVEL_WARNED] = True
        running = "AUTOCOMMIT, so StarRocks commits each statement at once and a rollback undoes nothing"
        if not transactional:
            running = "READ COMMITTED, so StarRocks holds its INSERT, UPDATE and DELETE until a commit, or undoes them"
        util.warn(
            f"This Connection's isolation_level is {requested!r}, but its DBAPI connection runs as {running}. "
            "SQLAlchemy doesn't set a level given to execution_options() on the DBAPI connection that takes the "
            "place of one after a disconnect or invalidate(). Roll back, then set the level again, or give it to "
            "create_engine(isolation_level=...), which sets it on every new DBAPI connection."
        )

    def _refuse_several_statements_in_transaction(
        self, statement: str, context: ExecutionContext, info: Dict[str, Any]
    ) -> None:
        """Refuse text holding several statements on a ``READ COMMITTED`` connection or with a transaction open.

        StarRocks runs each statement of such text, but the dialect can open, commit or refuse a transaction only
        for the first, and the driver reads the later statements' results only when the cursor closes, where it
        logs their errors instead of raising them. So ``SELECT 1; INSERT ...`` would commit its insert at once,
        and a refused second statement would be dropped without an error. A DDL construct that compiles to several
        statements commits the open transaction first, as any DDL does. The text is scanned for a second statement
        only where that matters, so a long INSERT on an ``AUTOCOMMIT`` connection is not read token by token.
        """
        if context.isddl:
            return
        if not info.get(self._TRANSACTION_OPEN) and self._driver_autocommits(context.root_connection.connection):
            return
        if self._holds_several_statements(statement):
            raise exc.InvalidRequestError(
                "Text holding several statements can't run on a READ COMMITTED connection or inside a transaction: "
                "the dialect would open the transaction for the first statement only, and the driver drops the "
                "errors of the others; execute each statement on its own"
            )

    @classmethod
    def _holds_several_statements(cls, statement: str) -> bool:
        """Return whether a statement is followed by another, past a ``;`` outside quotes and comments."""
        ended = False
        for token in cls._STATEMENT_TOKEN.finditer(statement):
            text = token.group(0)
            if text.startswith(("/*", "--", "#")):
                continue
            if text == ";":
                ended = True
            elif ended:
                return True
        return False

    def _track_transaction_control(self, statement: str, context: Optional[ExecutionContext]) -> None:
        """Note a BEGIN, START TRANSACTION, COMMIT or ROLLBACK sent as text, once StarRocks has accepted it.

        So the SQLAlchemy commit and rollback, and DDL, see a transaction opened by hand on an ``AUTOCOMMIT``
        connection. ``ROLLBACK TO`` a savepoint ends nothing.
        """
        info = self._pool_info(context.root_connection.connection) if context is not None else None
        if info is None:
            return
        position = self._LEADING_COMMENTS.match(statement).end()
        if self._TRANSACTION_START.match(statement, position):
            info[self._TRANSACTION_OPEN] = True
        elif self._TRANSACTION_END.match(statement, position):
            info.pop(self._TRANSACTION_OPEN, None)

    def _require_transactions(self, feature: str) -> None:
        """Refuse a transaction before StarRocks 3.5, which accepts BEGIN WORK and commits each statement anyway."""
        require_server_version(self, self._TRANSACTIONS_SINCE, feature, error=exc.InvalidRequestError)

    def _require_sql_transactions(self, dbapi_connection: Any, feature: str = _READ_COMMITTED_CONNECTION) -> None:
        """Refuse ``READ COMMITTED``, or another ``feature``, on a connection where StarRocks would commit each
        statement anyway.

        That is before StarRocks 3.5, and from 4.0.7 when the session variable ``enable_sql_transaction`` is off:
        StarRocks then answers BEGIN WORK, COMMIT and ROLLBACK with OK and does nothing.
        """
        self._require_transactions(feature)
        if not self._sql_transactions_enabled(dbapi_connection):
            raise exc.InvalidRequestError(
                f"{feature} needs the session variable {self._SQL_TRANSACTIONS_VARIABLE} on; it is off, so "
                "StarRocks would ignore BEGIN WORK, COMMIT and ROLLBACK and commit each statement at once"
            )

    def _sql_transactions_enabled(self, dbapi_connection: Any) -> bool:
        """Return whether the session's ``enable_sql_transaction`` is on, or True on a StarRocks without it.

        The variable exists from 4.0.7 and 4.1.0. SHOW VARIABLES lists nothing for it before 4.0.7, where reading
        ``@@enable_sql_transaction`` fails.
        """
        cursor = self._dbapi_connection_of(dbapi_connection).cursor()
        try:
            cursor.execute(f"SHOW VARIABLES LIKE '{self._SQL_TRANSACTIONS_VARIABLE}'")
            values = {name: value for name, value in cursor.fetchall()}
        finally:
            cursor.close()
        return str(values.get(self._SQL_TRANSACTIONS_VARIABLE, "true")).lower() not in {"false", "0", "off"}

    def _open_transaction_for_dml(
        self,
        cursor: "DBAPICursor",
        statement: str,
        keyword: str,
        context: ExecutionContext,
        info: Dict[str, Any],
    ) -> None:
        """Open a transaction for a DML statement on a ``READ COMMITTED`` connection, unless one is open already.

        A ``delete()`` without a WHERE clause compiles to TRUNCATE TABLE, which StarRocks runs outside any
        transaction, so it is refused. StarRocks can't run ``INSERT INTO FILES`` or ``INSERT OVERWRITE`` inside a
        transaction, so they open none and run on their own, and with a transaction open they are refused.
        """
        transaction_open = bool(info.get(self._TRANSACTION_OPEN))
        if not transaction_open and self._driver_autocommits(context.root_connection.connection):
            return
        if keyword == "TRUNCATE":
            raise exc.InvalidRequestError(
                "A delete() without a WHERE clause compiles to TRUNCATE TABLE, which StarRocks runs outside the "
                "transaction; give it a WHERE clause, or run it on an AUTOCOMMIT connection"
            )
        insert_outside_transactions = self._insert_run_outside_transactions(statement)
        if insert_outside_transactions is not None and transaction_open:
            raise exc.InvalidRequestError(
                f"StarRocks can't run {insert_outside_transactions} inside a transaction; run it before the "
                "transaction's first INSERT, UPDATE or DELETE, or on an AUTOCOMMIT connection"
            )
        self._refuse_delete_escaping_transaction(cursor, statement, context)
        if transaction_open or insert_outside_transactions is not None:
            return
        self._require_transactions("A transaction (isolation_level 'READ COMMITTED')")
        cursor.execute("BEGIN WORK")
        info[self._TRANSACTION_OPEN] = True

    @classmethod
    def _insert_run_outside_transactions(cls, statement: str) -> Optional[str]:
        """Return ``INSERT INTO FILES`` or ``INSERT OVERWRITE`` for such a statement, past any comment or hint.

        ``insert().with_dialect_options(starrocks_overwrite=True)`` compiles to ``INSERT OVERWRITE`` too.
        """
        keyword = cls._statement_keyword_token(statement)
        if keyword is None:
            return None
        if cls._INSERT_INTO_FILES.match(statement, keyword.start()):
            return "INSERT INTO FILES"
        if cls._INSERT_OVERWRITE.match(statement, keyword.start()):
            return "INSERT OVERWRITE"
        return None

    def _refuse_delete_escaping_transaction(
        self, cursor: "DBAPICursor", statement: str, context: ExecutionContext
    ) -> None:
        """Refuse a DELETE that StarRocks would apply at once, outside the transaction.

        StarRocks runs a DELETE on a DUPLICATE, AGGREGATE or UNIQUE KEY table as a job of its own, which ROLLBACK
        does not undo (https://github.com/StarRocks/starrocks/issues/81471). Any other DELETE, on a table
        information_schema doesn't list or on a view, is left for StarRocks to judge.
        """
        target = self._delete_target(statement, context)
        if target is None:
            return
        table_name, schema = target
        table_model = self._table_model(cursor, table_name, schema)
        if table_model in self._KEYED_MODELS_WITHOUT_PRIMARY_KEY:
            raise exc.InvalidRequestError(
                f"StarRocks applies a DELETE on the {table_model} table {table_name!r} at once, outside the "
                "transaction, and ROLLBACK does not undo it (https://github.com/StarRocks/starrocks/issues/81471); "
                "delete from it on an AUTOCOMMIT connection instead"
            )

    @classmethod
    def _delete_target(
        cls, statement: str, context: Optional[ExecutionContext] = None
    ) -> Optional[Tuple[str, Optional[str]]]:
        """Return the (name, schema) of the table a DELETE deletes from, or None for other statements.

        A ``delete()`` construct names its table, in the schema the ``schema_translate_map`` gives it. Text is read
        from its DELETE keyword, after a WITH clause too, past comments and optimizer hints before the table, and a
        catalog-qualified ``catalog.database.table`` gives the database as the schema. A text DELETE whose table
        can't be read raises InvalidRequestError, since it might escape the transaction unnoticed.
        """
        if context is not None and context.isdelete:
            table = context.compiled.statement.table
            return table.name, cls._schema_as_executed(getattr(table, "schema", None), context)
        keyword = cls._statement_keyword_token(statement)
        if keyword is None or keyword.group(0).upper() != "DELETE":
            return None
        target = cls._DELETE_TARGET.match(statement, keyword.start())
        if target is None:
            raise exc.InvalidRequestError(
                "The dialect can't read which table this DELETE deletes from, so it can't tell whether StarRocks "
                "would apply it outside the transaction; name the table after FROM, or run the DELETE on an "
                "AUTOCOMMIT connection"
            )
        names = [cls._unquote_identifier(name) for name in target.groups() if name]
        return names[-1], names[-2] if len(names) > 1 else None

    @staticmethod
    def _schema_as_executed(schema: Optional[str], context: ExecutionContext) -> Optional[str]:
        """Return the schema a construct's table has in the executed statement, after the ``schema_translate_map``."""
        schema_translate_map = context.execution_options.get("schema_translate_map")
        return schema_translate_map.get(schema, schema) if schema_translate_map else schema

    @staticmethod
    def _unquote_identifier(name: str) -> str:
        """Return a backquoted identifier's name, undoubling its backquotes, or a bare one as it is."""
        return name[1:-1].replace("``", "`") if name.startswith("`") else name

    @staticmethod
    def _table_model(cursor: "DBAPICursor", table_name: str, schema: Optional[str]) -> Optional[str]:
        """Return the table's TABLE_MODEL from information_schema.tables_config, or None if it lists no such table."""
        schema_condition = "TABLE_SCHEMA = %s" if schema is not None else "TABLE_SCHEMA = DATABASE()"
        parameters = (schema, table_name) if schema is not None else (table_name,)
        cursor.execute(
            f"SELECT TABLE_MODEL FROM information_schema.tables_config WHERE {schema_condition} AND TABLE_NAME = %s",
            parameters,
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def _wait_for_schema_change_blocking(
        self, cursor: "DBAPICursor", statement: str, context: Optional[ExecutionContext]
    ) -> Dict[str, Optional[SchemaChangeJob]]:
        """Wait for the schema change running on the table a DDL construct changes, and return its jobs."""
        altered = self._table_altered_by(statement, context)
        return self.wait_while_schema_change_runs(cursor, *altered) if altered is not None else {}

    @classmethod
    def _table_altered_by(
        cls, statement: str, context: Optional[ExecutionContext]
    ) -> Optional[Tuple[str, Optional[str]]]:
        """Return the (name, schema) of the existing table a compiled DDL statement changes, if it changes one.

        That is the table of a ``CreateIndex`` or ``DropIndex``, as ``Index.create()``, ``create_all`` and Alembic's
        index operations send, the ``table_name`` of an Alembic ALTER TABLE construct or of the dialect's own, and
        the ``Table`` of a SQLAlchemy one, such as ``SetTableComment``. Text names no table the dialect reads: a
        statement sent through ``exec_driver_sql()``, ``text()`` or ``op.execute()``, and SQLAlchemy's ``DDL()``,
        which is text too, as in a ``DDL("ALTER TABLE ...")`` listening for ``after_create``, return None.

        The schema is the one the statement names. A construct that names a ``Table`` is compiled through the
        ``schema_translate_map``; Alembic's ALTER constructs, and the dialect's own, write the schema they are
        given into the statement, so the map does not reach them.
        """
        if context is None or not context.isddl:
            return None
        ddl = context.compiled.statement
        if isinstance(ddl, (sa_schema.CreateIndex, sa_schema.DropIndex)):
            table = ddl.element.table
            return (table.name, cls._schema_as_executed(table.schema, context)) if table is not None else None
        if statement.lstrip()[:11].upper() != "ALTER TABLE":
            return None
        if isinstance(getattr(ddl, "table_name", None), str):
            return ddl.table_name, getattr(ddl, "schema", None)
        element = getattr(ddl, "element", None)
        if isinstance(element, Table):
            return element.name, cls._schema_as_executed(element.schema, context)
        return None

    def wait_while_schema_change_runs(
        self, cursor: "DBAPICursor", table_name: str, schema: Optional[str]
    ) -> Dict[str, Optional[SchemaChangeJob]]:
        """Block until the table's newest COLUMN, OPTIMIZE and ROLLUP jobs, if any is still running, have ended.

        Return each kind's newest job as the wait last read it, or None for a kind the table has no job of. The
        result is empty when the wait is off.

        A job that ends CANCELLED counts as ended: the statement waiting for it reports its own errors. The wait
        lasts at most ``starrocks_schema_change_timeout`` seconds in all, then raises SchemaChangeTimeoutError
        naming the job and its state; a job can wait on an open transaction, even one of this program's own
        connections. It logs each poll at INFO, and at WARNING once the job has kept it waiting a few seconds.
        ``starrocks_wait_for_schema_change=False`` turns the wait off.
        """
        if not self.wait_for_schema_change:
            return {}
        deadline = None if self.schema_change_timeout is None else time.monotonic() + self.schema_change_timeout
        return {
            kind: self._wait_for_newest_job(cursor, kind, table_name, schema, deadline)
            for kind in self._SCHEMA_CHANGE_JOB_KINDS
        }

    def _wait_for_newest_job(
        self, cursor: "DBAPICursor", kind: str, table_name: str, schema: Optional[str], deadline: Optional[float]
    ) -> Optional[SchemaChangeJob]:
        """Poll the table's newest job of a kind until it ends and return it; past the deadline raise
        SchemaChangeTimeoutError.
        """
        quoted_name = table_name.replace("\\", "\\\\").replace("'", "\\'")
        query = self.gen_show_alter_table_statement(
            quoted_name, kind, schema=schema, state=None, order_by="JobId DESC", limit=1
        )
        table = ".".join(filter(None, (schema, table_name)))
        polls = 0
        job = self._newest_job(cursor, query, table_name)
        while job is not None and job.state not in self._SCHEMA_CHANGE_ENDED_STATES:
            if deadline is not None and time.monotonic() >= deadline:
                raise SchemaChangeTimeoutError(self._schema_change_timeout_message(kind, job.job_id, table, job.state))
            polls += 1
            logger.log(
                self._schema_change_poll_log_level(polls),
                "Waiting for the %s job %s on table %r to end (state: %s, polls: %d).",
                kind, job.job_id, table, job.state, polls,
            )
            self._pause_between_polls(cursor, self._seconds_until_next_poll(deadline))
            job = self._newest_job(cursor, query, table_name)
        return job

    def _seconds_until_next_poll(self, deadline: Optional[float]) -> float:
        """Return the poll interval, or less when the deadline comes sooner."""
        if deadline is None:
            return self._SCHEMA_CHANGE_POLL_SECONDS
        return min(self._SCHEMA_CHANGE_POLL_SECONDS, max(deadline - time.monotonic(), 0.0))

    @classmethod
    def _schema_change_poll_log_level(cls, polls: int) -> int:
        """Return WARNING for the poll after the first few, and for every minute's worth of polls after it."""
        late_polls = polls - cls._SCHEMA_CHANGE_POLLS_BEFORE_WARNING
        warns = late_polls >= 0 and late_polls % cls._SCHEMA_CHANGE_POLLS_BETWEEN_WARNINGS == 0
        return logging.WARNING if warns else logging.INFO

    def _schema_change_timeout_message(self, kind: str, job_id: str, table: str, state: str) -> str:
        """Explain a schema-change wait that timed out: the job, its state, and the options that change the wait."""
        waiting_on_a_transaction = (
            " A job WAITING_TXN waits for the transactions writing the table to end, which can include one that this "
            "program holds open on another connection."
            if state == "WAITING_TXN"
            else ""
        )
        return (
            f"StarRocks' {kind} job {job_id} on table {table!r} is still {state} after {self.schema_change_timeout:g} "
            f"seconds, and StarRocks refuses another change to the table until it ends.{waiting_on_a_transaction} "
            "Pass create_engine() a longer starrocks_schema_change_timeout, or None to wait without a limit, or "
            "starrocks_wait_for_schema_change=False to send statements without waiting."
        )

    def _newest_job(self, cursor: "DBAPICursor", query: str, table_name: str) -> Optional[SchemaChangeJob]:
        """Return the newest of the table's jobs the query lists, or None when it lists none or fails.

        The rows are kept to the table's, and the highest JobId taken, here as well: StarRocks before 4.0.16 and 4.1.6
        ignores the WHERE, ORDER BY and LIMIT of ``SHOW ALTER TABLE ROLLUP``, and lists the ROLLUP jobs of every
        table in the database (https://github.com/StarRocks/starrocks/pull/79281). TableName is compared with
        case, as StarRocks' own filter compares it. A failure, such as an unknown database, is left for the
        statement that waits to report.
        """
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        except self.dbapi.Error:
            return None
        names = [column[0] for column in cursor.description or ()]
        if "State" not in names:
            return None
        if "TableName" in names:
            rows = [row for row in rows if row[names.index("TableName")] == table_name]
        if not rows:
            return None
        row = max(rows, key=lambda job_row: int(job_row[names.index("JobId")])) if "JobId" in names else rows[0]
        job_id = row[names.index("JobId")] if "JobId" in names else "?"
        message = row[names.index("Msg")] if "Msg" in names else ""
        return SchemaChangeJob(str(job_id), str(row[names.index("State")]), str(message or ""))

    def _pause_between_polls(self, cursor: "DBAPICursor", seconds: float) -> None:
        """Wait between schema-change polls; an async driver's dialect waits through the cursor's awaiter."""
        time.sleep(seconds)

    def _get_server_version_info(self, connection: Connection) -> Tuple[int, ...]:
        # get database server version info explicitly over the wire
        # to avoid proxy servers like MaxScale getting in the
        # way with their own values, see #4205
        dbapi_con = connection.connection
        cursor = dbapi_con.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        val = cursor.fetchone()[0]
        cursor.close()
        if isinstance(val, bytes):
            val = val.decode()

        return self._parse_server_version(val)

    def _parse_server_version(self, val: str) -> Tuple[int, ...]:
        server_version_info: tuple[int, ...] = tuple()
        m = re.match(r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:[-\s])?(?P<commit>.*)?", val)
        if m is not None:
            server_version_info = tuple([int(x) for x in m.group(1, 2, 3) if x is not None])

        # setting it here to help w the test suite
        self.server_version_info = server_version_info
        return server_version_info

    def _get_run_mode(self, connection: Connection) -> str:
        """Get the StarRocks system run_mode (shared_data or shared_nothing).

        Args:
            connection: The SQLAlchemy connection object.

        Prefers the read-only ``@@run_mode`` global variable, which needs no
        privilege (StarRocks PR #69247). Falls back to ``ADMIN SHOW FRONTEND
        CONFIG`` (needs OPERATE) for servers without that variable.

        Returns:
            The run_mode as a string ('shared_data' or 'shared_nothing').
        """
        try:
            value = connection.execute(text("SELECT @@run_mode")).scalar()
            if value:
                return value.lower()
        except exc.DBAPIError as e:
            logger.debug(f"@@run_mode unavailable, falling back to ADMIN SHOW FRONTEND CONFIG: {e}")

        try:
            result = connection.execute(text("ADMIN SHOW FRONTEND CONFIG LIKE 'run_mode'"))
            rows = result.fetchall()
            if rows and len(rows) > 0:
                # The result format is: | Key | AliasNames | Value | Type | IsMutable | Comment |
                return rows[0][2].lower()  # Value column
            else:
                # Default to shared_nothing if not found
                return SystemRunMode.SHARED_NOTHING
        except exc.DBAPIError as e:
            # Log the error but don't fail the entire operation
            logger.warning(f"Failed to get run_mode: {e}")
            # Default to shared_nothing if query fails
            return SystemRunMode.SHARED_NOTHING

    def _show_create_table(
        self,
        connection: Connection,
        table: Optional[Table],
        charset: Optional[str] = None,
        full_name: Optional[str] = None,
    ) -> str:
        try:
            return super()._show_create_table(
                connection,
                table,
                charset,
                full_name,
            )
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) == 1064:  # type: ignore[arg-type] # noqa: E501
                raise exc.NoSuchTableError(full_name) from e
            else:
                raise

    @util.memoized_property
    def _tabledef_parser(self) -> _reflection.StarRocksTableDefinitionParser:
        """return the StarRocksTableDefinitionParser, generate if needed.

        The deferred creation ensures that the dialect has
        retrieved server version information first.

        """
        preparer = self.identifier_preparer
        return _reflection.StarRocksTableDefinitionParser(self, preparer)

    def _read_from_information_schema(
        self, connection: Connection, inf_sch_table: str, charset: Optional[str] = None, **kwargs: Any
    ) -> List[_DecodingRow]:
        st = text(dedent(
            f"""
            SELECT *
            FROM information_schema.{inf_sch_table}
            WHERE {" AND ".join([f"{k} = :{k}" for k in kwargs.keys()])}
        """
        )).bindparams(
            *[
                bindparam(k, type_=sqltypes.Unicode)
                for k in kwargs.keys()
            ]
        )
        try:
            rp = connection.execution_options(
                skip_user_error_events=False
            ).execute(st, kwargs)
            rows: list[_DecodingRow] = [
                _DecodingRow(row, charset)
                for row in rp.mappings().fetchall()
            ]
            # NOTE: We should not raise NoSuchTableError if the query returns empty rows.
            # if not rows:
            #     raise exc.NoSuchTableError(f"Empty response for query: '{st}'")
            return rows

        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) == 1146:
                raise exc.NoSuchTableError(
                    f"information_schema.{inf_sch_table}"
                ) from e
            else:
                raise

    @reflection.cache
    def _setup_parser(
        self, connection, table_name, schema = None, **kwargs
    ):
        """
        Override to return different ReflectedState subclasses based on object type.

        Key: Query table_kind only once here, leveraging @reflection.cache.
        """
        if (state := self._preloaded_table_state(kwargs.get("info_cache"), table_name, schema)) is not None:
            return state

        # 1. Query object type (only once)
        table_kind = self._get_table_kind_from_db(connection, table_name, schema)

        # 2. Dispatch based on type
        if table_kind == TableKind.VIEW:
            return self._setup_view_parser(connection, table_name, schema, **kwargs)
        elif table_kind == TableKind.MATERIALIZED_VIEW:
            return self._setup_mv_parser(connection, table_name, schema, **kwargs)
        else:
            return self._setup_table_parser(connection, table_name, schema, **kwargs)

    def _get_table_kind_from_db(self, connection: Connection, table_name: str, schema: Optional[str]) -> str:
        """
        Query object type from the database (without cache).
        Only called once in _setup_parser.
        """
        if not schema:
            schema = connection.dialect.default_schema_name
        # 1. Query information_schema.tables
        table_rows = self._read_from_information_schema(
            connection, "tables", table_schema=schema, table_name=table_name
        )
        if not table_rows:
            raise exc.NoSuchTableError(table_name)

        table_type = table_rows[0].TABLE_TYPE

        # 2. BASE TABLE → "TABLE"
        if table_type == 'BASE TABLE':
            return TableKind.TABLE

        # 3. VIEW → Further Distinguish
        if table_type == 'VIEW':
            mv_rows = self._read_from_information_schema(
                connection, "materialized_views",
                table_schema=schema, table_name=table_name
            )
            return TableKind.MATERIALIZED_VIEW if mv_rows else TableKind.VIEW

        return TableKind.TABLE  # Default fallback

    def _parsed_state_or_create(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> ReflectedState:
        """
        Look the parsed state up under plain string names.

        SQLAlchemy 2.0's ``@reflection.cache`` keys a ``quoted_name`` by ``(name, quote)``
        but a plain ``str`` by its value alone, so the same table reached once through
        the ``Table`` (a ``quoted_name``) and once through a name list (a ``str``) used to
        miss the cache and be reflected twice. SQLAlchemy 1.4 keys both by value.
        """
        return super()._parsed_state_or_create(
            connection,
            None if table_name is None else str(table_name),
            None if schema is None else str(schema),
            **kw,
        )

    @reflection.cache
    def get_table_kind(self, connection, table_name, schema = None, **kwargs):
        """
        Get the object's table_kind (from cache).
        Reuse _setup_parser's cache via _parsed_state_or_create.
        Pass kwargs with `info_cache=inspector.info_cache` if you want to use the cache.
        """
        parsed_state = self._parsed_state_or_create(connection, table_name, schema, **kwargs)
        return parsed_state.table_kind

    @reflection.cache
    def _setup_table_parser(
        self, connection, table_name, schema = None, **kwargs
    ):
        charset: Optional[str] = self._connection_charset
        # logger.debug("setup table parser for table: %s, schema: %s", table_name, schema)

        if not schema:
            schema = connection.dialect.default_schema_name

        table_rows: List[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="tables",
            charset=charset,
            table_schema=schema,
            table_name=table_name,
        )
        if not table_rows:
            raise exc.NoSuchTableError(table_name)
        if len(table_rows) > 1:
            raise exc.InvalidRequestError(
                f"Multiple tables found with name {table_name} in schema {schema}"
            )
        # logger.debug("reflected table row for table: %s, info: %s", table_name, dict(table_rows[0]))
        try:
            table_config_rows: List[_DecodingRow] = self._read_from_information_schema(
                connection=connection,
                inf_sch_table="tables_config",
                charset=charset,
                table_schema=schema,
                table_name=table_name,
            )
        except Exception as e:
            if 'Unknown table \'information_schema.tables_config\'' in str(e):
                table_config_rows = [{
                    'TABLE_ENGINE': table_rows[0].get('ENGINE'),
                }]
            else:
                raise
        if table_config_rows:
            if len(table_config_rows) > 1:
                raise exc.InvalidRequestError(
                    f"Multiple tables found with name {table_name} in schema {schema}"
                )
            table_config_row = table_config_rows[0]
        else:
            table_config_row = {}
        # logger.debug("reflected table config for table: %s, table_config: %s", table_name, dict(table_config_row))

        column_rows: List[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="columns",
            charset=charset,
            table_schema=schema,
            table_name=table_name,
        )

        return self._parse_table_state(
            connection, table_name, schema, table_rows[0], table_config_row, column_rows, charset
        )

    def _parse_table_state(
        self,
        connection: Connection,
        table_name: str,
        schema: str,
        table_row: _DecodingRow,
        table_config_row: Any,
        column_rows: List[_DecodingRow],
        charset: Optional[str],
        read_aggregate_types: bool = True,
    ) -> ReflectedState:
        """
        Complete the reflected state of a table from the rows already read from information_schema.

        SHOW CREATE TABLE gives the partition clause and the auto increment columns, and SHOW FULL COLUMNS gives the
        aggregate type of each column. Only an AGGREGATE KEY table has aggregate types, so a caller that knows the
        table is not one can skip that statement. SHOW INDEX gives the indexes, and their comments, which the
        partition clause is read past.
        """
        parser: _reflection.StarRocksTableDefinitionParser = self._tabledef_parser
        full_name = self._get_quote_full_table_name(table_name, schema=schema)
        show_create_table = self._show_create_table(connection, None, charset, full_name)
        column_comments = {
            row.COLUMN_NAME: row.COLUMN_COMMENT for row in sorted(column_rows, key=lambda row: row.ORDINAL_POSITION)
        }
        column_types = {row.COLUMN_NAME: row.COLUMN_TYPE for row in column_rows}
        column_names = list(column_comments)
        column_autoinc = self._get_autoinc_from_show_create_table(
            show_create_table, column_names, column_comments, column_types
        )
        column_default_kinds = _reflection.column_default_kinds(
            show_create_table, column_names, column_comments, column_types
        )

        # Get aggregate info from `SHOW FULL COLUMNS`
        full_column_rows: List[Row] = (
            self._get_show_full_columns(connection, table_name=table_name, schema=schema)
            if read_aggregate_types
            else []
        )
        column_2_agg_type: Dict[str, str] = {
            row.Field: row.Extra.upper()
            for row in full_column_rows
            if row.Extra
        }

        secondary_indexes = self._secondary_indexes(connection, full_name, show_create_table)
        generated_partition_columns = self._generated_partition_columns(show_create_table)
        if generated_partition_columns:
            column_rows = [row for row in column_rows if row.COLUMN_NAME not in generated_partition_columns]
        partition_clause = self._get_partition_clause_from_create_table(
            show_create_table,
            column_names,
            column_comments,
            column_types,
            [index["dialect_options"].get(f"{DialectName}_comment") for index in secondary_indexes],
        )
        for name, expression in generated_partition_columns.items() if partition_clause else ():
            partition_clause = partition_clause.replace(self.identifier_preparer.quote_identifier(name), expression)
        # Add the partition info into table_config row for convenience
        # But the row object is immutable, so we convert it to a dictionary to modify it.
        table_config_dict = dict(table_config_row)
        if partition_clause:
            table_config_dict['PARTITION_CLAUSE'] = partition_clause

        return parser.parse_table(
            table=table_row,
            table_config=table_config_dict,
            columns=column_rows,
            column_2_agg_type=column_2_agg_type,
            column_autoinc=column_autoinc,
            column_default_kinds=column_default_kinds,
            charset=charset,
            secondary_indexes=secondary_indexes,
        )

    _SECONDARY_INDEX_LINE: Final[re.Pattern[str]] = re.compile(r"^[ \t]*INDEX[ \t]", re.MULTILINE)

    def _secondary_indexes(
        self, connection: Connection, full_name: str, show_create_table: str
    ) -> List[Dict[str, Any]]:
        """Return the table's BITMAP, NGRAMBF, GIN and VECTOR indexes, as SHOW INDEX lists them.

        SHOW CREATE TABLE prints an index and its comment as they are, so its text can't tell an index line from a
        line of a multi-line column comment, or where a multi-line index comment ends. SHOW INDEX lists each
        index as a row. It is read only when a line of SHOW CREATE TABLE starts with ``INDEX``, as every index's
        line does, so a table without indexes costs no statement.
        """
        if not self._SECONDARY_INDEX_LINE.search(show_create_table):
            return []
        rows = connection.exec_driver_sql(f"SHOW INDEX FROM {full_name}").mappings().all()
        return self._tabledef_parser.parse_show_index_rows(rows)

    def get_multi_columns(self, connection: Connection, **kw: Any) -> Any:
        """
        Reflect the columns of many tables, reading information_schema once per schema instead of once per table.

        SQLAlchemy 2.0's ``MetaData.reflect()`` calls this first, and the other multi methods and the single table
        ones then find the tables already parsed in the inspector's cache. Views and materialized views, and any
        table the bulk read cannot cover, are reflected one by one as before.
        """
        self._preload_table_states(connection, **kw)
        return super().get_multi_columns(connection, **kw)

    def _preload_table_states(
        self,
        connection: Connection,
        kind: Any = None,
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        **kw: Any,
    ) -> None:
        """
        Parse every requested base table of a schema into the inspector cache, from one read of each of
        information_schema.tables, tables_config and columns, plus SHOW CREATE TABLE per table.

        Nothing is read when there is no inspector cache to hold the result, when the request excludes tables or
        holds only temporary ones (``ObjectScope.TEMPORARY``), or when the server has no tables_config view; the
        single table path then does the work. A table that can't be read here, as when it is dropped meanwhile, is
        left out, so the single table path reports it, and SQLAlchemy skips it, as before.
        """
        info_cache: Optional[Dict[Any, Any]] = kw.get("info_cache")
        if info_cache is None or not self._request_can_hold_base_tables(kind, scope):
            return
        schema = schema or connection.dialect.default_schema_name
        names = None if not filter_names else sorted(set(filter_names))
        charset: Optional[str] = self._connection_charset
        try:
            table_rows = self._read_schema_rows(connection, "tables", schema, names, charset)
            base_table_rows = {row.TABLE_NAME: row for row in table_rows if row.TABLE_TYPE == "BASE TABLE"}
            if not base_table_rows:
                return
            config_rows = {
                row.TABLE_NAME: row
                for row in self._read_schema_rows(connection, "tables_config", schema, list(base_table_rows), charset)
            }
        except exc.NoSuchTableError:
            return
        column_rows = self._group_rows_by_table(
            self._read_schema_rows(connection, "columns", schema, list(base_table_rows), charset)
        )

        states: Dict[Tuple[str, str], ReflectedState] = info_cache.setdefault(self._PRELOADED_STATES_KEY, {})
        for table_name, table_row in base_table_rows.items():
            config_row = config_rows.get(table_name, {})
            try:
                states[(schema, table_name)] = self._parse_table_state(
                    connection,
                    table_name,
                    schema,
                    table_row,
                    config_row,
                    column_rows.get(table_name, []),
                    charset,
                    read_aggregate_types=self._is_aggregate_key_model(config_row),
                )
            except exc.NoSuchTableError:
                continue

    @staticmethod
    def _request_can_hold_base_tables(kind: Any, scope: Any) -> bool:
        """Whether a multi reflection request's ``kind`` and ``scope`` take in base tables of the default scope.

        A request without them, as SQLAlchemy 1.4 has none, takes them in.
        """
        object_kind = getattr(reflection, "ObjectKind", None)
        object_scope = getattr(reflection, "ObjectScope", None)
        if object_kind is not None and kind is not None and object_kind.TABLE not in kind:
            return False
        return object_scope is None or scope is None or object_scope.DEFAULT in scope

    def _preloaded_table_state(
        self, info_cache: Optional[Dict[Any, Any]], table_name: str, schema: Optional[str]
    ) -> Optional[ReflectedState]:
        """The state of a table that get_multi_columns already parsed, or None."""
        if info_cache is None:
            return None
        return info_cache.get(self._PRELOADED_STATES_KEY, {}).get((schema or self.default_schema_name, table_name))

    @staticmethod
    def _is_aggregate_key_model(table_config_row: Any) -> bool:
        table_model = table_config_row.get(TableConfigKey.TABLE_MODEL) if table_config_row else None
        return TableInfoKey.MODEL_TO_KEY_MAP.get(table_model) == TableInfoKey.AGGREGATE_KEY

    @staticmethod
    def _group_rows_by_table(rows: List[_DecodingRow]) -> Dict[str, List[_DecodingRow]]:
        grouped: Dict[str, List[_DecodingRow]] = {}
        for row in rows:
            grouped.setdefault(row.TABLE_NAME, []).append(row)
        return grouped

    def _read_schema_rows(
        self,
        connection: Connection,
        inf_sch_table: str,
        schema: str,
        table_names: Optional[List[str]],
        charset: Optional[str],
    ) -> List[_DecodingRow]:
        """
        Read the rows of an information_schema view for one schema, restricted to ``table_names`` when given.

        The names are sent in chunks so a long list does not become one huge statement.
        """
        chunks: List[Optional[List[str]]] = (
            [None]
            if table_names is None
            else [
                table_names[start:start + self._MULTI_REFLECT_CHUNK_SIZE]
                for start in range(0, len(table_names), self._MULTI_REFLECT_CHUNK_SIZE)
            ]
        )
        rows: List[_DecodingRow] = []
        for chunk in chunks:
            where = "table_schema = :table_schema" + ("" if chunk is None else " AND table_name IN :table_names")
            statement = text(f"SELECT * FROM information_schema.{inf_sch_table} WHERE {where}").bindparams(
                bindparam("table_schema", type_=sqltypes.Unicode),
                *([] if chunk is None else [bindparam("table_names", type_=sqltypes.Unicode, expanding=True)]),
            )
            params = {"table_schema": schema, **({} if chunk is None else {"table_names": chunk})}
            try:
                result = connection.execution_options(skip_user_error_events=False).execute(statement, params)
                rows.extend(_DecodingRow(row, charset) for row in result.mappings().fetchall())
            except exc.DBAPIError as e:
                if self._extract_error_code(e.orig) == 1146:
                    raise exc.NoSuchTableError(f"information_schema.{inf_sch_table}") from e
                raise
        return rows

    def _get_quote_full_table_name(
        self, table_name: str, schema: Optional[str] = None
    ) -> str:
        return ".".join(
            self.identifier_preparer._quote_free_identifiers(
                schema, table_name
            )
        )

    def _get_autoinc_from_show_create_table(
        self,
        create_table: str,
        column_names: Optional[Sequence[str]] = None,
        column_comments: _reflection.ColumnComments = None,
        column_types: _reflection.ColumnTypes = None,
    ) -> Dict[str, Any]:
        """
        Get the auto increment info from the SHOW CREATE TABLE statement.

        Args:
            create_table: The SHOW CREATE TABLE statement.
            column_names: The table's column names in ORDINAL_POSITION order, which delimit the column definitions.
            column_comments: Each column's COLUMN_COMMENT, by which its comment is stepped over.
            column_types: Each column's COLUMN_TYPE, by which its type is stepped over.

        Returns:
            A dictionary of column names and their auto increment info.
            The key is the column name, the value is True/False whether the column has auto increment
            Example: `{"col1": True, "col2": False, ...}`
        """
        return _reflection.column_auto_increments(create_table, column_names, column_comments, column_types)

    _GENERATED_PARTITION_COLUMN: Final[re.Pattern[str]] = re.compile(
        r'^\s*`(__generated_partition_column_\d+)`\s.*?\sAS\s(.+?)\sCOMMENT\s"', re.MULTILINE
    )

    @classmethod
    def _generated_partition_columns(cls, create_table: str) -> Dict[str, str]:
        """Return the hidden columns StarRocks generated for an expression partition, with their expressions.

        StarRocks 3.5 partitions by an expression such as ``substr(s, 1, 2)`` or ``time_slice(dt, ..., ceil)``
        through a generated column ``__generated_partition_column_N``, which SHOW CREATE TABLE and information_schema
        list and PARTITION BY names. The model has neither, so reflection leaves the column out and puts its
        expression back into the partition clause. StarRocks 4.1 hides the column itself.
        """
        return {match.group(1): match.group(2) for match in cls._GENERATED_PARTITION_COLUMN.finditer(create_table)}

    def _get_partition_clause_from_create_table(
        self,
        create_table: str,
        column_names: Optional[Sequence[str]] = None,
        column_comments: _reflection.ColumnComments = None,
        column_types: _reflection.ColumnTypes = None,
        index_comments: Optional[Sequence[Optional[str]]] = None,
    ) -> Optional[str]:
        """
        Get the PARTITION BY clause from the SHOW CREATE TABLE statement.
        Because we can't get the partition info from any information_schema views.

        It is read after the column list and outside literals, so a column or index comment that names PARTITION BY
        is not taken for it; see :func:`starrocks.reflection.show_create_partition_clause`.
        """
        return _reflection.show_create_partition_clause(
            create_table, column_names, column_comments, column_types, index_comments
        )

    def _get_show_full_columns(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> List[Row]:
        """Run SHOW FULL COLUMNS to get detailed column information.
        Currently, it's only used to get aggregate type of columns.
        Other column info are still mainly extracted from information_schema.columns.
        """
        full_table_name = self._get_quote_full_table_name(table_name, schema)
        try:
            st: str = "SHOW FULL COLUMNS FROM %s" % full_table_name
            # logger.debug(f"query special column info by using: {st}")
            return list(connection.exec_driver_sql(st).fetchall())
        except exc.DBAPIError as e:
            # 1146: Table ... doesn't exist
            if e.orig and e.orig.args[0] == 1146:
                raise exc.NoSuchTableError(table_name) from e
            raise
        except Exception as e:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            logger.warning(f"Could not get SHOW FULL COLUMNS for table {full_table_name}: {e}")
            return []

    @staticmethod
    def gen_show_alter_table_statement(table_name: str, alter_type: str,
            schema: Optional[str] = None, state: Optional[str] = 'RUNNING',
            order_by: Optional[str] = None, limit: Optional[int] = None,
            bind_table_name: bool = False) -> str:
        """Generate a SHOW ALTER TABLE [ COLUMN | OPTIMIZE | ROLLUP ] statement for a table.

        Args:
            table_name: The name of the table to filter on.
            alter_type: ``COLUMN``, ``OPTIMIZE`` or ``ROLLUP``.
            schema: The schema (StarRocks database) of the table.
            state: Filter on a job state (e.g. ``RUNNING``); pass ``None`` to
                return jobs in any state.
            order_by: An ``ORDER BY`` expression, e.g. ``JobId DESC``.
            limit: A ``LIMIT`` row count.
            bind_table_name: Render the table predicate as the bound parameter
                ``:table_name`` instead of a literal, so the caller can pass it
                through the driver (the caller then binds ``table_name``).

        The schema is quoted by the identifier preparer of a dialect that leaves ``%`` single, since the
        statement is run as ``text()``, which doubles ``%`` itself, or by a cursor without parameters.
        """
        preparer = _dialect_leaving_percents_single().identifier_preparer
        from_db_clause = f"FROM {preparer.quote_identifier(schema)} " if schema else ""
        table_predicate = ":table_name" if bind_table_name else f"'{table_name}'"
        state_clause = f" AND State='{state}'" if state else ""
        order_by_clause = f" ORDER BY {order_by}" if order_by else ""
        limit_clause = f" LIMIT {int(limit)}" if limit else ""
        stmt = (f"SHOW ALTER TABLE {alter_type} {from_db_clause}"
                f"WHERE TableName={table_predicate}{state_clause}{order_by_clause}{limit_clause}")
        # logger.debug("generate show alter table statement: %s", stmt)
        return stmt

    @staticmethod
    def get_show_alter_table(connection: Connection, table_name: str, alter_type: str,
            schema: Optional[str] = None, state: str = 'RUNNING') -> Optional[Row]:
        """Get the SHOW ALTER TABLE OPTIMIZE statement for a given table."""
        if alter_type.upper() not in ["COLUMN", "OPTIMIZE"]:
            raise exc.NotSupportedError("You can only SHOW ALTER TABLE [ COLUMN | OPTIMIZE ].")
        st: str = StarRocksDialect.gen_show_alter_table_statement(table_name, alter_type, schema, state)
        try:
            return connection.execute(text(st)).fetchone()
        except exc.DBAPIError as e:
            # 1146: Table ... doesn't exist
            if e.orig and e.orig.args[0] == 1146:
                raise exc.NoSuchTableError(table_name) from e
            raise
        except Exception as e:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            logger.warning(f"Could not get SHOW ALTER TABLE OPTIMIZE for table {full_table_name}: {e}")
            return None

    @reflection.cache
    def get_table_comment(
            self,
            connection,
            table_name,
            schema = None,
            **kw,
    ):
        """Get the table comment from the parsed state.
        Overrides the mysql's implementation, which will use 'mysql_comment' as the key,
        here the `comment` supports lower case only.
        """
        parsed_state = self._parsed_state_or_create(connection, table_name, schema, **kw)
        comment = parsed_state.table_options.get(TableInfoKeyWithPrefix.COMMENT, None)
        if comment is not None:
            return {"text": comment}
        else:
            return ReflectionTableDefaults.table_comment()

    @reflection.cache
    def get_indexes(
        self, connection, table_name, schema = None, **kwargs
    ):

        parsed_state: Any = self._parsed_state_or_create(
            connection, table_name, schema, **kwargs
        )

        indexes: List[Dict[str, Any]] = []

        # TODO: same logic as MySQL?
        for spec in parsed_state.keys:

            dialect_options: Dict[str, Any] = {}
            unique = False
            flavor: Optional[str] = spec["type"]
            if flavor == "PRIMARY":
                continue
            if flavor == "DUPLICATE":
                continue
            if flavor == "UNIQUE":
                unique = True
            elif flavor in ("FULLTEXT", "SPATIAL"):
                dialect_options["%s_prefix" % self.name] = flavor
            elif flavor is None:
                pass
            else:
                self.logger.info(
                    "Converting unknown KEY type %s to a plain KEY", flavor
                )
                pass

            if spec["parser"]:
                dialect_options["%s_with_parser" % (self.name)] = spec[
                    "parser"
                ]

            index_d: Dict[str, Any] = {}

            index_d["name"] = spec["name"]
            index_d["column_names"] = [s[0] for s in spec["columns"]]
            mysql_length: Dict[str, Any] = {
                s[0]: s[1] for s in spec["columns"] if s[1] is not None
            }
            if mysql_length:
                dialect_options["%s_length" % self.name] = mysql_length

            index_d["unique"] = unique
            if flavor:
                index_d["type"] = flavor

            if dialect_options:
                index_d["dialect_options"] = dialect_options

            indexes.append(index_d)
        indexes.extend(dict(index) for index in parsed_state.secondary_indexes)
        return indexes

    @reflection.cache
    def has_table(
        self, connection, table_name, schema = None, **kwargs
    ):
        try:
            return super().has_table(connection, table_name, schema, **kwargs)
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) in (5501, 5502):
                return False
            if 'not exists' in str(e.orig).lower():
                return False
            raise

    def get_view_names(self, connection: Connection, schema: Optional[str] = None, **kwargs: Any) -> List[str]:
        """Return all view names in a given schema."""
        if schema is None:
            schema = self.default_schema_name
        try:
            rows = self._read_from_information_schema(
                connection,
                "views",
                table_schema=schema,
            )
            return [row.TABLE_NAME for row in rows]
        except Exception:
            return []

    @reflection.cache
    def _setup_view_parser(
        self, connection, view_name, schema = None, **kwargs
    ):
        """
        Fetches raw data for a view and passes it to the parser.
        """
        if schema is None:
            schema = self.default_schema_name

        view_rows = self._read_from_information_schema(
            connection, "views", table_schema=schema, table_name=view_name
        )
        if not view_rows:
            raise exc.NoSuchTableError(view_name)
        view_row = view_rows[0]

        table_row = None
        try:
            table_rows = self._read_from_information_schema(
                connection, "tables", table_schema=schema, table_name=view_name
            )
            if table_rows:
                table_row = table_rows[0]
        except Exception as e:
            self.logger.info(f"Could not retrieve comment for View '{schema}.{view_name}': {e}")

        # Reflect columns for view (only name and comment are meaningful for views)
        column_rows: List[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="columns",
            table_schema=schema,
            table_name=view_name,
        )

        # Get SECURITY from SHOW CREATE VIEW
        # Note: information_schema.views.SECURITY_TYPE is always empty in StarRocks
        create_view_sql = None
        try:
            # Execute SHOW CREATE VIEW to get the full CREATE VIEW statement
            qualified_name = f"`{schema}`.`{view_name}`" if schema else f"`{view_name}`"
            result = connection.execute(text(f"SHOW CREATE VIEW {qualified_name}"))
            rows = result.fetchone()
            if rows:
                # SHOW CREATE VIEW returns: (View, Create View, character_set_client, collation_connection)
                create_view_sql = rows[1] if len(rows) > 1 else None
        except Exception as e:
            self.logger.warning(f"Could not retrieve CREATE VIEW statement for '{schema}.{view_name}': {e}")

        parser = self._tabledef_parser
        return parser.parse_view(view_row, table_row, column_rows, create_view_sql)

    def get_view(self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> ReflectedViewState:
        """
        Return a ReflectedViewState object for a single view.
        Pass kwargs with `info_cache=inspector.info_cache` if you want to use the cache.

        Raises NoSuchTableError if the view does not exist.
        """
        view_info = self._setup_view_parser(connection, view_name, schema=schema, **kwargs)
        logger.debug(
            "get_view normalized: schema=%s, name=%s, security=%s, definition=(%s)",
            schema, view_info.name, view_info.security, view_info.definition
        )
        return ReflectionViewDefaults.apply_info(view_info)

    def get_view_definition(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> Optional[str]:
        """Return the definition of a view.
        Pass kwargs with `info_cache=inspector.info_cache` if you want to use the cache.
        """
        view_state = self._setup_view_parser(connection, view_name, schema=schema, **kwargs)
        return view_state.definition if view_state else None

    def get_materialized_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        """Return all materialized view names in a given schema."""
        if schema is None:
            schema = self.default_schema_name
        try:
            rows: List[_DecodingRow] = self._read_from_information_schema(
                connection,
                "materialized_views",
                table_schema=schema,
            )
            return [row.TABLE_NAME for row in rows]
        except Exception:
            return []

    @reflection.cache
    def _setup_mv_parser(
        self, connection, view_name, schema = None, **kwargs
    ):
        """
        Fetches all raw data for a Materialized View and passes it to the parser.
        """
        if schema is None:
            schema = self.default_schema_name

        try:
            # 1. Get MV row (contains DDL) from information_schema.materialized_views
            mv_rows = self._read_from_information_schema(
                connection, "materialized_views", table_schema=schema, table_name=view_name
            )
            if not mv_rows:
                raise exc.NoSuchTableError(view_name)
            mv_row = mv_rows[0]

            # 2. Get table row (for comment) from information_schema.tables
            table_row = None
            try:
                table_rows = self._read_from_information_schema(
                    connection, "tables", table_schema=schema, table_name=view_name
                )
                if table_rows:
                    table_row = table_rows[0]
            except Exception as e:
                self.logger.info(f"Could not retrieve comment for MV '{schema}.{view_name}': {e}")

            # 3. Get config row (for distribution, order_by) from information_schema.tables_config
            config_row = None
            try:
                config_rows = self._read_from_information_schema(
                    connection, "tables_config", table_schema=schema, table_name=view_name
                )
                if config_rows:
                    config_row = config_rows[0]
            except Exception as e:
                self.logger.info(f"Could not retrieve config for MV '{schema}.{view_name}': {e}")

            # 4. Get the columns from information_schema.columns, as the view path does
            column_rows: List[_DecodingRow] = self._read_from_information_schema(
                connection=connection,
                inf_sch_table="columns",
                table_schema=schema,
                table_name=view_name,
            )

            # 5. Pass all raw data to the parser
            parser = self._tabledef_parser
            return parser.parse_mv(mv_row, table_row, config_row, column_rows)

        except Exception as e:
            self.logger.warning(f"Failed to get materialized view info for '{schema}.{view_name}': {e}")
            raise

    def get_materialized_view(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> ReflectedMVState:
        """Return all information about a materialized view.
        Pass kwargs with `info_cache=inspector.info_cache` if you want to use the cache.
        """
        mv_info = self._setup_mv_parser(connection, view_name, schema=schema, **kwargs)
        logger.debug("get_materialized_view normalized: schema=%s, name=%s, definition=(%s)",
                     schema, view_name, mv_info.definition)
        return ReflectionMVDefaults.apply_info(mv_info)

    def get_materialized_view_options(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> Dict[str, str]:
        """Return the physical properties of a materialized view.
        Pass kwargs with `info_cache=inspector.info_cache` if you want to use the cache.
        """
        mv_info = self._setup_mv_parser(connection, view_name, schema=schema, **kwargs)
        return mv_info.table_options


# --- Alembic alter column compilers for StarRocks ---
"""
For MySQLModifyColumn, MySQLChangeColumn, MySQLAlterDefault,
We should register the 'starrocks' compiler for them.
In the future, we may implement StarRocks's alter_table in StarRocksImpl to override MySQL's alter_table.
TODO: Then, we can add more StarRocks specific attributes, such as KEY/agg_type.
"""


@compiles(MySQLModifyColumn, DialectName)
def _starrocks_modify_column(element: MySQLModifyColumn, compiler: StarRocksDDLCompiler, **kw: Any) -> str:
    """Compile MODIFY COLUMN, with the ``KEY`` or aggregate type the Alembic impl found for the column, if any."""
    return "%s MODIFY COLUMN %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_column_name(compiler, element.column_name),
        compiler.get_column_spec_for_alter_column(
            name=element.column_name,
            nullable=element.nullable,
            default=element.default,
            type_=element.type_,
            autoincrement=element.autoincrement,
            comment=element.comment,
            key_or_aggregate=getattr(element, KEY_OR_AGGREGATE_ATTRIBUTE, None),
        ),
    )


KEY_OR_AGGREGATE_ATTRIBUTE: Final[str] = "starrocks_key_or_aggregate"
"""The attribute of a MODIFY or CHANGE COLUMN element that holds the column's ``KEY`` or aggregate type."""


@compiles(MySQLChangeColumn, DialectName)
def _starrocks_change_column(element: MySQLChangeColumn, compiler: StarRocksDDLCompiler, **kw: Any) -> str:
    """Compile MySQL's CHANGE COLUMN as StarRocks' MODIFY COLUMN, or its RENAME COLUMN.

    StarRocks has no CHANGE COLUMN, and refuses MODIFY COLUMN and RENAME COLUMN in one ALTER TABLE, so a
    CHANGE that does both raises; ``op.alter_column`` sends the two separately. Alembic's MySQL impl sends a
    CHANGE to the same name for every DATETIME column, which is a MODIFY alone.
    """
    renamed = element.newname is not None and element.newname != element.column_name
    rename_clause = "RENAME COLUMN %s TO %s" % (
        format_column_name(compiler, element.column_name),
        format_column_name(compiler, element.newname),
    ) if renamed else None

    modify_clause = "MODIFY COLUMN %s %s" % (
        format_column_name(compiler, element.column_name),
        compiler.get_column_spec_for_alter_column(
            name=element.column_name,
            nullable=element.nullable,
            default=element.default,
            type_=element.type_,
            autoincrement=element.autoincrement,
            comment=element.comment,
            key_or_aggregate=getattr(element, KEY_OR_AGGREGATE_ATTRIBUTE, None),
        ),
    ) if (element.nullable is not None
            or element.type_ is not None
            or element.autoincrement is not None
            or element.comment is not None
    ) else None

    alter_claus_header: str = alter_table(compiler, element.table_name, element.schema)
    if rename_clause and modify_clause:
        raise exc.CompileError(
            f"StarRocks cannot MODIFY and RENAME column {element.column_name!r} in one ALTER TABLE; "
            "op.alter_column sends them separately"
        )
    return "%s %s" % (alter_claus_header, rename_clause or modify_clause)


@compiles(ColumnName, DialectName)
def _starrocks_rename_column(element: ColumnName, compiler: StarRocksDDLCompiler, **kw: Any) -> str:
    """StarRocks renames a column with ``RENAME COLUMN``, where Alembic's default leaves out ``COLUMN``."""
    return "%s RENAME COLUMN %s TO %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_column_name(compiler, element.column_name),
        format_column_name(compiler, element.newname),
    )


@compiles(RenameTable, DialectName)
def _starrocks_rename_table(element: RenameTable, compiler: StarRocksDDLCompiler, **kw: Any) -> str:
    """StarRocks renames with ``ALTER TABLE t RENAME new_name``: there is no ``TO``, and the new name is unqualified."""
    return "%s RENAME %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_table_name(compiler, element.new_table_name, None),
    )


@compiles(MySQLAlterDefault, DialectName)
def _starrocks_alter_default(element: MySQLAlterDefault, compiler: StarRocksDDLCompiler, **kw: Any) -> str:  # type: ignore[name-defined]
    """
    StarRocks cannot change a column's default value, so refuse instead of rendering a statement it rejects.

    The statement Alembic's MySQL dialect builds, ``MODIFY COLUMN c DEFAULT ...``, has no column type, and
    StarRocks rejects it with "Column's type is null" even before it reaches the real limitation.
    """
    raise column_default_change_error(element.table_name, element.column_name)


@compiles(_literal_bindparam, DialectName)
def _starrocks_inline_literal(
    element: _literal_bindparam,
    compiler: StarRocksSQLCompiler,
    skip_bind_expression: bool = False,
    **kw: Any,
) -> str:
    """Render an Alembic inline literal inside its type's bind expression, as SQLAlchemy renders a literal bind.

    Alembic renders ``op.inline_literal`` and, in offline mode, each value of ``op.bulk_insert`` as this
    construct, and compiles it to the literal alone. An ARRAY, MAP or STRUCT literal is the JSON document that
    its bind expression builds the value from, and the server cast the bare document to NULLs.
    """
    impl = element.type.dialect_impl(compiler.dialect)
    if not skip_bind_expression and impl._has_bind_expression:
        return compiler.process(impl.bind_expression(element), skip_bind_expression=True, **kw)
    return compiler.render_literal_bindparam(element, **kw)


def column_default_change_error(table_name: str, column_name: str) -> exc.CompileError:
    """Return the CompileError for a change of a column's default value, which StarRocks cannot make."""
    return exc.CompileError(
        f"StarRocks cannot change a column's default value ('Can not change default value'), so the default "
        f"of column {column_name!r} of table {table_name!r} cannot be altered. Recreate the table or the "
        f"column with the new default instead."
    )
