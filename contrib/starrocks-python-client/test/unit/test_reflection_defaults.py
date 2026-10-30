# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from types import SimpleNamespace

from sqlalchemy import testing
from sqlalchemy.testing import expect_raises, fixtures

from starrocks.common.defaults import ReflectionViewDefaults
from starrocks.common.types import ViewSecurityType
from starrocks.common.utils import SQLParseError
from starrocks.engine.interfaces import ReflectedViewState
from starrocks.reflection import (
    StarRocksTableDefinitionParser,
    column_auto_increments,
    column_default_kinds,
    show_create_partition_clause,
)


class ReflectionViewDefaultsTest(fixtures.TestBase):
    def test_reflection_view_defaults_apply_basic(self):
        info: ReflectedViewState = ReflectionViewDefaults.apply(
            name="v1",
            definition="SELECT 1",
            comment=None,
            security=None,
        )
        assert info.name == "v1"
        assert info.definition == "SELECT 1"
        assert info.comment is None or info.comment == ""
        assert info.security == ViewSecurityType.NONE or info.security is None

    def test_reflection_view_defaults_apply_upper_security_and_keep_comment(self):
        info: ReflectedViewState = ReflectionViewDefaults.apply(
            name="v2",
            definition="SELECT 2",
            comment="some comment",
            security="invoker",
        )
        assert info.comment == "some comment"
        assert info.security == ViewSecurityType.INVOKER

    def test_reflection_view_defaults_empty_strings(self):
        info: ReflectedViewState = ReflectionViewDefaults.apply(
            name="v3",
            definition="SELECT 3",
            comment="",
            security="",
        )
        assert info.comment is None or info.comment == ""
        assert info.security == ViewSecurityType.NONE or info.security is None


class ColumnDefaultValueRendererTest(fixtures.TestBase):
    """How a COLUMN_DEFAULT from information_schema.columns becomes a reflected server default."""

    @staticmethod
    def _rendered(column_type: str, default: str | None) -> str | None:
        column = SimpleNamespace(COLUMN_TYPE=column_type, COLUMN_DEFAULT=default)
        return StarRocksTableDefinitionParser._ColumnDefaultValueRenderer().build(column)

    @testing.combinations(
        ("hll", "hll", "\x00", None),
        ("bitmap", "bitmap", "\x00", None),
        ("percentile", "percentile", "\x00", None),
        ("varchar_keeps_nul", "varchar(10)", "\x00", "'\\0'"),
        ("no_default", "int", None, None),
        ("null_default", "int", "NULL", None),
        ("function", "varchar(36)", "uuid()", "(uuid())"),
        ("datetime_now", "datetime", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"),
        argnames="column_type, default, expected",
        id_="iaaa",
    )
    def test_rendered_default(self, column_type, default, expected):
        assert self._rendered(column_type, default) == expected

    @testing.combinations(
        ("padded_literal", "  padded  ", "literal", "'  padded  '"),
        ("null_word_literal", "NULL", "literal", "'NULL'"),
        ("call_like_literal", "uuid()", "literal", "'uuid()'"),
        ("expression", "uuid()", "expression", "(uuid())"),
        ("keyword", "CURRENT_TIMESTAMP", "keyword", "CURRENT_TIMESTAMP"),
        ("no_default_clause", "x", "none", None),
        id_="iaaa",
        argnames="default, kind, expected",
    )
    def test_spelling_from_show_create_table_decides(self, default, kind, expected):
        """information_schema reports these alike; SHOW CREATE TABLE's spelling tells them apart."""
        column = SimpleNamespace(COLUMN_TYPE="varchar(40)", COLUMN_DEFAULT=default)
        assert StarRocksTableDefinitionParser._ColumnDefaultValueRenderer().build(column, kind) == expected


class ColumnDefaultKindsTest(fixtures.TestBase):
    def test_kinds_read_from_show_create_table(self):
        ddl = (
            "CREATE TABLE `t` (\n"
            '  `id` int(11) NULL COMMENT "",\n'
            '  `c_lit` varchar(20) NULL DEFAULT "uuid()" COMMENT "has DEFAULT (x) inside",\n'
            '  `c_fn` varchar(40) NULL DEFAULT (uuid()) COMMENT "",\n'
            '  `d_ts` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT "",\n'
            '  `odd``name` int(11) NULL COMMENT " DEFAULT \\"x\\""\n'
            ") ENGINE=OLAP\nDUPLICATE KEY(`id`)"
        )
        assert column_default_kinds(ddl) == {
            "id": "none",
            "c_lit": "literal",
            "c_fn": "expression",
            "d_ts": "keyword",
            "odd`name": "none",
        }

    def test_engine_text_in_a_comment_or_default_does_not_end_the_columns(self):
        ddl = (
            "CREATE TABLE `t` (\n"
            '  `a` varchar(20) NULL DEFAULT "ENGINE=OLAP" COMMENT "",\n'
            '  `b` int(11) NULL COMMENT "multi\nline ENGINE= comment",\n'
            '  `c` varchar(40) NULL DEFAULT (uuid()) COMMENT "",\n'
            "  INDEX idx_a (`a`) USING BITMAP\n"
            ") ENGINE=OLAP\nDUPLICATE KEY(`a`)"
        )
        assert column_default_kinds(ddl) == {"a": "literal", "b": "none", "c": "expression"}


class ColumnAutoIncrementsTest(fixtures.TestBase):
    def test_only_the_column_definition_counts(self):
        ddl = (
            "CREATE TABLE `t` (\n"
            '  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT "",\n'
            '  `n` int(11) NULL COMMENT "unlike id, not AUTO_INCREMENT",\n'
            '  `d` varchar(20) NULL DEFAULT "AUTO_INCREMENT" COMMENT "",\n'
            '  `my id` bigint(20) NOT NULL auto_increment COMMENT ""\n'
            ") ENGINE=OLAP\nPRIMARY KEY(`id`)"
        )
        assert column_auto_increments(ddl) == {"id": True, "n": False, "d": False, "my id": True}

    @testing.combinations(
        ("4_1", "struct<`auto_increment` int(11), `b` int(11)>"),
        ("3_3", "struct<auto_increment int(11), b int(11)>"),
        argnames="struct_type",
        id_="ia",
    )
    def test_a_struct_field_named_auto_increment(self, struct_type):
        """StarRocks prints the field names in the column's type, backquoted from 4.1."""
        ddl = (
            "CREATE TABLE `t` (\n"
            '  `k` int(11) NULL COMMENT "",\n'
            f'  `s` {struct_type} NULL COMMENT ""\n'
            ") ENGINE=OLAP\nDUPLICATE KEY(`k`)"
        )
        assert column_auto_increments(ddl, ["k", "s"], {"k": "", "s": ""}) == {"k": False, "s": False}

    def test_a_view_lists_no_columns(self):
        assert column_auto_increments("CREATE VIEW `v` AS SELECT 1") == {}


ODD_QUOTE_COLUMNS = ["id", "v", "s", "w", "z"]
"""The columns of a PRIMARY KEY table whose comments hold one ``"`` each, in ORDINAL_POSITION order."""


def _odd_quote_table(comment_quote: str) -> str:
    """SHOW CREATE TABLE of that table, with the ``"`` in its comments spelt ``comment_quote``."""
    return (
        "CREATE TABLE `t_odd` (\n"
        f'  `id` bigint(20) NOT NULL COMMENT "12{comment_quote} pizza",\n'
        '  `v` bigint(20) NOT NULL AUTO_INCREMENT COMMENT "",\n'
        '  `s` varchar(10) NULL DEFAULT "NULL" COMMENT "",\n'
        '  `w` varchar(10) NULL DEFAULT "  x  " COMMENT "",\n'
        f'  `z` varchar(10) NULL COMMENT "closing{comment_quote}"\n'
        ") ENGINE=OLAP \nPRIMARY KEY(`id`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 1 \n"
        'PROPERTIES (\n"replication_num" = "1"\n);'
    )


QUOTED_KEYWORD_TABLE_3_3 = (
    "CREATE TABLE `t_even` (\n"
    '  `id` bigint(20) NOT NULL COMMENT "not "AUTO_INCREMENT" here",\n'
    '  `s` varchar(20) NULL DEFAULT "a\\"b" COMMENT "x" DEFAULT "y",\n'
    '  `n` int(11) NULL COMMENT "line1\n'
    '  `fake` int NOT NULL AUTO_INCREMENT COMMENT "",\n'
    'line3"\n'
    ") ENGINE=OLAP \nPRIMARY KEY(`id`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 1 "
)
"""StarRocks 3.3.22's SHOW CREATE TABLE of a PRIMARY KEY table: the ``"`` in a column comment is not escaped."""


class ShowCreateTableSpellingsTest(fixtures.TestBase):
    """Column definitions read the same from StarRocks 4.1's escaped comments and 3.3's unescaped ones.

    For a PRIMARY KEY table's column, StarRocks 3.3.22 prints ``COMMENT "12" pizza"`` and 4.1 prints
    ``COMMENT "12\\" pizza"``.
    """

    @testing.combinations(("4_1", '\\"'), ("3_3_primary_key", '"'), argnames="comment_quote", id_="ia")
    def test_auto_increment_and_defaults_after_a_comment_holding_a_quote(self, comment_quote):
        ddl = _odd_quote_table(comment_quote)

        assert column_auto_increments(ddl, ODD_QUOTE_COLUMNS) == {
            "id": False, "v": True, "s": False, "w": False, "z": False
        }
        assert column_default_kinds(ddl, ODD_QUOTE_COLUMNS) == {
            "id": "none", "v": "none", "s": "literal", "w": "literal", "z": "none"
        }

    @testing.combinations(("4_1", '\\"'), ("3_3_primary_key", '"'), argnames="comment_quote", id_="ia")
    def test_column_names_read_from_the_statement(self, comment_quote):
        """Without information_schema's names, the columns are the lines' backquoted names."""
        assert column_auto_increments(_odd_quote_table(comment_quote)) == {
            "id": False, "v": True, "s": False, "w": False, "z": False
        }

    def test_a_quoted_keyword_in_a_comment_is_not_read(self):
        names = ["id", "s", "n"]

        assert column_auto_increments(QUOTED_KEYWORD_TABLE_3_3, names) == {"id": False, "s": False, "n": False}
        assert column_default_kinds(QUOTED_KEYWORD_TABLE_3_3, names) == {"id": "none", "s": "literal", "n": "none"}

    def test_columns_printed_without_their_empty_comments(self):
        """An external catalog's table prints a COMMENT clause only for a column that has a comment."""
        ddl = (
            "CREATE TABLE `ext` (\n"
            "  `id` int(11) DEFAULT NULL,\n"
            '  `s` varchar(10) DEFAULT "NULL",\n'
            '  `n` int(11) DEFAULT NULL COMMENT "a \\"n\\""\n'
            ")\nPARTITION BY (`n`)"
        )
        comments = {"id": "", "s": "", "n": 'a "n"'}

        assert column_default_kinds(ddl, list(comments), comments) == {
            "id": "keyword", "s": "literal", "n": "keyword"
        }
        assert show_create_partition_clause(ddl, list(comments), comments) == "(`n`)"


DOC_COMMENT = 'Event day, in "UTC",\n`currency` is next.'
"""A doc-style column comment whose second line starts with the next column's backquoted name."""

NEXT_LINE_COMMENT = 'one\n  `b` bigint(20) NOT NULL AUTO_INCREMENT COMMENT "'
"""A column comment whose second line is the next column's line, with AUTO_INCREMENT, up to its comment."""

LOOKALIKE_LINE_COMMENT = 'one,\n  `b` bigint(20) NOT NULL AUTO_INCREMENT COMMENT "'
"""The same, with a ``,`` closing the first line, so that StarRocks 3.3 prints the second as a column's line."""


def _escaped(comment: str) -> str:
    """The comment as StarRocks prints it, except in the column comments of a 3.3 PRIMARY KEY table."""
    return comment.replace("\\", "\\\\").replace('"', '\\"')


def _doc_comment_table(model: str, printed_comment: str) -> str:
    """SHOW CREATE TABLE of a partitioned table whose ``dt`` column has ``DOC_COMMENT``, printed as given."""
    return (
        "CREATE TABLE `events` (\n"
        '  `id` bigint(20) NOT NULL COMMENT "",\n'
        f'  `dt` date NOT NULL COMMENT "{printed_comment}",\n'
        '  `currency` varchar(3) NULL DEFAULT "EUR" COMMENT ""\n'
        f") ENGINE=OLAP \n{model} KEY(`id`, `dt`)\n"
        "PARTITION BY date_trunc('day', `dt`)\n"
        "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n"
        'PROPERTIES (\n"replication_num" = "1"\n);'
    )


def _next_line_comment_table(model: str, printed_comment: str) -> str:
    """SHOW CREATE TABLE of a table whose ``a`` column has a comment holding ``b``'s line, printed as given."""
    return (
        "CREATE TABLE `t_next` (\n"
        f'  `a` int(11) NOT NULL COMMENT "{printed_comment}",\n'
        '  `b` bigint(20) NULL DEFAULT "7" COMMENT "b\'s own"\n'
        f") ENGINE=OLAP \n{model} KEY(`a`)\nDISTRIBUTED BY HASH(`a`) BUCKETS 1 \n"
    )


SPELLINGS = (
    ("duplicate_key", "DUPLICATE", _escaped),
    ("primary_key_4_1", "PRIMARY", _escaped),
    ("primary_key_3_3", "PRIMARY", str),
)
"""How StarRocks prints a column comment, by key model: escaped, or as it is in a 3.3 PRIMARY KEY table."""


class MultiLineCommentTest(fixtures.TestBase):
    """A line of a multi-line comment that starts with the next column's backquoted name is not read as its line.

    StarRocks prints a comment's newlines as they are, on every key model. The comment is stepped over by its text
    from information_schema; without it, the next column's line is the one that looks like a column's.
    """

    @testing.combinations(*SPELLINGS, argnames="model, spelling", id_="iaa")
    def test_doc_comment_with_information_schema_comments(self, model, spelling):
        ddl = _doc_comment_table(model, spelling(DOC_COMMENT))
        comments = {"id": "", "dt": DOC_COMMENT, "currency": ""}

        assert column_default_kinds(ddl, list(comments), comments) == {
            "id": "none", "dt": "none", "currency": "literal"
        }
        assert show_create_partition_clause(ddl, list(comments), comments) == "date_trunc('day', `dt`)"

    @testing.combinations(*SPELLINGS, argnames="model, spelling", id_="iaa")
    def test_doc_comment_with_column_names_only(self, model, spelling):
        ddl = _doc_comment_table(model, spelling(DOC_COMMENT))
        names = ["id", "dt", "currency"]

        assert column_default_kinds(ddl, names) == {"id": "none", "dt": "none", "currency": "literal"}
        assert show_create_partition_clause(ddl, names) == "date_trunc('day', `dt`)"

    @testing.combinations(*SPELLINGS, argnames="model, spelling", id_="iaa")
    def test_next_column_line_inside_a_comment_with_column_names_only(self, model, spelling):
        ddl = _next_line_comment_table(model, spelling(NEXT_LINE_COMMENT))

        assert column_auto_increments(ddl, ["a", "b"]) == {"a": False, "b": False}
        assert column_default_kinds(ddl, ["a", "b"]) == {"a": "none", "b": "literal"}

    @testing.combinations(*SPELLINGS, argnames="model, spelling", id_="iaa")
    def test_a_comment_is_stepped_over_by_its_text(self, model, spelling):
        """With StarRocks 3.3's spelling, the comment's second line looks just like ``b``'s own line."""
        ddl = _next_line_comment_table(model, spelling(LOOKALIKE_LINE_COMMENT))
        comments = {"a": LOOKALIKE_LINE_COMMENT, "b": "b's own"}

        assert column_auto_increments(ddl, list(comments), comments) == {"a": False, "b": False}
        assert column_default_kinds(ddl, list(comments), comments) == {"a": "none", "b": "literal"}

    @testing.combinations(*SPELLINGS, argnames="model, spelling", id_="iaa")
    def test_a_comment_printed_otherwise_reads_the_line_its_own_comment_follows(self, model, spelling):
        """When a comment can't be stepped over, the next column's line is the one, of those that look like it,
        that the column's own comment follows."""
        ddl = _next_line_comment_table(model, spelling(LOOKALIKE_LINE_COMMENT))
        comments = {"a": "printed otherwise", "b": "b's own"}

        assert column_auto_increments(ddl, list(comments), comments) == {"a": False, "b": False}
        assert column_default_kinds(ddl, list(comments), comments) == {"a": "none", "b": "literal"}


def _struct_table(printed_field: str) -> str:
    """SHOW CREATE TABLE of a partitioned table with a STRUCT column ``s``, later followed by an AUTO_INCREMENT one.

    ``printed_field`` is the STRUCT's field name as StarRocks prints it: backquoted from 4.1, bare in 3.3.
    """
    return (
        "CREATE TABLE `st` (\n"
        '  `id` bigint(20) NOT NULL COMMENT "",\n'
        '  `dt` datetime NOT NULL COMMENT "",\n'
        f'  `s` struct<{printed_field} int(11)> NULL COMMENT "it\'s",\n'
        '  `w` varchar(10) NULL DEFAULT "NULL" COMMENT "",\n'
        '  `ai` bigint(20) NOT NULL AUTO_INCREMENT COMMENT ""\n'
        ") ENGINE=OLAP \nDUPLICATE KEY(`id`, `dt`)\n"
        "PARTITION BY date_trunc('day', dt)\n"
        "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n"
        'PROPERTIES (\n"replication_num" = "1"\n);'
    )


def _struct_table_columns(printed_field: str) -> tuple:
    """The names, comments and COLUMN_TYPEs information_schema gives for ``_struct_table(printed_field)``."""
    comments = {"id": "", "dt": "", "s": "it's", "w": "", "ai": ""}
    types = {
        "id": "bigint(20)", "dt": "datetime", "s": f"struct<{printed_field} int(11)>", "w": "varchar(10)",
        "ai": "bigint(20)",
    }
    return list(comments), comments, types


STRUCT_FIELD_SPELLINGS = (
    ("single_quote_3_3", "owner's"),
    ("double_quote_3_3", 'a"b'),
    ("two_quotes_3_3", "it's int(11), x\"y"),
    ("comment_clause_3_3", 'x COMMENT "y'),
    ("null_auto_increment_3_3", "null auto_increment"),
    ("newline_3_3", "a\nb"),
    ("single_quote_4_1", "`owner's`"),
    ("double_quote_4_1", '`a"b`'),
    ("null_auto_increment_4_1", "`null auto_increment`"),
)
"""STRUCT field names as StarRocks 3.3.22 prints them, bare, and as 4.1.6 does, backquoted."""


class StructFieldNameTest(fixtures.TestBase):
    """A STRUCT field name in a column's type changes nothing else that is read from SHOW CREATE TABLE.

    StarRocks 3.3 prints field names bare, so a quote in one pairs with a quote on a later line, and a field named
    ``null auto_increment`` looks like the column's own AUTO_INCREMENT.
    """

    @testing.combinations(*STRUCT_FIELD_SPELLINGS, argnames="printed_field", id_="ia")
    def test_the_type_is_stepped_over_by_its_column_type(self, printed_field):
        ddl = _struct_table(printed_field)
        names, comments, types = _struct_table_columns(printed_field)

        assert column_auto_increments(ddl, names, comments, types) == {
            "id": False, "dt": False, "s": False, "w": False, "ai": True
        }
        assert column_default_kinds(ddl, names, comments, types) == {
            "id": "none", "dt": "none", "s": "none", "w": "literal", "ai": "none"
        }
        assert show_create_partition_clause(ddl, names, comments, types) == "date_trunc('day', dt)"

    @testing.combinations(
        ("single_quote_3_3", "owner's"), ("double_quote_3_3", 'a"b'), argnames="printed_field", id_="ia"
    )
    def test_without_its_type_a_definition_does_not_run_over_the_next_lines(self, printed_field):
        """Without the types, the STRUCT column is left out rather than read up to a later quote."""
        ddl = _struct_table(printed_field)
        names, comments, _ = _struct_table_columns(printed_field)

        assert column_auto_increments(ddl, names, comments) == {"id": False, "dt": False, "w": False, "ai": True}
        assert column_default_kinds(ddl, names, comments)["w"] == "literal"
        assert show_create_partition_clause(ddl, names, comments) == "date_trunc('day', dt)"

    def test_a_generated_column_naming_a_column_null_auto_increment(self):
        """StarRocks 3.3.22 and 4.1.6 backquote the column names in a generated column's expression."""
        ddl = (
            "CREATE TABLE `gen_ai` (\n"
            '  `id` bigint(20) NOT NULL COMMENT "",\n'
            '  `null auto_increment` bigint(20) NULL COMMENT "",\n'
            '  `ai` bigint(20) NOT NULL AUTO_INCREMENT COMMENT "",\n'
            '  `g` bigint(20) NULL AS `null auto_increment` + 1 COMMENT ""\n'
            ") ENGINE=OLAP \nDUPLICATE KEY(`id`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 1 \n"
        )
        comments = {"id": "", "null auto_increment": "", "ai": "", "g": ""}
        types = dict.fromkeys(comments, "bigint(20)")

        assert column_auto_increments(ddl, list(comments), comments, types) == {
            "id": False, "null auto_increment": False, "ai": True, "g": False
        }

    def test_a_generated_column_literal_may_run_onto_the_next_line_before_its_comment(self):
        """Past a known type the literals pair up, so a literal holding a newline doesn't leave the column out."""
        ddl = (
            "CREATE TABLE `gen_nl` (\n"
            '  `id` bigint(20) NOT NULL COMMENT "",\n'
            '  `w` varchar(10) NULL DEFAULT "NULL" COMMENT "",\n'
            "  `g` varchar(100) NULL AS concat(`w`, 'it\\'s \" COMMENT \"x\n') COMMENT \"gc\"\n"
            ") ENGINE=OLAP \nDUPLICATE KEY(`id`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 1 \n"
        )
        comments = {"id": "", "w": "", "g": "gc"}
        types = {"id": "bigint(20)", "w": "varchar(10)", "g": "varchar(100)"}

        assert column_default_kinds(ddl, list(comments), comments, types) == {
            "id": "none", "w": "literal", "g": "none"
        }


INDEX_COMMENTS = ("x\n) ENGINE=OLAP \nDUPLICATE KEY(`id`)\nPARTITION BY nope(", "", "it's\n) ENGINE=OLAP y")
"""The comments of the indexes of ``INDEX_COMMENT_TABLE``, as SHOW INDEX lists them."""

INDEX_COMMENT_TABLE_PARTITION = 'RANGE(`id`)\n(PARTITION p_texas VALUES [("-9223372036854775808"), ("10")))'

INDEX_COMMENT_TABLE = (
    "CREATE TABLE `ix` (\n"
    '  `id` bigint(20) NOT NULL COMMENT "",\n'
    '  `v` varchar(10) NULL COMMENT "",\n'
    '  `w` varchar(10) NULL COMMENT "",\n'
    '  `x` varchar(10) NULL COMMENT "",\n'
    f"  INDEX i1 (`v`) USING BITMAP COMMENT '{INDEX_COMMENTS[0]}',\n"
    f"  INDEX i2 (`w`) USING BITMAP COMMENT '{INDEX_COMMENTS[1]}',\n"
    '  INDEX i 3 (`x`) USING NGRAMBF("bloom_filter_fpp" = "0.05", "case_sensitive" = "true", "gram_num" = "4") '
    f"COMMENT '{INDEX_COMMENTS[2]}'\n"
    ") ENGINE=OLAP \nDUPLICATE KEY(`id`)\n"
    "COMMENT \"it's\"\n"
    f"PARTITION BY {INDEX_COMMENT_TABLE_PARTITION}\n"
    "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n"
    'PROPERTIES (\n"compression" = "LZ4",\n"replication_num" = "1"\n);'
)
"""StarRocks 3.3.22's and 4.1.6's SHOW CREATE TABLE of a table whose index comments hold lines of the table's."""


class ShowCreatePartitionClauseTest(fixtures.TestBase):
    """The PARTITION BY clause is read after the column list, outside literals."""

    @testing.combinations(("4_1", '\\"'), ("3_3_primary_key", '"'), argnames="comment_quote", id_="ia")
    def test_comments_naming_partition_by_are_no_partition(self, comment_quote):
        ddl = (
            "CREATE TABLE `t` (\n"
            f'  `k` int(11) NOT NULL COMMENT "kept for PARTITION BY month, 12{comment_quote} wide",\n'
            '  `v` int(11) NULL COMMENT ""\n'
            ") ENGINE=OLAP \nPRIMARY KEY(`k`)\n"
            'COMMENT "table \\"PARTITION BY\\" day"\n'
            "DISTRIBUTED BY HASH(`k`) BUCKETS 1 \n"
            'PROPERTIES (\n"replication_num" = "1"\n);'
        )
        assert show_create_partition_clause(ddl, ["k", "v"]) is None

    def test_a_value_naming_a_later_clause_stays_in_the_partition(self):
        ddl = (
            "CREATE TABLE `t` (\n"
            '  `c` varchar(10) NOT NULL COMMENT "",\n'
            '  `v` int(11) NULL COMMENT ""\n'
            ") ENGINE=OLAP \nDUPLICATE KEY(`c`)\n"
            "PARTITION BY LIST(`c`)(\n"
            "  PARTITION p1 VALUES IN ('a', 'DISTRIBUTED BY'),\n"
            "  PARTITION p2 VALUES IN ('b')\n"
            ")\n"
            "DISTRIBUTED BY HASH(`c`) BUCKETS 1 \n"
            'PROPERTIES (\n"replication_num" = "1"\n);'
        )
        assert show_create_partition_clause(ddl, ["c", "v"]) == (
            "LIST(`c`)(\n  PARTITION p1 VALUES IN ('a', 'DISTRIBUTED BY'),\n  PARTITION p2 VALUES IN ('b')\n)"
        )

    @testing.combinations(
        ("ending_in_one", ("p_texas", "p_refresh", "p_properties")),
        ("named_one", ("as", "refresh", "properties")),
        argnames="names",
        id_="ia",
    )
    def test_partition_names_and_clause_keywords(self, names):
        """StarRocks prints partition names bare, even those it needs backquoted, such as ``as``."""
        partitions = (
            f'(PARTITION {names[0]} VALUES [("0000-01-01"), ("2020-01-01")),\n'
            f'PARTITION {names[1]} VALUES [("2020-01-01"), ("2021-01-01")),\n'
            f'PARTITION {names[2]} VALUES [("2021-01-01"), ("2022-01-01")))'
        )
        ddl = (
            "CREATE TABLE `t` (\n"
            '  `dt` date NOT NULL COMMENT ""\n'
            ") ENGINE=OLAP \nDUPLICATE KEY(`dt`)\n"
            f"PARTITION BY RANGE(`dt`)\n{partitions}\n"
            "DISTRIBUTED BY HASH(`dt`) BUCKETS 1 \n"
            'PROPERTIES (\n"replication_num" = "1"\n);'
        )
        assert show_create_partition_clause(ddl, ["dt"], {"dt": ""}) == f"RANGE(`dt`)\n{partitions}"

    def test_an_index_comment_holding_an_engine_line_without_the_index_comments(self):
        """The column list ends at the ``) ENGINE=`` line that a table clause follows."""
        ddl = INDEX_COMMENT_TABLE.replace(INDEX_COMMENTS[0], "x")
        names = ["id", "v", "w", "x"]

        assert show_create_partition_clause(ddl, names, dict.fromkeys(names, "")) == INDEX_COMMENT_TABLE_PARTITION

    def test_an_index_comment_holding_the_table_clauses_is_stepped_over_by_its_text(self):
        """StarRocks 3.3.22 and 4.1.6 print an index comment as it is, even one that copies the table's clauses."""
        names = ["id", "v", "w", "x"]

        assert show_create_partition_clause(
            INDEX_COMMENT_TABLE, names, dict.fromkeys(names, ""), index_comments=INDEX_COMMENTS
        ) == INDEX_COMMENT_TABLE_PARTITION

    @testing.combinations(("opening", "(%Y-%m-%d"), ("closing", ")%Y-%m-%d"), argnames="date_format", id_="ia")
    def test_a_parenthesis_in_a_range_expression_literal(self, date_format):
        """StarRocks 3.3.22 and 4.1.6 print the RANGE expression and partitions so."""
        partitions = '(PARTITION p1 VALUES [("0000-01-01"), ("2020-01-01")))'

        partition = StarRocksTableDefinitionParser.parse_partition_clause(
            f"RANGE(str2date(s, '{date_format}'))\n{partitions}"
        )

        assert (partition.partition_method, partition.pre_created_partitions) == (
            f"RANGE(str2date(s, '{date_format}'))", partitions
        )

    @testing.combinations(
        ("unclosed", "RANGE(dt", "mismatched parentheses"),
        ("no_columns", "LIST", "no columns specified"),
        argnames="clause, message",
        id_="iaa",
    )
    def test_a_range_or_list_clause_that_cant_be_read_raises_a_parse_error(self, clause, message):
        with expect_raises(SQLParseError) as raised:
            StarRocksTableDefinitionParser.parse_partition_clause(clause)

        assert (message in str(raised.error), raised.error.statement) == (True, clause)

    def test_a_partition_clause_whose_parentheses_dont_close_ends_at_the_first_keyword(self):
        masked = "PARTITION BY RANGE(dt\nDISTRIBUTED BY HASH(dt) BUCKETS 1\nPROPERTIES ()"

        start, end = StarRocksTableDefinitionParser.partition_clause_bounds(masked)

        assert masked[start:end].strip() == "RANGE(dt"


class KnownCommentEndTest(fixtures.TestBase):
    """Where a column's COMMENT clause ends, when StarRocks prints the comment escaped or, on 3.3, as it is."""

    @testing.combinations(
        ("escaped_quote", 'a"b', 'a\\"b'),
        ("raw_quote_as_3_3_prints_a_primary_key_table", 'a"b', 'a"b'),
        ("escaped_backslash", "a\\c", "a\\\\c"),
        ("raw_backslash_as_3_3_prints_a_primary_key_table", "a\\c", "a\\c"),
        ("plain", "plain", "plain"),
        argnames="comment,printed",
        id_="iaa",
    )
    def test_the_clause_ends_after_the_closing_quote_in_either_spelling(self, comment, printed):
        from starrocks.reflection import _known_comment_end

        create_table = f'CREATE TABLE `t` (\n  `a` int(11) NULL COMMENT "{printed}",\n  `b` int(11) NULL COMMENT "b"\n)'
        definition_end = create_table.index(" COMMENT")
        expected = create_table.index(',\n  `b`')
        assert _known_comment_end(create_table, definition_end, comment) == expected

    def test_a_clause_holding_another_comment_is_not_stepped_over(self):
        from starrocks.reflection import _known_comment_end

        create_table = 'CREATE TABLE `t` (\n  `a` int(11) NULL COMMENT "a\\"b",\n  `b` int(11) NULL COMMENT "b"\n)'
        assert _known_comment_end(create_table, create_table.index(" COMMENT"), "other") is None
