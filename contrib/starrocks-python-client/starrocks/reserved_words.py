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

"""StarRocks' reserved keywords, which an identifier must be backquoted to use.

The keywords of the StarRocks parser's lexer that its ``nonReserved`` rule leaves out, joined with the list in the
StarRocks documentation (sql-reference/sql-statements/keywords), lower case. Some are reserved only from some
version on, but backquoting a name is always accepted, so the list covers every version.
"""

from typing import FrozenSet


STARROCKS_RESERVED_WORDS: FrozenSet[str] = frozenset({
    "add", "all", "alter", "analyze", "and", "array", "as", "asc", "asof", "between", "bigint", "bitmap",
    "bitshiftleft", "bitshiftright", "bitshiftrightlogical", "both", "by", "case", "char", "character", "check",
    "collate", "column", "compaction", "convert", "create", "cross", "cube", "current_date", "current_group",
    "current_role", "current_time", "current_timestamp", "current_user", "current_warehouse", "database",
    "databases", "decimal", "decimal128", "decimal256", "decimal32", "decimal64", "decimalv2", "default",
    "deferred", "delete", "dense_rank", "desc", "describe", "distinct", "div", "double", "drop", "dual", "else",
    "except", "exclude", "exists", "explain", "false", "files", "first_value", "float", "for", "force", "from",
    "full", "function", "gin", "grant", "group", "grouping", "grouping_id", "groups", "having", "hll", "host",
    "if", "ignore", "immediate", "in", "index", "infile", "inner", "insert", "int", "integer", "intersect",
    "into", "invoker", "is", "join", "json", "key", "keys", "kill", "lag", "largeint", "last_query_id",
    "last_value", "lateral", "lead", "left", "like", "limit", "load", "localtime", "localtimestamp", "maxvalue",
    "microsecond", "millisecond", "minus", "mod", "not", "ntile", "null", "on", "optimize", "or", "order",
    "outer", "outfile", "over", "partition", "percentile", "prepare", "primary", "procedure", "qualify",
    "range", "rank", "read", "regexp", "release", "rename", "replace", "revoke", "right", "rlike", "row",
    "row_number", "rows", "schema", "schemas", "select", "set", "set_var", "show", "smallint", "system",
    "table", "terminated", "text", "then", "tinyint", "to", "true", "union", "unique", "unsigned", "update",
    "use", "using", "values", "varchar", "when", "where", "with",
})
