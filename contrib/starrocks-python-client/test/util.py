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

import asyncio
import re
from typing import Any, Coroutine

from sqlalchemy import testing, util
from sqlalchemy.testing import config


def normalize_sql(sql: str) -> str:
    """Normalizes a SQL string for comparison."""
    # Remove comments
    sql = re.sub(r'--.*\n', '', sql)
    # Replace newlines and tabs with spaces
    sql = sql.replace('\n', ' ').replace('\t', '')
    # Collapse multiple spaces into one
    sql = re.sub(r'\s+', ' ', sql)
    # Remove spaces around parentheses, commas, and equals for consistency
    sql = re.sub(r'\s*\(\s*', '(', sql)
    sql = re.sub(r'\s*\)\s*', ')', sql)
    sql = re.sub(r'\s*,\s*', ',', sql)
    sql = re.sub(r'\s*=\s*', '=', sql)
    # Remove all paired backticks around identifiers
    sql = re.sub(r'`([^`]+)`', r'\1', sql)
    sql = re.sub(r'\'', '"', sql)

    return sql.strip()


def normalize_py_call(s: str) -> str:
    """Collapse the whitespace in rendered Python calls, so line breaks and indentation are ignored."""
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r" , ", ", ", s)
    s = re.sub(r"\( ", "(", s)
    s = re.sub(r" \)", ")", s)
    s = re.sub(r"\[ ", "[", s)
    s = re.sub(r" \]", "]", s)
    return s


def run_coroutine(coroutine: Coroutine[Any, Any, Any]) -> Any:
    """Run a coroutine from a test, under any ``--dburi``.

    Under an async driver the plugin already runs each test in a greenlet inside its event loop, where
    ``asyncio.run()`` refuses to start another, so the coroutine is awaited on that loop instead.
    """
    if config.db.dialect.is_async:
        return util.await_only(coroutine)
    return asyncio.run(coroutine)


def _server_version(config: object) -> tuple:
    return tuple(config.db.dialect.server_version_info or ())


VIEW_SECURITY_NEEDS_3_4_1 = testing.skip_if("starrocks<3.4.1", "StarRocks has no view SECURITY before 3.4.1")
TRANSACTIONS_NEED_3_5 = testing.skip_if("starrocks<3.5", "StarRocks has transactions from 3.5")
MV_PARTITION_RETENTION_NEEDS_3_5 = testing.skip_if(
    "starrocks<3.5", "partition_retention_condition is a materialized view property from StarRocks 3.5"
)
STATISTIC_COLLECT_PROPERTY_MISSING = testing.skip_if(
    lambda config: (version := _server_version(config)) < (3, 5, 10) or (4, 0, 0) <= version < (4, 0, 2),
    "enable_statistic_collect_on_first_load is a table property from StarRocks 3.5.10 and 4.0.2",
)
FLAT_JSON_PROPERTIES_NEED_4_0 = testing.skip_if("starrocks<4.0", "flat_json.* are table properties from StarRocks 4.0")
FLAT_JSON_NULL_AMONG_OBJECTS_READS_AS_EMPTY = testing.fails_if(
    "starrocks>=4.0",
    "Flat JSON, on by default from StarRocks 4.0, reads a JSON null stored with JSON objects back as {}: "
    "StarRocks issue 81490",
)
DECIMAL256_NEEDS_4_0 = testing.skip_if("starrocks<4.0", "DECIMAL precision above 38 needs StarRocks 4.0")
STRUCT_FIELD_NAMES_UNQUOTED = testing.skip_if(
    "starrocks<4.1.1",
    "Before 4.1.1 StarRocks prints STRUCT field names unquoted, so a name with a space cannot be read back",
)
JSON_TO_MAP_CAST_NEEDS_3_4_3 = testing.skip_if("starrocks<3.4.3", "StarRocks casts JSON to MAP from 3.4.3")
TIME_SLICE_CEIL_PARTITION_NEEDS_3_4_1 = testing.skip_if(
    "starrocks<3.4.1", "StarRocks before 3.4.1 refuses time_slice(..., ceil) in PARTITION BY"
)
AUTO_INCREMENT_COMMENT_MODIFY_NEEDS_4_1_3 = testing.skip_if(
    "starrocks<4.1.3",
    "Before 4.1.3, StarRocks refuses any MODIFY COLUMN of an AUTO_INCREMENT column; from 4.1.3 it takes one that "
    "changes nothing but the comment",
)
GENERATED_COLUMN_IN_THE_DEFAULT_KEY_BEFORE_3_4 = testing.skip_if(
    "starrocks<3.4",
    "Before 3.4, StarRocks puts a generated column in the DUPLICATE KEY it picks for a table that names no key, "
    "and then refuses it",
)


def _keeps_boolean_defaults_as_digits(config: object) -> bool:
    version = _server_version(config)
    return (3, 5, 11) <= version < (4, 0) or version >= (4, 0, 4)


BOOLEAN_DEFAULT_KEPT_AS_DIGITS = testing.skip_if(
    lambda config: not _keeps_boolean_defaults_as_digits(config),
    "StarRocks keeps a BOOLEAN column's 'true' and 'false' defaults as '1' and '0' from 3.5.11, 4.0.4 and 4.1.0",
)
BOOLEAN_DEFAULT_KEPT_AS_WRITTEN = testing.skip_if(
    _keeps_boolean_defaults_as_digits,
    "Before 3.5.11, and before 4.0.4 in 4.0, StarRocks keeps a BOOLEAN column's default as written",
)
BUCKET_SIZE_NEEDS_RANDOM_DISTRIBUTION_FROM_4_1 = testing.skip_if(
    "starrocks<4.1",
    "StarRocks 4.1 refuses bucket_size unless the table is distributed by RANDOM; 3.3 takes it for any distribution",
)
IN_MEMORY_PRIMARY_KEY_INDEX_REFUSED_FROM_3_5 = testing.skip_if(
    "starrocks<3.5", "StarRocks refuses a PRIMARY KEY table whose enable_persistent_index is false from 3.5"
)
PRIMARY_KEY_TABLE_SORTED_BY_BINARY_FROM_4_0 = testing.skip_if(
    "starrocks>=4.0",
    "From 4.0 StarRocks sorts a PRIMARY KEY table by VARBINARY, which the dialect refuses on every version, so that "
    "a model gives the same table everywhere",
)
MV_TEXT_REWRITTEN_BEFORE_4_0_2 = testing.skip_if(
    "starrocks<4.0.2", "Before 4.0.2 StarRocks shows an MV definition rewritten, so its text is not compared"
)
MV_TEXT_STORED_AS_WRITTEN_FROM_4_0_2 = testing.skip_if(
    "starrocks>=4.0.2", "From 4.0.2 StarRocks shows an MV definition as written, so its text is compared"
)
VIEW_TEXT_REWRITTEN_BEFORE_4_0_7 = testing.skip_if(
    "starrocks<4.0.7",
    "Before 4.0.7 StarRocks stores a view's SQL rewritten, and the backslash of an escape such as \\t is lost",
)
VIEW_TEXT_NOT_COMPARED_BEFORE_4_0_7 = testing.skip_if(
    "starrocks<4.0.7", "Before 4.0.7 StarRocks keeps a view's SQL rewritten, so its text is not compared"
)
VIEW_TEXT_COMPARED_FROM_4_0_7 = testing.skip_if(
    "starrocks>=4.0.7", "From 4.0.7 StarRocks keeps a view's SQL as written, so its text is compared"
)
