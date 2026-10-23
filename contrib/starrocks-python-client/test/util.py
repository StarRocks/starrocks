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

import re

from alembic import util as alembic_util
from sqlalchemy import testing


STRUCT_FIELDS_BACKTICKED = testing.fails_on(
    "starrocks>=4.1",
    "StarRocks 4.1 reports STRUCT fields with backticked names, which the type parser rejects; reflecting the "
    "column warns, which the testing plugin raises as an error, and leaves it NullType with no result processor",
)
TOLERATE_UNPARSED_STRUCT_REFLECTION = testing.emits_warning(r"Did not recognize type 'struct<")
"""For tests whose checks on other columns hold while STRUCT_FIELDS_BACKTICKED leaves a STRUCT column NullType.

Their STRUCT columns pass without being compared; the tests marked STRUCT_FIELDS_BACKTICKED track that bug.
"""


def alembic_runs_dialect_comparators_first(config: object) -> bool:
    """Whether Alembic runs the comparators in its global registry ahead of its own.

    Alembic's PriorityDispatcher (1.18 and later) copies the global registry, which holds the dialect's
    comparators, before adding its own, so the dialect's ``schema`` and ``table`` comparators run first.
    """
    return hasattr(alembic_util, "PriorityDispatcher")


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
