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
