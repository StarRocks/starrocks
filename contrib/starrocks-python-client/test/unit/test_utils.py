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
