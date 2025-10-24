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

"""Shared constants for StarRocks dialect.

This module centralizes string keys and other constants that are used across
the StarRocks SQLAlchemy dialect to avoid typo-prone string literals and to
improve discoverability.
"""

from __future__ import annotations

from typing import Final


class TableConfigKey:
    """Keys found in information_schema.tables_config rows.

    Grouping related string literals as class-level constants helps avoid
    accidental typos and makes callsites self-documenting.
    """

    TABLE_ENGINE: Final[str] = "TABLE_ENGINE"
    TABLE_MODEL: Final[str] = "TABLE_MODEL"

    PARTITION_CLAUSE: Final[str] = "PARTITION_CLAUSE"  # Added, because only PARTITION_KEY in tables_config, which is not enough.
    # PARTITION_KEY: Final[str] = "PARTITION_KEY"

    DISTRIBUTE_TYPE: Final[str] = "DISTRIBUTE_TYPE"
    DISTRIBUTE_KEY: Final[str] = "DISTRIBUTE_KEY"
    DISTRIBUTE_BUCKET: Final[str] = "DISTRIBUTE_BUCKET"

    SORT_KEY: Final[str] = "SORT_KEY"
    PROPERTIES: Final[str] = "PROPERTIES"
