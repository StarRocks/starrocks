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

import logging
import time
from typing import Any, Optional, Tuple

from alembic.ddl import base as alembic_base
from alembic.ddl.mysql import MySQLImpl
from sqlalchemy import Column, MetaData, Table, text

from starrocks import datatype
from starrocks.alembic import compare
from starrocks.datatype import BIGINT, VARCHAR
from starrocks.sql.ddl import AlterTableColumns


logger = logging.getLogger(__name__)

# DDL constructs that submit an asynchronous StarRocks column schema-change job.
# StarRocks allows only one in-flight schema-change job per table, so when the
# wait feature is enabled we poll for a terminal state after each of these.
_SCHEMA_CHANGE_CONSTRUCTS = (
    AlterTableColumns,
    alembic_base.AddColumn,
    alembic_base.DropColumn,
    alembic_base.AlterColumn,  # covers Column{Nullable,Type,Default,Comment} + MySQL modify/change
)

# Terminal states reported by ``SHOW ALTER TABLE COLUMN``.
_SCHEMA_CHANGE_FINISHED = "FINISHED"
_SCHEMA_CHANGE_CANCELLED = "CANCELLED"


class StarRocksImpl(MySQLImpl):
    """Alembic DDL implementation for StarRocks."""

    __dialect__ = "starrocks"

    def version_table_impl(
        self,
        *,
        version_table: str,
        version_table_schema: Optional[str],
        version_table_pk: bool,  # ignored as StarRocks requires a primary key
        **kw,
    ) -> Table:
        version_table_kwargs = self.context_opts.get("version_table_kwargs", {}) if self.context_opts else {}
        if version_table_kwargs:
            logger.info(f"There are extra kwargs for version_table: {version_table_kwargs}")
        return Table(
            version_table,
            MetaData(),
            Column("id", BIGINT, autoincrement=True, primary_key=True),
            Column("version_num", VARCHAR(32), primary_key=False),
            schema=version_table_schema,
            starrocks_primary_key="id",
            **version_table_kwargs,
            **kw,
        )

    def _exec(self, construct, *args, **kw):
        """Execute DDL, then optionally block until a column schema change finishes.

        StarRocks runs column schema changes asynchronously and rejects a new
        job while a prior one on the same table is still running. When the user
        opts in via ``context.configure(starrocks_wait_for_schema_change=True)``,
        we poll ``SHOW ALTER TABLE COLUMN`` after each column-altering statement
        until it reaches a terminal state, keeping the migration in lock-step
        with the cluster.
        """
        result = super()._exec(construct, *args, **kw)

        target = self._schema_change_target(construct)
        if target is not None and self._wait_for_schema_change_enabled():
            self._wait_for_schema_change(*target)

        return result

    def _wait_for_schema_change_enabled(self) -> bool:
        """Whether to block on column schema changes (opt-in, online mode only)."""
        if self.as_sql or self.connection is None:
            # Offline (--sql) mode has no live connection to poll.
            return False
        opts = self.context_opts or {}
        return bool(opts.get("starrocks_wait_for_schema_change", False))

    @staticmethod
    def _schema_change_target(construct: Any) -> Optional[Tuple[str, Optional[str]]]:
        """Return (table_name, schema) if the construct triggers a column schema change."""
        if isinstance(construct, _SCHEMA_CHANGE_CONSTRUCTS):
            return getattr(construct, "table_name", None), getattr(construct, "schema", None)
        return None

    def _wait_for_schema_change(self, table_name: str, schema: Optional[str]) -> None:
        """Poll ``SHOW ALTER TABLE COLUMN`` until the latest job reaches a terminal state.

        Raises:
            RuntimeError: if the schema change is CANCELLED or the timeout elapses.
        """
        opts = self.context_opts or {}
        poll_interval = float(opts.get("starrocks_schema_change_poll_interval", 2.0))
        # None / 0 means wait indefinitely.
        timeout = opts.get("starrocks_schema_change_timeout", None)

        from_clause = f"FROM `{schema}` " if schema else ""
        query = text(
            f"SHOW ALTER TABLE COLUMN {from_clause}"
            "WHERE TableName = :table_name ORDER BY CreateTime DESC LIMIT 1"
        )

        deadline = None if not timeout else time.monotonic() + float(timeout)
        while True:
            row = self.connection.execute(
                query, {"table_name": table_name}
            ).mappings().first()

            # No job row (e.g. a metadata-only change) => nothing to wait for.
            state = (row.get("State") if row else None)
            if state is None:
                return

            state = str(state).upper()
            if state == _SCHEMA_CHANGE_FINISHED:
                logger.debug("Schema change for %s finished.", table_name)
                return
            if state == _SCHEMA_CHANGE_CANCELLED:
                msg = row.get("Msg") if row else ""
                raise RuntimeError(
                    f"StarRocks schema change for table '{table_name}' was CANCELLED: {msg}"
                )

            if deadline is not None and time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Timed out after {timeout}s waiting for the schema change on "
                    f"table '{table_name}' to finish (last state: {state})."
                )

            logger.info(
                "Waiting for StarRocks schema change on table '%s' (state: %s)...",
                table_name, state,
            )
            time.sleep(poll_interval)

    def compare_type(self, inspector_column: Column[Any], metadata_column: Column[Any]) -> bool:
        """
        Set StarRocks' specific type comparison logic for some special cases.

        For some special cases:
            - complex type comparison: ARRAY, MAP, STRUCT
            - simple type comparison:
                - meta.BOOLEAN equals to conn.TINYINT(1)
                - meta.STRING equals to conn.VARCHAR(65533)

        Args:
            inspector_column: The column from the inspector.
            metadata_column: The column from the metadata.

        Returns:
            True if the types are different, False if the types are the same.
        """
        inspector_type = inspector_column.type
        metadata_type = metadata_column.type

        # Handle complex type comparison.
        if isinstance(metadata_type, datatype.StructuredType):
            # If the inspector found a different base type, they are different.
            is_different = False
            if not isinstance(inspector_type, type(metadata_type)):
                is_different = True
            else:
                # Perform deep, recursive comparison.
                # Returns True if different, False if same.
                is_different = compare.compare_complex_type(self, inspector_type, metadata_type)
            if is_different:
                table: Optional[Table] = metadata_column.table
                table_info_msg = f" of table: {table.name}, schema: {table.schema}," if table is not None else ""
                logger.warning(f"Detected type change{table_info_msg} from inspector_type: {inspector_type!r} to metadata_type: {metadata_type!r}. "
                            f"But, StarRocks does not support schema change for complex type columns. "
                            f"You should check and change the metadata carefully to make sure there is no type difference for this column. "
                            )
            return is_different

        return compare.compare_simple_type(self, inspector_column, metadata_column)
