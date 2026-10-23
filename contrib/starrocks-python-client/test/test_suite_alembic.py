# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The Alembic dialect suite, with StarRocks limitations skipped and known dialect bugs marked as failing.

Tests are re-bound to the upstream function wrapped in an exclusion, so upstream combinations are kept.
"""

from alembic.testing import suite as alembic_suite
from alembic.testing.suite import *  # noqa: F403
from alembic.testing.suite import (
    AutogenerateCommentsTest as _AutogenerateCommentsTest,
    AutogenerateFKOptionsTest as _AutogenerateFKOptionsTest,
    AutoincrementTest as _AutoincrementTest,
    BackendAlterColumnTest as _BackendAlterColumnTest,
)
from sqlalchemy import testing
from sqlalchemy.exc import DBAPIError
from sqlalchemy.testing.fixtures import TestBase

from starrocks.common.defaults import ReflectionTableDefaults
from starrocks.common.types import SystemRunMode
from test.util import alembic_runs_dialect_comparators_first


def _server_replication_default_differs_from_dialect_default(config: object) -> bool:
    """Whether the FE's ``default_replication_num`` differs from the default the dialect assumes for its run mode.

    Autogenerate compares a table's reflected ``replication_num`` against the dialect's hard-coded default, so
    it emits a spurious ``alter_table_properties`` only when the two differ, as on a single-replica cluster.
    Shared-data clusters are treated as matching: the dialect assumes 1 there, and this has not been checked.
    A test user that may not read the FE config is treated as matching too, so a failure there shows up.
    """
    with config.db.connect() as connection:
        run_mode = connection.dialect.run_mode
        if run_mode == SystemRunMode.SHARED_DATA:
            return False
        try:
            configured = connection.exec_driver_sql(
                "ADMIN SHOW FRONTEND CONFIG LIKE 'default_replication_num'"
            ).first()
        except DBAPIError:
            return False
    return configured[2] != ReflectionTableDefaults.properties(run_mode)["replication_num"]


def _spurious_property_alter_precedes_table_comment_op(config: object) -> bool:
    """Whether the spurious ``alter_table_properties`` op comes before Alembic's table comment op.

    Upstream's table comment tests check only the first op. The dialect's table comparator runs ahead of
    Alembic's comment comparator only where Alembic runs its global registry first.
    """
    return _server_replication_default_differs_from_dialect_default(config) and (
        alembic_runs_dialect_comparators_first(config)
    )


PROPERTY_ALTER_FROM_HARDCODED_DEFAULTS = testing.fails_if(
    _server_replication_default_differs_from_dialect_default,
    "autogenerate emits alter_table_properties for default.replication_num, comparing against a hard-coded "
    "default instead of the server's (https://github.com/StarRocks/starrocks/pull/80738)",
)
PROPERTY_ALTER_BEFORE_TABLE_COMMENT_OP = testing.fails_if(
    _spurious_property_alter_precedes_table_comment_op,
    "autogenerate emits alter_table_properties for default.replication_num ahead of the table comment op, "
    "comparing against a hard-coded default instead of the server's "
    "(https://github.com/StarRocks/starrocks/pull/80738)",
)
RENAME_COMBINED_WITH_MODIFY = testing.fails_on(
    "starrocks", "the dialect combines MODIFY COLUMN with RENAME COLUMN, which StarRocks rejects as conflicting"
)
MODIFY_KEY_COLUMN_WITHOUT_KEY = testing.fails_on(
    "starrocks", "the dialect leaves KEY out of MODIFY COLUMN for a key column, so StarRocks finds no key column left"
)
FLOAT_FIRST_COLUMN = testing.skip("starrocks", "StarRocks cannot use FLOAT as a table's first (key) column")
DEFAULT_CANNOT_CHANGE = testing.skip("starrocks", "StarRocks cannot change a column's default value")
UNQUOTED_DEFAULT = testing.skip(
    "starrocks",
    "StarRocks accepts only quoted string literals as DEFAULT values, and cannot change a column's default value",
)
NULLABLE_TO_NOT_NULL = testing.skip("starrocks", "StarRocks cannot change a column from nullable to NOT NULL")
AUTO_INCREMENT_IS_BIGINT = testing.skip(
    "starrocks",
    "StarRocks requires an AUTO_INCREMENT column to be BIGINT, so the dialect creates the INTEGER column as BIGINT "
    "and there is no type change to detect",
)


class AutogenerateFKOptionsTest(_AutogenerateFKOptionsTest):
    """Upstream gates every test but ``test_nochange_ondelete`` on FK reflection, which StarRocks lacks.

    The gate goes on that one test rather than the class: SQLAlchemy 2.0 drops a ``__backend__`` class whose
    requirements fail without reporting its tests as skipped.
    """

    test_nochange_ondelete = testing.requires.foreign_key_constraint_reflection(
        _AutogenerateFKOptionsTest.test_nochange_ondelete
    )


class AutogenerateCommentsTest(_AutogenerateCommentsTest):
    test_add_column_comment = PROPERTY_ALTER_FROM_HARDCODED_DEFAULTS(_AutogenerateCommentsTest.test_add_column_comment)
    test_add_table_comment = PROPERTY_ALTER_BEFORE_TABLE_COMMENT_OP(_AutogenerateCommentsTest.test_add_table_comment)
    test_alter_column_comment = PROPERTY_ALTER_FROM_HARDCODED_DEFAULTS(
        _AutogenerateCommentsTest.test_alter_column_comment
    )
    test_alter_table_comment = PROPERTY_ALTER_BEFORE_TABLE_COMMENT_OP(
        _AutogenerateCommentsTest.test_alter_table_comment
    )
    test_existing_column_comment_no_change = PROPERTY_ALTER_FROM_HARDCODED_DEFAULTS(
        _AutogenerateCommentsTest.test_existing_column_comment_no_change
    )
    test_existing_table_comment_no_change = PROPERTY_ALTER_FROM_HARDCODED_DEFAULTS(
        _AutogenerateCommentsTest.test_existing_table_comment_no_change
    )
    test_remove_column_comment = PROPERTY_ALTER_FROM_HARDCODED_DEFAULTS(
        _AutogenerateCommentsTest.test_remove_column_comment
    )
    test_remove_table_comment = PROPERTY_ALTER_BEFORE_TABLE_COMMENT_OP(
        _AutogenerateCommentsTest.test_remove_table_comment
    )


if hasattr(alembic_suite, "AutogenerateEmptyCommentsTest"):

    class AutogenerateEmptyCommentsTest(alembic_suite.AutogenerateEmptyCommentsTest):
        test_column_empty_comment_no_change = FLOAT_FIRST_COLUMN(
            alembic_suite.AutogenerateEmptyCommentsTest.test_column_empty_comment_no_change
        )
        test_column_empty_to_nonempty_comment = FLOAT_FIRST_COLUMN(
            alembic_suite.AutogenerateEmptyCommentsTest.test_column_empty_to_nonempty_comment
        )
        test_column_nonempty_to_empty_comment = FLOAT_FIRST_COLUMN(
            alembic_suite.AutogenerateEmptyCommentsTest.test_column_nonempty_to_empty_comment
        )
        test_table_empty_comment_no_change = PROPERTY_ALTER_FROM_HARDCODED_DEFAULTS(
            alembic_suite.AutogenerateEmptyCommentsTest.test_table_empty_comment_no_change
        )
        test_table_empty_to_nonempty_comment = PROPERTY_ALTER_FROM_HARDCODED_DEFAULTS(
            alembic_suite.AutogenerateEmptyCommentsTest.test_table_empty_to_nonempty_comment
        )
        test_table_nonempty_to_empty_comment = PROPERTY_ALTER_FROM_HARDCODED_DEFAULTS(
            alembic_suite.AutogenerateEmptyCommentsTest.test_table_nonempty_to_empty_comment
        )


class AutoincrementTest(_AutoincrementTest):
    test_alter_column_autoincrement_pk_explicit_true = AUTO_INCREMENT_IS_BIGINT(
        _AutoincrementTest.test_alter_column_autoincrement_pk_explicit_true
    )


class BackendAlterColumnTest(_BackendAlterColumnTest):
    test_add_server_default_int = DEFAULT_CANNOT_CHANGE(_BackendAlterColumnTest.test_add_server_default_int)
    test_modify_server_default_int = UNQUOTED_DEFAULT(_BackendAlterColumnTest.test_modify_server_default_int)
    test_modify_non_nullable_to_nullable = MODIFY_KEY_COLUMN_WITHOUT_KEY(
        _BackendAlterColumnTest.test_modify_non_nullable_to_nullable
    )
    test_modify_nullable_to_non = NULLABLE_TO_NOT_NULL(_BackendAlterColumnTest.test_modify_nullable_to_non)
    test_modify_type_int_str = MODIFY_KEY_COLUMN_WITHOUT_KEY(_BackendAlterColumnTest.test_modify_type_int_str)
    test_rename_column = RENAME_COMBINED_WITH_MODIFY(_BackendAlterColumnTest.test_rename_column)


def _require_sqlalchemy_2(namespace: dict[str, object]) -> None:
    """Make every suite class require SQLAlchemy 2.0, which the dialect's Alembic integration depends on."""
    for value in list(namespace.values()):
        if isinstance(value, type) and issubclass(value, TestBase) and value.__name__.endswith("Test"):
            value.__requires__ = (*getattr(value, "__requires__", ()), "sqlalchemy_2")


_require_sqlalchemy_2(globals())
