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

from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.testing import suite as alembic_suite
from alembic.testing.suite import *  # noqa: F403
from alembic.testing.suite import (
    AutogenerateFKOptionsTest as _AutogenerateFKOptionsTest,
    AutoincrementTest as _AutoincrementTest,
    BackendAlterColumnTest as _BackendAlterColumnTest,
)
from sqlalchemy import testing
from sqlalchemy.testing.fixtures import TestBase


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
AUTOINCREMENT_LEFT_OFF_UNLESS_IT_CHANGES = testing.skip(
    "starrocks",
    "StarRocks can't add or remove AUTO_INCREMENT, and from 4.1.3 takes a MODIFY COLUMN of an AUTO_INCREMENT column "
    "only when it changes nothing but the comment, so the dialect leaves autoincrement off an alter unless the model "
    "declares one the database lacks; upstream expects it on the alter",
)


class AutogenerateFKOptionsTest(_AutogenerateFKOptionsTest):
    """Upstream gates every test but ``test_nochange_ondelete`` on FK reflection, which StarRocks lacks.

    The gate goes on that one test rather than the class: SQLAlchemy 2.0 drops a ``__backend__`` class whose
    requirements fail without reporting its tests as skipped.
    """

    test_nochange_ondelete = testing.requires.foreign_key_constraint_reflection(
        _AutogenerateFKOptionsTest.test_nochange_ondelete
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


class AutoincrementTest(_AutoincrementTest):
    test_alter_column_autoincrement_pk_explicit_true = AUTO_INCREMENT_IS_BIGINT(
        _AutoincrementTest.test_alter_column_autoincrement_pk_explicit_true
    )
    test_alter_column_autoincrement_pk_false = AUTOINCREMENT_LEFT_OFF_UNLESS_IT_CHANGES(
        _AutoincrementTest.test_alter_column_autoincrement_pk_false
    )
    test_alter_column_autoincrement_pk_implicit_true = AUTOINCREMENT_LEFT_OFF_UNLESS_IT_CHANGES(
        _AutoincrementTest.test_alter_column_autoincrement_pk_implicit_true
    )
    test_alter_column_autoincrement_nonpk_false = AUTOINCREMENT_LEFT_OFF_UNLESS_IT_CHANGES(
        _AutoincrementTest.test_alter_column_autoincrement_nonpk_false
    )
    test_alter_column_autoincrement_nonpk_explicit_true = AUTOINCREMENT_LEFT_OFF_UNLESS_IT_CHANGES(
        _AutoincrementTest.test_alter_column_autoincrement_nonpk_explicit_true
    )
    test_alter_column_autoincrement_compositepk_false = AUTOINCREMENT_LEFT_OFF_UNLESS_IT_CHANGES(
        _AutoincrementTest.test_alter_column_autoincrement_compositepk_false
    )


class BackendAlterColumnTest(_BackendAlterColumnTest):
    def setUp(self):
        """Wait for each schema change to end before the test reads the table back.

        StarRocks runs a column change as an asynchronous job, so without the wait the test can reflect the
        table as it was.
        """
        super().setUp()
        self.ctx = MigrationContext.configure(self.conn, opts={"starrocks_wait_for_schema_change": True})
        self.op = Operations(self.ctx)

    test_add_server_default_int = DEFAULT_CANNOT_CHANGE(_BackendAlterColumnTest.test_add_server_default_int)
    test_modify_server_default_int = UNQUOTED_DEFAULT(_BackendAlterColumnTest.test_modify_server_default_int)
    test_modify_nullable_to_non = NULLABLE_TO_NOT_NULL(_BackendAlterColumnTest.test_modify_nullable_to_non)


def _require_sqlalchemy_2(namespace: dict[str, object]) -> None:
    """Make every suite class require SQLAlchemy 2.0, which the dialect's Alembic integration depends on."""
    for value in list(namespace.values()):
        if isinstance(value, type) and issubclass(value, TestBase) and value.__name__.endswith("Test"):
            value.__requires__ = (*getattr(value, "__requires__", ()), "sqlalchemy_2")


_require_sqlalchemy_2(globals())
