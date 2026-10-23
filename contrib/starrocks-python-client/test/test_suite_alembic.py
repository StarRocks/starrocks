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
    AutogenerateFKOptionsTest as _AutogenerateFKOptionsTest,
    AutoincrementTest as _AutoincrementTest,
    BackendAlterColumnTest as _BackendAlterColumnTest,
)
from sqlalchemy import testing
from sqlalchemy.testing.fixtures import TestBase


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
