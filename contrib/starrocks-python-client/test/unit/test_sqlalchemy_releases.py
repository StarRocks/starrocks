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

"""What the dialect takes from the SQLAlchemy release it runs on, which can be any 1.4 or 2.0 release."""

import os
import subprocess
import sys

import pymysql
from sqlalchemy import testing
from sqlalchemy.dialects.mysql.base import MySQLIdentifierPreparer
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

import starrocks
from starrocks.dialect import StarRocksDialect
from starrocks.reserved_words import STARROCKS_RESERVED_WORDS


class ReservedWordsTest(fixtures.TestBase):
    def test_the_reserved_words_are_mysqls_and_starrocks(self):
        """Every word the MySQL preparer of this SQLAlchemy release reserves, and every word StarRocks reserves."""
        reserved = StarRocksDialect().identifier_preparer.reserved_words
        eq_(sorted(MySQLIdentifierPreparer.reserved_words - reserved), [])
        eq_(sorted(STARROCKS_RESERVED_WORDS - reserved), [])

    @testing.combinations(
        ("starrocks_only", "files", "`files`"),
        ("starrocks_only_type", "text", "`text`"),
        ("mysql_only", "accessible", "`accessible`"),
        ("both", "select", "`select`"),
        ("neither", "name", "name"),
        id_="iaa",
        argnames="word, quoted",
    )
    def test_a_reserved_word_is_backquoted(self, word, quoted):
        eq_(StarRocksDialect().identifier_preparer.quote(word), quoted)

    def test_the_dialect_imports_where_sqlalchemy_has_no_reserved_words_module(self):
        """SQLAlchemy has ``sqlalchemy.dialects.mysql.reserved_words`` only from 1.4.27, and the dialect needs 1.4."""
        script = (
            "import sys\n"
            "import sqlalchemy.dialects.mysql.base\n"
            "sys.modules['sqlalchemy.dialects.mysql.reserved_words'] = None\n"
            "from starrocks.dialect import StarRocksDialect\n"
            "print('files' in StarRocksDialect().identifier_preparer.reserved_words)\n"
        )
        client_dir = os.path.dirname(os.path.dirname(os.path.abspath(starrocks.__file__)))
        python_path = os.pathsep.join(filter(None, [client_dir, os.environ.get("PYTHONPATH")]))
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONPATH": python_path},
            timeout=120,
        )
        eq_((result.returncode, result.stdout.strip()), (0, "True"), result.stderr[-2000:])


class _DialectWithoutGetDriverConnection(StarRocksDialect):
    """The dialect as SQLAlchemy 1.4.0 to 1.4.23 builds it, which has no ``Dialect.get_driver_connection``."""

    @property
    def get_driver_connection(self):
        raise AttributeError("get_driver_connection")


class DriverConnectionTest(fixtures.TestBase):
    @testing.combinations(
        ("autocommit_on", True, "AUTOCOMMIT"),
        ("autocommit_off", False, "READ COMMITTED"),
        ("server_default", None, "AUTOCOMMIT"),
        id_="iaa",
        argnames="autocommit, level",
    )
    def test_the_isolation_level_is_read_where_sqlalchemy_has_no_get_driver_connection(self, autocommit, level):
        """SQLAlchemy has ``Dialect.get_driver_connection`` only from 1.4.24, and the dialect needs 1.4."""
        connection = pymysql.connections.Connection(defer_connect=True, autocommit=autocommit)
        eq_(_DialectWithoutGetDriverConnection().get_isolation_level(connection), level)
