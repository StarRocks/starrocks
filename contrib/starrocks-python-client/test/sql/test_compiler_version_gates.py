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

"""Features the connected StarRocks version lacks are refused at compile time instead of failing or degrading there."""

from sqlalchemy import MetaData, exc, testing
from sqlalchemy.testing import expect_raises_message, fixtures

from starrocks.datatype import VARIANT
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterViewSecurity, CreateView
from starrocks.sql.schema import View


def _dialect_at(version):
    dialect = StarRocksDialect()
    dialect.server_version_info = version
    return dialect


def _invoker_view():
    return View("v", MetaData(), definition="SELECT 1", starrocks_security="INVOKER")


class VersionGateTest(fixtures.TestBase):
    @testing.requires.sqlalchemy_2
    def test_view_security_before_3_4_1_raises(self):
        with expect_raises_message(exc.CompileError, r"needs StarRocks 3.4.1 or later; the server is 3.4.0"):
            CreateView(_invoker_view()).compile(dialect=_dialect_at((3, 4, 0)))
        with expect_raises_message(exc.CompileError, r"View SECURITY \(view 'v'\) needs StarRocks 3.4.1"):
            AlterViewSecurity(_invoker_view(), "NONE").compile(dialect=_dialect_at((3, 3, 22)))

    def test_variant_before_4_1_raises(self):
        with expect_raises_message(exc.CompileError, "The VARIANT type needs StarRocks 4.1 or later"):
            VARIANT().compile(dialect=_dialect_at((4, 0, 17)))

    @testing.combinations(((4, 1, 6),), (None,), ((),), argnames="version")
    def test_supported_or_unknown_version_compiles(self, version):
        """An unknown version, as when compiling offline, checks nothing."""
        dialect = _dialect_at(version)
        assert VARIANT().compile(dialect=dialect) == "VARIANT"

    @testing.combinations(((3, 4, 1),), (None,), argnames="version")
    @testing.requires.sqlalchemy_2
    def test_view_security_on_supported_or_unknown_version_compiles(self, version):
        assert "SECURITY INVOKER" in str(CreateView(_invoker_view()).compile(dialect=_dialect_at(version)))
