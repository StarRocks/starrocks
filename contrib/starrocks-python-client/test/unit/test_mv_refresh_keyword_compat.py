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

import pytest
from sqlalchemy import MetaData
from sqlalchemy.schema import CreateTable

from starrocks.common.utils import is_untimed_async_refresh, render_refresh_for_server
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterMaterializedView
from starrocks.sql.schema import MaterializedView


V26_2 = (26, 2, 0)
V4_1 = (4, 1, 1)
V3_5 = (3, 5, 12)


def _dialect(server_version_info):
    dialect = StarRocksDialect()
    dialect.server_version_info = server_version_info
    return dialect


def _create_ddl(refresh, server_version_info):
    mv = MaterializedView(
        "mv1", MetaData(), definition="SELECT k FROM t",
        starrocks_distributed_by="HASH(k)", starrocks_refresh=refresh)
    return str(CreateTable(mv).compile(dialect=_dialect(server_version_info)))


def _alter_ddl(refresh, server_version_info):
    return str(AlterMaterializedView("mv1", refresh=refresh)
               .compile(dialect=_dialect(server_version_info)))


class TestRenderRefreshForServer:
    @pytest.mark.parametrize(
        "refresh, expected",
        [
            ("ASYNC", "ON_CHANGE"),
            ("IMMEDIATE ASYNC", "IMMEDIATE ON_CHANGE"),
            ("DEFERRED ASYNC", "DEFERRED ON_CHANGE"),
            ("async", "ON_CHANGE"),
            ("ON_CHANGE", "ON_CHANGE"),
        ],
    )
    def test_untimed_async_becomes_on_change_from_26_2(self, refresh, expected):
        assert render_refresh_for_server(refresh, V26_2) == expected

    @pytest.mark.parametrize(
        "refresh, expected",
        [
            ("ON_CHANGE", "ASYNC"),
            ("DEFERRED ON_CHANGE", "DEFERRED ASYNC"),
            ("on_change", "ASYNC"),
            ("ASYNC", "ASYNC"),
        ],
    )
    @pytest.mark.parametrize("version", [V4_1, V3_5])
    def test_on_change_becomes_async_before_26_2(self, refresh, expected, version):
        assert render_refresh_for_server(refresh, version) == expected

    @pytest.mark.parametrize(
        "refresh",
        [
            # The timed mode keeps the same spelling on every release; rewriting it here would
            # silently turn a scheduled view into an on-change one.
            "ASYNC EVERY(INTERVAL 1 HOUR)",
            "ASYNC START('2026-01-01 00:00:00') EVERY(INTERVAL 1 DAY)",
            "DEFERRED ASYNC EVERY(INTERVAL 1 HOUR)",
            "SCHEDULE EVERY(INTERVAL 1 HOUR)",
            "MANUAL",
            "DEFERRED MANUAL",
            "INCREMENTAL",
        ],
    )
    @pytest.mark.parametrize("version", [V26_2, V4_1, V3_5, ()])
    def test_other_modes_are_untouched(self, refresh, version):
        assert render_refresh_for_server(refresh, version) == refresh

    @pytest.mark.parametrize("refresh", ["ASYNC", "ON_CHANGE", "DEFERRED ASYNC", "MANUAL"])
    def test_unknown_server_version_keeps_the_caller_spelling(self, refresh):
        assert render_refresh_for_server(refresh, ()) == refresh

    @pytest.mark.parametrize(
        "refresh, expected",
        [
            ("ASYNC", True),
            ("DEFERRED ASYNC", True),
            ("async", True),
            ("ASYNC EVERY(INTERVAL 1 HOUR)", False),
            ("ASYNC START('2026-01-01 00:00:00') EVERY(INTERVAL 1 DAY)", False),
            ("ON_CHANGE", False),
            ("MANUAL", False),
        ],
    )
    def test_is_untimed_async_refresh(self, refresh, expected):
        assert is_untimed_async_refresh(refresh) is expected


class TestServerVersionStrings:
    """The keyword is picked from what CURRENT_VERSION() actually returns, not from a tuple."""

    @pytest.mark.parametrize(
        "current_version, expected",
        [
            ("26.2.0", "ON_CHANGE"),
            ("26.2.0-ee", "ON_CHANGE"),
            ("26.2.1-rc01", "ON_CHANGE"),
            ("26.3.0-ee", "ON_CHANGE"),
            ("4.1.1-ee", "ASYNC"),
            ("4.0.9-ee", "ASYNC"),
            ("3.5.17", "ASYNC"),
            # A build from a branch reports its branch name, which carries no version to compare
            # against, so the caller's own spelling is sent and the server reports any mismatch.
            ("main-d4df1c9", "ASYNC"),
            ("", "ASYNC"),
        ],
    )
    def test_keyword_follows_the_reported_version(self, current_version, expected):
        version_info = StarRocksDialect()._parse_server_version(current_version)
        assert render_refresh_for_server("ASYNC", version_info) == expected


class TestRefreshKeywordEmission:
    @pytest.mark.parametrize("refresh", ["ASYNC", "ON_CHANGE"])
    def test_create_targets_26_2_with_on_change(self, refresh):
        assert "REFRESH ON_CHANGE" in _create_ddl(refresh, V26_2)

    @pytest.mark.parametrize("refresh", ["ASYNC", "ON_CHANGE"])
    @pytest.mark.parametrize("version", [V4_1, V3_5])
    def test_create_targets_older_servers_with_async(self, refresh, version):
        ddl = _create_ddl(refresh, version)
        assert "REFRESH ASYNC" in ddl
        assert "ON_CHANGE" not in ddl

    def test_create_keeps_the_timed_form_on_26_2(self):
        assert "REFRESH ASYNC EVERY(INTERVAL 1 HOUR)" in _create_ddl(
            "ASYNC EVERY(INTERVAL 1 HOUR)", V26_2)

    @pytest.mark.parametrize("refresh", ["ASYNC", "ON_CHANGE"])
    def test_alter_targets_26_2_with_on_change(self, refresh):
        assert _alter_ddl(refresh, V26_2) == "ALTER MATERIALIZED VIEW mv1 REFRESH ON_CHANGE"

    @pytest.mark.parametrize("refresh", ["ASYNC", "ON_CHANGE"])
    def test_alter_targets_older_servers_with_async(self, refresh):
        assert _alter_ddl(refresh, V4_1) == "ALTER MATERIALIZED VIEW mv1 REFRESH ASYNC"

    def test_retired_spelling_warns_once_rewritten(self):
        with pytest.warns(DeprecationWarning, match="REFRESH ON_CHANGE"):
            _create_ddl("ASYNC", V26_2)

    @pytest.mark.parametrize(
        "refresh, version",
        [
            ("ON_CHANGE", V26_2),
            ("ASYNC", V4_1),
            ("ASYNC EVERY(INTERVAL 1 HOUR)", V26_2),
            ("ASYNC", ()),
        ],
    )
    def test_no_warning_when_nothing_is_rewritten(self, refresh, version, recwarn):
        _create_ddl(refresh, version)
        assert [w for w in recwarn if issubclass(w.category, DeprecationWarning)] == []
