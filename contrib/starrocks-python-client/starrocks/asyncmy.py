# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from typing import Optional, Union

from sqlalchemy.connectors.asyncio import AsyncIODBAPIConnection
from sqlalchemy.dialects.mysql.asyncmy import AsyncAdapt_asyncmy_dbapi, AsyncAdapt_asyncmy_ss_cursor
from sqlalchemy.engine.interfaces import DBAPIConnection, DBAPICursor, DBAPIModule, PoolProxiedConnection
from sqlalchemy.engine.url import URL
from sqlalchemy import pool, util

from .dialect import StarRocksDialect


class StarRocksDialect_asyncmy(StarRocksDialect):
    driver = "asyncmy"
    supports_statement_cache = True

    supports_server_side_cursors = True
    _sscursor = AsyncAdapt_asyncmy_ss_cursor

    is_async = True
    has_terminate = True

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        return AsyncAdapt_asyncmy_dbapi(__import__("asyncmy"))

    @classmethod
    def get_pool_class(cls, url: URL) -> type:
        async_fallback = url.query.get("async_fallback", False)

        if util.asbool(async_fallback):
            return pool.FallbackAsyncAdaptedQueuePool
        else:
            return pool.AsyncAdaptedQueuePool

    def do_terminate(self, dbapi_connection: DBAPIConnection) -> None:
        dbapi_connection.terminate()

    def create_connect_args(self, url: URL) -> ConnectArgsType:  # type: ignore[override]  # noqa: E501
        return super().create_connect_args(
            url, _translate_args=dict(username="user", database="db")
        )

    def is_disconnect(
        self,
        e: DBAPIModule.Error,
        connection: Optional[Union[PoolProxiedConnection, DBAPIConnection]],
        cursor: Optional[DBAPICursor],
    ) -> bool:
        if super().is_disconnect(e, connection, cursor):
            return True
        else:
            str_e = str(e).lower()
            return (
                "not connected" in str_e or "network operation failed" in str_e
            )

    def _found_rows_client_flag(self) -> int:
        from asyncmy.constants import CLIENT  # type: ignore

        return CLIENT.FOUND_ROWS  # type: ignore[no-any-return]

    def get_driver_connection(
        self, connection: DBAPIConnection
    ) -> AsyncIODBAPIConnection:
        return connection._connection  # type: ignore[no-any-return]


dialect = StarRocksDialect_asyncmy
