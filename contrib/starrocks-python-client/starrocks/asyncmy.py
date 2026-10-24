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

from typing import Any, Dict, Optional, Union

from sqlalchemy import pool, util
from sqlalchemy.connectors.asyncio import AsyncIODBAPIConnection
from sqlalchemy.dialects.mysql.asyncmy import AsyncAdapt_asyncmy_dbapi, AsyncAdapt_asyncmy_ss_cursor
from sqlalchemy.engine.interfaces import DBAPIConnection, DBAPICursor, DBAPIModule, PoolProxiedConnection
from sqlalchemy.engine.url import URL

from .dialect import StarRocksDialect


class _HexLiteralBytes:
    """Bytes bound through asyncmy, which the connection's encoder writes as the hex literal ``X'...'``."""

    __slots__ = ("value",)

    def __init__(self, value: Union[bytes, bytearray, memoryview]) -> None:
        self.value = bytes(value)

    def __bytes__(self) -> bytes:
        return self.value

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.value!r})"

    def __str__(self) -> str:
        """Refuse to become text, which asyncmy would bind as a string when its connection lacks the encoder."""
        raise TypeError(
            "this asyncmy connection has no encoder for binary values; a connection made by async_creator "
            "must be given conv=starrocks.asyncmy.hex_literal_conversions(asyncmy.converters.conversions)"
        )


def _escape_hex_literal(value: _HexLiteralBytes, mapping: Optional[Dict[Any, Any]] = None) -> str:
    return "X'%s'" % value.value.hex()


def hex_literal_conversions(conversions: Dict[Any, Any]) -> Dict[Any, Any]:
    """Return asyncmy ``conversions`` with the encoder that writes a bound binary value as ``X'...'``."""
    return {**conversions, _HexLiteralBytes: _escape_hex_literal}


class AsyncAdapt_starrocks_asyncmy_dbapi(AsyncAdapt_asyncmy_dbapi):
    """asyncmy's DBAPI, binding bytes as the hex literal ``X'...'``, as PyMySQL does.

    asyncmy writes a bytes parameter as ``_binary'...'``, which StarRocks refuses, so no LargeBinary, BINARY or
    VARBINARY value could be bound. ``Binary``, which SQLAlchemy's binary types bind through, wraps the bytes,
    and each connection is given an encoder for the wrapper.
    """

    def Binary(self, value: Union[bytes, bytearray, memoryview]) -> _HexLiteralBytes:
        return _HexLiteralBytes(value)

    def connect(self, *arg: Any, **kw: Any) -> Any:
        """Connect with the hex encoder added to the connection's conversions; ``async_creator`` makes its own."""
        if "async_creator_fn" not in kw:
            kw["conv"] = hex_literal_conversions(kw.get("conv") or self.asyncmy.converters.conversions)
        return super().connect(*arg, **kw)


class StarRocksDialect_asyncmy(StarRocksDialect):
    driver = "asyncmy"
    supports_statement_cache = True

    supports_server_side_cursors = True
    _sscursor = AsyncAdapt_asyncmy_ss_cursor

    is_async = True
    has_terminate = True

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        return AsyncAdapt_starrocks_asyncmy_dbapi(__import__("asyncmy"))

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
