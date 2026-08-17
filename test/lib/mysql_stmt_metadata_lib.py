#!/usr/bin/env python
# -- coding: utf-8 --
###########################################################################
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
###########################################################################

import hashlib
import socket
import struct

from dataclasses import dataclass
from typing import List, Optional, Tuple

CLIENT_LONG_PASSWORD = 0x00000001
CLIENT_LONG_FLAG = 0x00000004
CLIENT_CONNECT_WITH_DB = 0x00000008
CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_TRANSACTIONS = 0x00002000
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_MULTI_RESULTS = 0x00020000
CLIENT_PLUGIN_AUTH = 0x00080000

COM_QUERY = 0x03
COM_STMT_PREPARE = 0x16
COM_STMT_EXECUTE = 0x17
COM_STMT_CLOSE = 0x19

TYPE_CODE_NAMES = {
    0: "DECIMAL",
    1: "TINY",
    2: "SHORT",
    3: "LONG",
    4: "FLOAT",
    5: "DOUBLE",
    6: "NULL",
    7: "TIMESTAMP",
    8: "LONGLONG",
    9: "INT24",
    10: "DATE",
    11: "TIME",
    12: "DATETIME",
    13: "YEAR",
    15: "VARCHAR",
    16: "BIT",
    245: "JSON",
    246: "NEWDECIMAL",
    247: "ENUM",
    248: "SET",
    249: "TINY_BLOB",
    250: "MEDIUM_BLOB",
    251: "LONG_BLOB",
    252: "BLOB/TEXT",
    253: "VAR_STRING",
    254: "STRING",
    255: "GEOMETRY",
}

CHARSET_MAX_BYTES = {
    8: 1,      # latin1_swedish_ci
    33: 3,     # utf8_general_ci
    45: 4,     # utf8mb4_general_ci
    46: 4,     # utf8mb4_bin
    63: 1,     # binary
    83: 3,     # utf8_bin
    192: 3,    # utf8_unicode_ci
    224: 4,    # utf8mb4_unicode_ci
    255: 4,    # utf8mb4_0900_ai_ci / compatible mappings on newer servers
}


def sha1(data: bytes) -> bytes:
    return hashlib.sha1(data).digest()


def decode_lenenc_int(data: bytes, pos: int = 0) -> Tuple[int, int]:
    first = data[pos]
    pos += 1
    if first < 0xFB:
        return first, pos
    if first == 0xFC:
        return struct.unpack_from("<H", data, pos)[0], pos + 2
    if first == 0xFD:
        return int.from_bytes(data[pos:pos + 3], "little"), pos + 3
    if first == 0xFE:
        return struct.unpack_from("<Q", data, pos)[0], pos + 8
    raise ValueError(f"Unsupported lenenc first byte: {first}")


def decode_lenenc_str(data: bytes, pos: int = 0) -> Tuple[str, int]:
    length, pos = decode_lenenc_int(data, pos)
    raw = data[pos:pos + length]
    return raw.decode("utf-8", errors="replace"), pos + length


def decode_null_terminated_str(data: bytes, pos: int = 0) -> Tuple[str, int]:
    end = data.index(0, pos)
    return data[pos:end].decode("utf-8", errors="replace"), end + 1


class MySQLStmtMetadataError(RuntimeError):
    pass


@dataclass
class ColumnDef:
    schema: str
    table: str
    org_table: str
    name: str
    org_name: str
    charset: int
    column_length: int
    type_code: int
    flags: int
    decimals: int

    @property
    def type_name(self) -> str:
        return TYPE_CODE_NAMES.get(self.type_code, f"UNKNOWN({self.type_code})")

    @property
    def normalized_length(self) -> int:
        """
        Normalize string metadata length from bytes to characters.

        For textual result columns the wire-level `column_length` is often
        reported in bytes. SQL cases care about `VARCHAR(N)` in characters, so
        the probe converts by the charset max-bytes width when the column is a
        string type. Non-string types are returned unchanged.
        """
        if self.type_name not in {"VARCHAR", "VAR_STRING", "STRING"}:
            return self.column_length
        max_bytes = CHARSET_MAX_BYTES.get(self.charset, 1)
        return self.column_length // max_bytes if max_bytes > 0 else self.column_length


class PacketReader:
    """Minimal packet reader/writer for a single MySQL connection."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.sequence_id = 0

    def reset_sequence(self) -> None:
        self.sequence_id = 0

    def _read_fully(self, n: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < n:
            chunk = self.sock.recv(n - len(chunks))
            if not chunk:
                raise ConnectionError("Connection closed while reading packet")
            chunks.extend(chunk)
        return bytes(chunks)

    def read_packet(self) -> bytes:
        header = self._read_fully(4)
        length = header[0] | (header[1] << 8) | (header[2] << 16)
        seq = header[3]
        self.sequence_id = (seq + 1) & 0xFF
        return self._read_fully(length)

    def send_packet(self, payload: bytes) -> None:
        header = struct.pack("<I", len(payload))[:3] + bytes([self.sequence_id])
        self.sock.sendall(header + payload)
        self.sequence_id = (self.sequence_id + 1) & 0xFF


class MySQLStmtMetadataClient:
    """
    A minimal MySQL-wire client used only for result-set metadata probing.

    Regular mysql drivers hide COM_STMT_PREPARE result-column metadata, so this
    helper reads the packets directly and exposes the prepare/execute lengths
    needed by the SQL test framework.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        connect_timeout: float = 5.0,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connect_timeout = connect_timeout
        self.sock: Optional[socket.socket] = None
        self.io: Optional[PacketReader] = None
        self.server_capabilities = 0
        self.auth_plugin_name = "mysql_native_password"
        self.auth_plugin_data = b""

    def connect(self) -> None:
        """Open the socket, complete handshake/auth, then keep the session open."""
        self.sock = socket.create_connection((self.host, self.port), self.connect_timeout)
        self.io = PacketReader(self.sock)
        handshake = self.io.read_packet()
        self._parse_handshake(handshake)
        self._send_handshake_response()
        auth_result = self.io.read_packet()
        self._handle_auth_result(auth_result)

    def close(self) -> None:
        if self.sock is not None:
            self.sock.close()
            self.sock = None
            self.io = None

    def query(self, sql: str) -> None:
        """
        Execute a plain COM_QUERY and drain its result.

        The probe only uses this for lightweight SET statements before the
        prepared statement is tested.
        """
        assert self.io is not None
        self.io.reset_sequence()
        self.io.send_packet(bytes([COM_QUERY]) + sql.encode("utf-8"))
        first = self.io.read_packet()
        self._raise_if_error(first)
        if first[0] == 0x00:
            return
        column_count, _ = decode_lenenc_int(first, 0)
        self._read_column_defs(column_count)
        self._drain_result_rows()

    def prepare(self, sql: str) -> Tuple[int, int, List[ColumnDef]]:
        """Send COM_STMT_PREPARE and return statement id, parameter count, and result metadata."""
        assert self.io is not None
        self.io.reset_sequence()
        self.io.send_packet(bytes([COM_STMT_PREPARE]) + sql.encode("utf-8"))
        payload = self.io.read_packet()
        self._raise_if_error(payload)
        if payload[0] != 0x00:
            raise MySQLStmtMetadataError(f"Unexpected COM_STMT_PREPARE response: {payload[0]}")
        statement_id = struct.unpack_from("<I", payload, 1)[0]
        num_columns = struct.unpack_from("<H", payload, 5)[0]
        num_params = struct.unpack_from("<H", payload, 7)[0]
        if num_params:
            self._read_column_defs(num_params)
        columns = self._read_column_defs(num_columns)
        return statement_id, num_params, columns

    def execute_and_fetch_metadata(self, statement_id: int, num_params: int) -> List[ColumnDef]:
        """
        Execute a prepared statement and return the result-column metadata.

        Current SQL cases only need metadata for non-parameterized statements,
        so parameter binding is intentionally left unsupported here.
        """
        assert self.io is not None
        if num_params:
            raise MySQLStmtMetadataError("Metadata probe currently supports only non-parameterized statements")
        payload = bytearray()
        payload.append(COM_STMT_EXECUTE)
        payload.extend(struct.pack("<I", statement_id))
        payload.append(0)
        payload.extend(struct.pack("<I", 1))
        self.io.reset_sequence()
        self.io.send_packet(bytes(payload))
        first = self.io.read_packet()
        self._raise_if_error(first)
        if first[0] == 0x00:
            return []
        column_count, _ = decode_lenenc_int(first, 0)
        columns = self._read_column_defs(column_count)
        self._drain_result_rows()
        return columns

    def close_statement(self, statement_id: int) -> None:
        """Explicitly close the server-side prepared statement handle."""
        assert self.io is not None
        self.io.reset_sequence()
        self.io.send_packet(bytes([COM_STMT_CLOSE]) + struct.pack("<I", statement_id))

    def _parse_handshake(self, payload: bytes) -> None:
        pos = 0
        protocol = payload[pos]
        if protocol != 10:
            raise MySQLStmtMetadataError(f"Unsupported protocol version: {protocol}")
        pos += 1
        _, pos = decode_null_terminated_str(payload, pos)
        pos += 4
        part1 = payload[pos:pos + 8]
        pos += 8
        pos += 1
        capability_low = struct.unpack_from("<H", payload, pos)[0]
        pos += 2
        pos += 1
        pos += 2
        capability_high = struct.unpack_from("<H", payload, pos)[0]
        pos += 2
        self.server_capabilities = capability_low | (capability_high << 16)
        auth_len = payload[pos]
        pos += 1
        pos += 10
        part2_len = max(13, auth_len - 8) if (self.server_capabilities & CLIENT_SECURE_CONNECTION) else 0
        part2 = payload[pos:pos + part2_len]
        pos += part2_len
        self.auth_plugin_data = part1 + part2.rstrip(b"\x00")
        if self.server_capabilities & CLIENT_PLUGIN_AUTH and pos < len(payload):
            self.auth_plugin_name, pos = decode_null_terminated_str(payload, pos)

    def _send_handshake_response(self) -> None:
        assert self.io is not None
        capabilities = (
            CLIENT_LONG_PASSWORD
            | CLIENT_LONG_FLAG
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH
            | CLIENT_MULTI_RESULTS
        )
        if self.database:
            capabilities |= CLIENT_CONNECT_WITH_DB
        capabilities &= self.server_capabilities
        payload = bytearray()
        payload.extend(struct.pack("<I", capabilities))
        payload.extend(struct.pack("<I", 1024 * 1024 * 1024 - 1))
        payload.append(33)
        payload.extend(b"\x00" * 23)
        payload.extend(self.user.encode("utf-8") + b"\x00")
        auth_response = self._build_auth_response(self.auth_plugin_name)
        payload.append(len(auth_response))
        payload.extend(auth_response)
        if self.database:
            payload.extend(self.database.encode("utf-8") + b"\x00")
        if capabilities & CLIENT_PLUGIN_AUTH:
            payload.extend(self.auth_plugin_name.encode("utf-8") + b"\x00")
        self.io.send_packet(bytes(payload))

    def _build_auth_response(self, plugin_name: str) -> bytes:
        if not self.password:
            return b""
        if plugin_name == "mysql_native_password":
            stage1 = sha1(self.password.encode("utf-8"))
            stage2 = sha1(stage1)
            scramble = sha1(self.auth_plugin_data + stage2)
            return bytes(a ^ b for a, b in zip(stage1, scramble))
        if plugin_name == "caching_sha2_password":
            stage1 = sha1(self.password.encode("utf-8"))
            stage2 = sha1(stage1)
            scramble = sha1(stage2 + self.auth_plugin_data)
            return bytes(a ^ b for a, b in zip(stage1, scramble))
        raise MySQLStmtMetadataError(f"Unsupported auth plugin: {plugin_name}")

    def _handle_auth_result(self, payload: bytes) -> None:
        assert self.io is not None
        if payload[0] == 0x00:
            return
        if payload[0] == 0xFE:
            plugin_name, pos = decode_null_terminated_str(payload, 1)
            self.auth_plugin_name = plugin_name
            self.auth_plugin_data = payload[pos:].rstrip(b"\x00")
            self.io.send_packet(self._build_auth_response(plugin_name))
            final_payload = self.io.read_packet()
            if final_payload[0] != 0x00:
                self._raise_if_error(final_payload)
            return
        self._raise_if_error(payload)

    def _read_column_defs(self, count: int) -> List[ColumnDef]:
        """Read `count` column-definition packets followed by EOF."""
        columns = []
        for _ in range(count):
            payload = self.io.read_packet()
            self._raise_if_error(payload)
            columns.append(self._parse_column_def(payload))
        if count > 0:
            eof = self.io.read_packet()
            self._raise_if_error(eof)
            if not self._is_eof_packet(eof):
                raise MySQLStmtMetadataError(f"Expected EOF after column defs, got {eof[:8]!r}")
        return columns

    def _drain_result_rows(self) -> None:
        """Drain all result rows so the connection can be reused safely."""
        while True:
            payload = self.io.read_packet()
            self._raise_if_error(payload)
            if self._is_eof_packet(payload):
                return

    @staticmethod
    def _is_eof_packet(payload: bytes) -> bool:
        return len(payload) < 9 and payload[:1] == b"\xFE"

    @staticmethod
    def _parse_column_def(payload: bytes) -> ColumnDef:
        """Parse a Protocol::ColumnDefinition41 packet into a compact Python object."""
        pos = 0
        _, pos = decode_lenenc_str(payload, pos)
        schema, pos = decode_lenenc_str(payload, pos)
        table, pos = decode_lenenc_str(payload, pos)
        org_table, pos = decode_lenenc_str(payload, pos)
        name, pos = decode_lenenc_str(payload, pos)
        org_name, pos = decode_lenenc_str(payload, pos)
        _, pos = decode_lenenc_int(payload, pos)
        charset = struct.unpack_from("<H", payload, pos)[0]
        pos += 2
        column_length = struct.unpack_from("<I", payload, pos)[0]
        pos += 4
        type_code = payload[pos]
        pos += 1
        flags = struct.unpack_from("<H", payload, pos)[0]
        pos += 2
        decimals = payload[pos]
        return ColumnDef(schema, table, org_table, name, org_name, charset, column_length, type_code, flags, decimals)

    @staticmethod
    def _raise_if_error(payload: bytes) -> None:
        """Turn MySQL ERR packets into Python exceptions with code/state/message."""
        if payload[:1] == b"\xFF":
            code = struct.unpack_from("<H", payload, 1)[0]
            if len(payload) >= 9 and payload[3:4] == b"#":
                sql_state = payload[4:9].decode("ascii", errors="replace")
                message = payload[9:].decode("utf-8", errors="replace")
            else:
                sql_state = "HY000"
                message = payload[3:].decode("utf-8", errors="replace")
            raise MySQLStmtMetadataError(f"{code} [{sql_state}] {message}")
