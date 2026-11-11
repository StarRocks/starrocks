#!/usr/bin/env python3
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

"""Register a Paimon fixture in HMS without requiring HiveServer2's Paimon jar.

Only the fields needed for an external table are written. Field IDs come from
Hive's metastore Thrift API; no generated client or extra dependency is needed.
The external table points at existing fixture data and drop never deletes data.
"""

import argparse
from urllib.parse import urlparse

from thrift.Thrift import TApplicationException, TMessageType, TType
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport


def _write_value(protocol, kind, value):
    if kind == TType.STRING:
        protocol.writeString(value)
    elif kind == TType.BOOL:
        protocol.writeBool(value)
    elif kind == TType.I32:
        protocol.writeI32(value)
    elif kind == TType.STRUCT:
        _write_struct(protocol, value)
    elif kind == TType.LIST:
        element_kind, values = value
        protocol.writeListBegin(element_kind, len(values))
        for item in values:
            _write_value(protocol, element_kind, item)
        protocol.writeListEnd()
    elif kind == TType.MAP:
        key_kind, value_kind, values = value
        protocol.writeMapBegin(key_kind, value_kind, len(values))
        for key, item in values.items():
            _write_value(protocol, key_kind, key)
            _write_value(protocol, value_kind, item)
        protocol.writeMapEnd()
    else:
        raise ValueError('unsupported Thrift type %s' % kind)


def _write_struct(protocol, fields):
    protocol.writeStructBegin('')
    for field_id, kind, value in fields:
        protocol.writeFieldBegin('', kind, field_id)
        _write_value(protocol, kind, value)
        protocol.writeFieldEnd()
    protocol.writeFieldStop()
    protocol.writeStructEnd()


def _exception_message(protocol):
    message = ''
    protocol.readStructBegin()
    while True:
        _, kind, field_id = protocol.readFieldBegin()
        if kind == TType.STOP:
            break
        if field_id == 1 and kind == TType.STRING:
            message = protocol.readString()
        else:
            protocol.skip(kind)
        protocol.readFieldEnd()
    protocol.readStructEnd()
    return message


def _call(uri, method, fields, allow_missing=False):
    address = urlparse(uri.split(',')[0])
    if address.scheme != 'thrift' or not address.hostname or not address.port:
        raise ValueError('expected a thrift://host:port Hive metastore URI')
    socket = TSocket.TSocket(address.hostname, address.port)
    socket.setTimeout(10000)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    transport.open()
    try:
        protocol.writeMessageBegin(method, TMessageType.CALL, 0)
        _write_struct(protocol, fields)
        protocol.writeMessageEnd()
        transport.flush()
        name, kind, sequence = protocol.readMessageBegin()
        if kind == TMessageType.EXCEPTION:
            error = TApplicationException()
            error.read(protocol)
            protocol.readMessageEnd()
            raise error
        if name != method or sequence != 0 or kind != TMessageType.REPLY:
            raise RuntimeError('unexpected Hive metastore response')
        protocol.readStructBegin()
        _, result_kind, field_id = protocol.readFieldBegin()
        if result_kind != TType.STOP:
            message = _exception_message(protocol) if result_kind == TType.STRUCT else ''
            if result_kind != TType.STRUCT:
                protocol.skip(result_kind)
            protocol.readFieldEnd()
            if not (allow_missing and field_id == 1):
                raise RuntimeError('Hive metastore %s failed (exception field %s): %s' %
                                   (method, field_id, message))
            _, result_kind, _ = protocol.readFieldBegin()
        if result_kind != TType.STOP:
            raise RuntimeError('unexpected Hive metastore result')
        protocol.readStructEnd()
        protocol.readMessageEnd()
    finally:
        transport.close()


def register(uri, database, table, location):
    fields = [
        [(1, TType.STRING, 'id'), (2, TType.STRING, 'bigint')],
        [(1, TType.STRING, 'amount'), (2, TType.STRING, 'int')],
        [(1, TType.STRING, 'note'), (2, TType.STRING, 'string')],
    ]
    empty_map = (TType.STRING, TType.STRING, {})
    serde = [(2, TType.STRING, 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'),
             (3, TType.MAP, empty_map)]
    storage = [
        (1, TType.LIST, (TType.STRUCT, fields)),
        (2, TType.STRING, location),
        (3, TType.STRING, 'org.apache.paimon.hive.mapred.PaimonInputFormat'),
        (4, TType.STRING, 'org.apache.paimon.hive.mapred.PaimonOutputFormat'),
        (5, TType.BOOL, False),
        (6, TType.I32, -1),
        (7, TType.STRUCT, serde),
        (8, TType.LIST, (TType.STRING, [])),
        (9, TType.LIST, (TType.STRUCT, [])),
        (10, TType.MAP, empty_map),
    ]
    hive_table = [
        (1, TType.STRING, table), (2, TType.STRING, database),
        (3, TType.STRING, 'hive'), (4, TType.I32, 0), (5, TType.I32, 0),
        (6, TType.I32, 0), (7, TType.STRUCT, storage),
        (8, TType.LIST, (TType.STRUCT, [])),
        (9, TType.MAP, (TType.STRING, TType.STRING,
                        {'EXTERNAL': 'TRUE', 'external.table.purge': 'false', 'table_type': 'PAIMON'})),
        (12, TType.STRING, 'EXTERNAL_TABLE'),
    ]
    _call(uri, 'create_table', [(1, TType.STRUCT, hive_table)])


def drop(uri, database, table):
    # The third parameter is deleteData: the shared fixture must never be deleted.
    _call(uri, 'drop_table', [(1, TType.STRING, database), (2, TType.STRING, table),
                              (3, TType.BOOL, False)], allow_missing=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('register', 'drop'))
    parser.add_argument('uri')
    parser.add_argument('database')
    parser.add_argument('table')
    parser.add_argument('location', nargs='?')
    args = parser.parse_args()
    if args.action == 'register':
        if not args.location:
            parser.error('register requires a fixture location')
        register(args.uri, args.database, args.table, args.location)
    else:
        drop(args.uri, args.database, args.table)
