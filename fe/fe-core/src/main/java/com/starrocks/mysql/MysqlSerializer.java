// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/MysqlSerializer.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

// used for serialize memory data to byte stream of MySQL protocol
public class MysqlSerializer {
    private final ByteArrayOutputStream out;
    private MysqlCapability capability;

    private MysqlSerializer(ByteArrayOutputStream out) {
        this(out, MysqlCapability.DEFAULT_CAPABILITY);
    }

    private MysqlSerializer(ByteArrayOutputStream out, MysqlCapability capability) {
        this.out = out;
        this.capability = capability;
    }

    public static MysqlSerializer newInstance() {
        return new MysqlSerializer(new ByteArrayOutputStream());
    }

    public static MysqlSerializer newInstance(MysqlCapability capability) {
        return new MysqlSerializer(new ByteArrayOutputStream(), capability);
    }

    // used after success handshake
    public void setCapability(MysqlCapability capability) {
        this.capability = capability;
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public void reset() {
        out.reset();
    }

    public byte[] toArray() {
        return out.toByteArray();
    }

    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(out.toByteArray());
    }

    public void writeNull() {
        MysqlCodec.writeNull(out);
    }

    public void writeByte(byte value) {
        MysqlCodec.writeByte(out, value);
    }

    public void writeBytes(byte[] value, int offset, int length) {
        MysqlCodec.writeBytes(out, value, offset, length);
    }

    public void writeBytes(byte[] value) {
        MysqlCodec.writeBytes(out, value, 0, value.length);
    }

    public void writeInt1(int value) {
        MysqlCodec.writeInt1(out, value);
    }

    public void writeInt2(int value) {
        MysqlCodec.writeInt2(out, value);
    }

    public void writeInt4(int value) {
        MysqlCodec.writeInt4(out, value);
    }

    public void writeVInt(long value) {
        MysqlCodec.writeVInt(out, value);
    }

    public void writeLenEncodedString(String value) {
        MysqlCodec.writeLenEncodedString(out, value);
    }

    public void writeEofString(String value) {
        MysqlCodec.writeEofString(out, value);
    }

    public void writeNulTerminateString(String value) {
        MysqlCodec.writeNulTerminateString(out, value);
    }

    public void writeField(String db, String table, Column column, boolean sendDefault) {
        MysqlCodec.writeField(out, db, table, column, sendDefault);
    }

    public void writeField(String colName, Type type) {
        MysqlCodec.writeField(out, colName, type);
    }
}
