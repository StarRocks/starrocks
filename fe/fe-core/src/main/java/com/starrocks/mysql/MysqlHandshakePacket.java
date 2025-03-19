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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/MysqlHandshakePacket.java

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

import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.AuthPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// MySQL protocol handshake packet.
public class MysqlHandshakePacket extends MysqlPacket {
    private static final Logger LOG = LogManager.getLogger(MysqlHandshakePacket.class);

    private static final int SCRAMBLE_LENGTH = 20;
    // Version of handshake packet, since MySQL 3.21.0, Handshake of protocol 10 is used
    private static final int PROTOCOL_VERSION = 10;

    // 33 stands for UTF-8 character set
    private static final int CHARACTER_SET = 33;
    // use default capability for all
    private static final MysqlCapability CAPABILITY = MysqlCapability.DEFAULT_CAPABILITY;
    // status flags not supported in StarRocks
    private static final int STATUS_FLAGS = 0;

    // connection id used in KILL statement.
    private final int connectionId;
    private final byte[] authPluginData;
    private final boolean supportSSL;

    public MysqlHandshakePacket(int connectionId, boolean supportSSL) {
        this.connectionId = connectionId;
        this.authPluginData = MysqlPassword.createRandomString(SCRAMBLE_LENGTH);
        this.supportSSL = supportSSL;
    }

    public byte[] getAuthPluginData() {
        return authPluginData;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        MysqlCapability capability = CAPABILITY;
        if (supportSSL) {
            capability = new MysqlCapability(capability.getFlags()
                    | MysqlCapability.Flag.CLIENT_SSL.getFlagBit());
        }

        serializer.writeInt1(PROTOCOL_VERSION);
        // JDBC use this version to check which protocol the server support
        serializer.writeNulTerminateString(Config.mysql_server_version);
        serializer.writeInt4(connectionId);
        // first 8 bytes of auth plugin data
        serializer.writeBytes(authPluginData, 0, 8);
        // filler
        serializer.writeInt1(0);
        // lower 2 bytes of capability flags
        serializer.writeInt2(capability.getFlags() & 0XFFFF);
        serializer.writeInt1(CHARACTER_SET);
        serializer.writeInt2(STATUS_FLAGS);
        // upper 2 byte of capability flags
        serializer.writeInt2(capability.getFlags() >> 16);
        if (capability.isPluginAuth()) {
            serializer.writeInt1(authPluginData.length + 1); // 1 byte is '\0'
        } else {
            serializer.writeInt1(0);
        }
        // reserved ten zeros
        serializer.writeBytes(new byte[10]);
        if (capability.isSecureConnection()) {
            // NOTE: MySQL protocol require writing at least 13 byte here.
            // write len(max(13, len(auth-plugin-data) - 8))
            serializer.writeBytes(authPluginData, 8, 12);
            // so we append one byte up to 13
            serializer.writeInt1(0);
        }
        if (capability.isPluginAuth()) {
            serializer.writeNulTerminateString(AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString());
        }
    }
}
