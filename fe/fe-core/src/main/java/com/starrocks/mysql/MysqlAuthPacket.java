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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/MysqlAuthPacket.java

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

import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.Map;

// MySQL protocol handshake response packet, which contain authenticate information.
public class MysqlAuthPacket extends MysqlPacket {
    private int maxPacketSize;
    private int characterSet;
    private String userName;
    private byte[] authResponse;
    private String database;
    private String pluginName;
    private MysqlCapability capability;
    private Map<String, String> connectAttributes;
    // zstd level the client asked for, 0 if it did not send one
    private int zstdCompressionLevel;

    public String getUser() {
        return userName;
    }

    public byte[] getAuthResponse() {
        return authResponse;
    }

    public void setAuthResponse(byte[] bytes) {
        authResponse = bytes;
    }

    public String getDb() {
        return database;
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public boolean isSSLConnRequest() {
        return capability.isSSL();
    }

    public Map<String, String> getConnectAttributes() {
        return connectAttributes;
    }

    public int getZstdCompressionLevel() {
        return zstdCompressionLevel;
    }

    @Override
    public boolean readFrom(ByteBuffer buffer) {
        // read capability four byte, which CLIENT_PROTOCOL_41 must be set
        capability = new MysqlCapability(MysqlCodec.readInt4(buffer));
        if (!capability.isProtocol41()) {
            return false;
        }
        // max packet size
        maxPacketSize = MysqlCodec.readInt4(buffer);
        // character set. only support 33(utf-8)?
        characterSet = MysqlCodec.readInt1(buffer);
        // reserved 23 bytes
        buffer.position(buffer.position() + 23);

        // if the request is a ssl request, the package is truncated here.
        if (buffer.remaining() <= 0 && capability.isSSL()) {
            return true;
        }
        // user name
        userName = new String(MysqlCodec.readNulTerminateString(buffer));
        if (capability.isPluginAuthDataLengthEncoded()) {
            authResponse = MysqlCodec.readLenEncodedString(buffer);
        } else if (capability.isSecureConnection()) {
            int len = MysqlCodec.readInt1(buffer);
            authResponse = MysqlCodec.readFixedString(buffer, len);
        } else {
            authResponse = MysqlCodec.readNulTerminateString(buffer);
        }
        // DB to use
        if (buffer.remaining() > 0 && capability.isConnectedWithDb()) {
            database = new String(MysqlCodec.readNulTerminateString(buffer));
        }
        // plugin name to plugin
        if (buffer.remaining() > 0 && capability.isPluginAuth()) {
            pluginName = new String(MysqlCodec.readNulTerminateString(buffer));
        }
        // connect attrs
        if (buffer.remaining() > 0 && capability.isConnectAttrs()) {
            connectAttributes = parseConnectAttrs(buffer);
        }
        // a client that asks for zstd sends its compression level after the connect attrs
        if (buffer.remaining() > 0 && capability.isZstdCompress()) {
            zstdCompressionLevel = MysqlCodec.readInt1(buffer);
        }

        // Commented for JDBC
        // if (buffer.remaining() != 0) {
        //     return false;
        // }
        return true;
    }

    private Map<String, String> parseConnectAttrs(ByteBuffer buffer) {
        String key = "";
        String value = "";
        connectAttributes = Maps.newHashMap();
        try {
            long allAttrLength = MysqlCodec.readVInt(buffer);
            // Parse the attrs from their own view and step over them, so the fields that follow the attrs
            // (e.g. the zstd level) are not read as attrs.
            int attrsEnd = (int) Math.min(buffer.limit(), buffer.position() + allAttrLength);
            ByteBuffer attrs = buffer.duplicate();
            attrs.limit(attrsEnd);
            buffer.position(attrsEnd);
            long curDealLen = 0;
            while (attrs.remaining() > 0 && allAttrLength - curDealLen > 0) {
                key = value = "";
                long keyLength = MysqlCodec.readVInt(attrs);

                if (attrs.remaining() >= keyLength) {
                    key = new String(MysqlCodec.readFixedString(attrs, (int) keyLength));
                } else {
                    return connectAttributes;
                }
                curDealLen += keyLength;
                long valLength = MysqlCodec.readVInt(attrs);
                if (attrs.remaining() >= valLength) {
                    value = new String(MysqlCodec.readFixedString(attrs, (int) valLength));
                } else {
                    // only parse key success
                    connectAttributes.put(key, "");
                    return connectAttributes;
                }
                curDealLen += valLength;
                connectAttributes.put(key, value);
            }
        } catch (Exception ex) {
            connectAttributes.put(key, value);
        }
        return connectAttributes;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {

    }
}
