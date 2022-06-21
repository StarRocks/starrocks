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

// MySQL protocol change user packet, which contain authenticate information.
public class MysqlChangeUserPacket extends MysqlPacket {
    private int characterSet;
    private String userName;
    private byte[] authResponse;
    private String database;
    private String pluginName;
    private MysqlCapability capability;
    private Map<String, String> connectAttributes;

    public MysqlChangeUserPacket(MysqlCapability capability) {
        this.capability = capability;
    }

    public String getUser() {
        return userName;
    }

    public byte[] getAuthResponse() {
        return authResponse;
    }

    public String getDb() {
        return database;
    }

    @Override
    public boolean readFrom(ByteBuffer buffer) {
        // protocol refer to: https://dev.mysql.com/doc/internals/en/com-change-user.html
        if (2 > buffer.limit() || MysqlCommand.COM_CHANGE_USER.getCommandCode() != buffer.get(0)) {
            return false;
        }
        buffer.position(1);
        userName = new String(MysqlProto.readNulTerminateString(buffer));
        if (1 > buffer.remaining()) {
            return false;
        }
        // parse the password with the capability previously set on connecting
        if (capability.isPluginAuthDataLengthEncoded()) {
            authResponse = MysqlProto.readLenEncodedString(buffer);
        } else if (capability.isSecureConnection()) {
            int len = MysqlProto.readInt1(buffer);
            authResponse = MysqlProto.readFixedString(buffer, len);
        } else {
            authResponse = MysqlProto.readNulTerminateString(buffer);
        }
        // parse database name
        if (0 < buffer.remaining()) {
            database = new String(MysqlProto.readNulTerminateString(buffer));
        }
        if (2 > buffer.remaining()) {
            return false;
        }
        characterSet = MysqlProto.readInt2(buffer);
        // plugin name to plugin
        if (0 < buffer.remaining() && capability.isPluginAuth()) {
            pluginName = new String(MysqlProto.readNulTerminateString(buffer));
        }
        // attribute map, no use now.
        if (0 < buffer.remaining() && capability.isConnectAttrs()) {
            connectAttributes = Maps.newHashMap();
            long numPair = MysqlProto.readVInt(buffer);
            for (long i = 0; i < numPair; ++i) {
                String key = new String(MysqlProto.readLenEncodedString(buffer));
                String value = new String(MysqlProto.readLenEncodedString(buffer));
                connectAttributes.put(key, value);
            }
        }
        return true;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {

    }
}
