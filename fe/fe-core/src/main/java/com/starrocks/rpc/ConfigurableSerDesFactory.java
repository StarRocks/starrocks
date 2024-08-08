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

package com.starrocks.rpc;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;

public class ConfigurableSerDesFactory {

    public static TSerializer getTSerializer() throws TTransportException {
        return getTSerializer(Protocol.BINARY.name());
    }

    public static TSerializer getTSerializer(String protocol) throws TTransportException {
        Protocol p = Protocol.getProtocol(protocol);
        TProtocolFactory factory = ConfigurableTProtocolFactory.getTProtocolFactory(p);
        return new TSerializer(factory);
    }
    public static TDeserializer getTDeserializer() throws TTransportException {
        return getTDeserializer(Protocol.BINARY.name());
    }

    public static TDeserializer getTDeserializer(String protocol) throws TTransportException {
        Protocol p = Protocol.getProtocol(protocol);
        TProtocolFactory factory = ConfigurableTProtocolFactory.getTProtocolFactory(p);
        return new TDeserializer(factory);
    }

    public enum Protocol {
        BINARY,
        COMPACT,
        JSON,
        SIMPLE_JSON;

        public static Protocol getProtocol(String name) {
            for (Protocol protocol : Protocol.values()) {
                if (protocol.name().equalsIgnoreCase(name)) {
                    return protocol;
                }
            }
            return BINARY;
        }
    }
}
