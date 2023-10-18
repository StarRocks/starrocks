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

package com.starrocks.rpc;

import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;

// used to compatible with our older thrift protocol
public class AttachmentRequest {
    protected byte[] serializedRequest;
    protected byte[] serializedResult;

    public static TSerializer getSerializer(String protocol) {
        TSerializer serializer;
        if (StringUtils.equalsIgnoreCase(protocol, "compact")) {
            serializer = new TSerializer(TCompactProtocol::new);
        } else if (StringUtils.equalsIgnoreCase(protocol, "json")) {
            serializer = new TSerializer(TJSONProtocol::new);
        } else {
            // default bianry
            serializer = new TSerializer(TBinaryProtocol::new);
        }
        return serializer;
    }

    public <T extends TBase<T, F>, F extends TFieldIdEnum> void setRequest(TBase<T, F> request, String protocol)
            throws TException {
        TSerializer serializer = getSerializer(protocol);
        try (Timer ignored = Tracers.watchScope(Tracers.Module.SCHEDULER, "DeploySerializeTime")) {
            serializedRequest = serializer.serialize(request);
        }
    }

    public <T extends TBase<T, F>, F extends TFieldIdEnum> void setRequest(TBase<T, F> request)
            throws TException {
        TSerializer serializer = new TSerializer(TBinaryProtocol::new);

        serializedRequest = serializer.serialize(request);
    }

    public void setRequest(byte[] request) {
        serializedRequest = request;
    }

    public byte[] getSerializedRequest() {
        return serializedRequest;
    }

    public void setSerializedResult(byte[] result) {
        this.serializedResult = result;
    }

    public byte[] getSerializedResult() {
        return serializedResult;
    }

    public <T extends TBase<T, F>, F extends TFieldIdEnum> void getResult(TBase<T, F> result) throws TException {
        TDeserializer deserializer = new TDeserializer();
        deserializer.deserialize(result, serializedResult);
    }

    public <T extends TBase<T, F>, F extends TFieldIdEnum> void getRequest(TBase<T, F> request) throws TException {
        TDeserializer deserializer = new TDeserializer();
        deserializer.deserialize(request, serializedRequest);
    }
}
