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

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.baidu.jprotobuf.pbrpc.client.PojoRpcMethodInfo;
import com.baidu.jprotobuf.pbrpc.client.RpcMethodInfo;
import com.baidu.jprotobuf.pbrpc.data.RpcDataPackage;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

/**
 * Tests for serialization and deserialization of {@link PExecPlanFragmentRequest}.
 */
public class PExecPlanFragmentRequestTest {
    private static final String ATTACHMENT_PROTOCOL = "binary";

    @Test
    public void testSerDePExecPlanFragmentRequestTest() throws Exception {
        PExecPlanFragmentRequest request = buildPExecPlanFragmentRequest();
        Codec<PExecPlanFragmentRequest> pExecPlanFragmentRequestCodec = ProtobufProxy.create(PExecPlanFragmentRequest.class);

        byte[] encode = pExecPlanFragmentRequestCodec.encode(request);
        PExecPlanFragmentRequest decodeRequest = pExecPlanFragmentRequestCodec.decode(encode);

        Assert.assertEquals(ATTACHMENT_PROTOCOL, decodeRequest.attachmentProtocol);
        Assert.assertNull(decodeRequest.getSerializedRequest());
        Assert.assertNull(decodeRequest.getSerializedResult());
    }

    @Test
    public void testBuildRpcDataPackage() throws Exception {
        RpcMethodInfo rpcMethodInfo = buildRpcMethodInfo("execPlanFragmentAsync", PExecPlanFragmentRequest.class);
        PExecPlanFragmentRequest request = buildPExecPlanFragmentRequest();
        RpcDataPackage rpcData = RpcDataPackage.buildRpcDataPackage(rpcMethodInfo, new Object[] {request});

        Codec<PExecPlanFragmentRequest> pExecPlanFragmentRequestCodec = ProtobufProxy.create(PExecPlanFragmentRequest.class);
        byte[] encode = pExecPlanFragmentRequestCodec.encode(request);

        Assert.assertArrayEquals(encode, rpcData.getData());
        Assert.assertArrayEquals(request.getSerializedRequest(), rpcData.getAttachment());
    }

    private PExecPlanFragmentRequest buildPExecPlanFragmentRequest() {
        PExecPlanFragmentRequest request = new PExecPlanFragmentRequest();
        request.setAttachmentProtocol(ATTACHMENT_PROTOCOL);
        request.setRequest("test-request".getBytes());
        request.setSerializedResult(null);
        return request;
    }

    private RpcMethodInfo buildRpcMethodInfo(String methodName, Class<?>... types) throws NoSuchMethodException {
        Method method = PBackendService.class.getMethod(methodName, types);
        ProtobufRPC protobufRPC = method.getAnnotation(ProtobufRPC.class);
        return new PojoRpcMethodInfo(method, protobufRPC);
    }
}
