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

import com.starrocks.common.Config;
import com.starrocks.thrift.TBinaryLiteral;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

class AttachmentRequestTest {

    @Test
    void readBinaryTest() throws TException {
        Config.thrift_max_message_size = 1000000;
        AttachmentRequest attachmentRequest = new AttachmentRequest();
        byte[] bytes = new byte[2000000];
        for (int i = 0; i < 2000000; i++) {
            bytes[i] = (byte) (i % 128);
        }
        TBinaryLiteral binaryLiteral = new TBinaryLiteral(ByteBuffer.wrap(bytes));
        attachmentRequest.setRequest(binaryLiteral);
        TBinaryLiteral res1 = new TBinaryLiteral();
        Assert.assertThrows("MaxMessageSize reached", TTransportException.class, () -> attachmentRequest.getRequest(res1));

        Config.thrift_max_message_size = 5000000;
        TBinaryLiteral res2 = new TBinaryLiteral();
        attachmentRequest.getRequest(res2);
        Assert.assertEquals(2000000, res2.value.remaining());
    }

    @After
    public void after() {
        Config.thrift_max_message_size = 1024 * 1024 * 1024;
    }
}