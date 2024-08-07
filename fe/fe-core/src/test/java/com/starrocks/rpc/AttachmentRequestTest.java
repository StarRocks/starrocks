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