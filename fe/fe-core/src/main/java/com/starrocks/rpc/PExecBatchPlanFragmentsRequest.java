// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.rpc;

import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class PExecBatchPlanFragmentsRequest extends AttachmentRequest {
    @Protobuf(order = 1, required = false)
    String attachmentProtocol;

    public void setAttachmentProtocol(String attachmentProtocol) {
        this.attachmentProtocol = attachmentProtocol;
    }
}
