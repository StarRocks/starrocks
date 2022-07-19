// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.RpcCallback;
// import io.netty.util.ReferenceCountUtil;


public class RpcCbAttach<T> implements RpcCallback<T> {
    private AttachmentRequest request;

    public RpcCbAttach(AttachmentRequest request) {
        this.request = request;
    }

    @Override
    public void success(T response) {
        System.out.println("success");
        if (RpcContext.isSet()) {
            RpcContext rpcContext = RpcContext.getContext();
            if (rpcContext.getResponseBinaryAttachment() != null) {
                byte[] bytes = new byte[rpcContext.getResponseBinaryAttachment().readableBytes()];
                rpcContext.getResponseBinaryAttachment().readBytes(bytes);
                System.out.println(bytes);
                System.out.println(rpcContext.getResponseKvAttachment());
                request.setSerializedResult(bytes);
                // ReferenceCountUtil.release(rpcContext.getResponseBinaryAttachment());
            }
        }
    }

    @Override
    public void fail(Throwable e) {
        System.out.println("fail");
    }
}

