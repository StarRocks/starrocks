// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.baidu.brpc.client.RpcCallback;


public class RpcCallBackImpl<T> implements RpcCallback<T> {
    @Override
    public void success(T response) {
        System.out.println("success");
    }

    @Override
    public void fail(Throwable e) {
        System.out.println("fail");
    }
}
