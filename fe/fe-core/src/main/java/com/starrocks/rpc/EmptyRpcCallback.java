// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.rpc;

import com.baidu.brpc.client.RpcCallback;

public class EmptyRpcCallback<T> implements RpcCallback<T> {

    @Override
    public void success(T response) {
    }

    @Override
    public void fail(Throwable e) {
    }
}
