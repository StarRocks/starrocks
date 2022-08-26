// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.pseudocluster;

import com.starrocks.common.GenericPool;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class PseudoGenericPool<VALUE extends org.apache.thrift.TServiceClient> extends GenericPool<VALUE> {
    public PseudoGenericPool(String name) {
        super(name, new GenericKeyedObjectPoolConfig(), 100);
    }

    @Override
    public boolean reopen(VALUE object, int timeoutMs) {
        return true;
    }

    @Override
    public boolean reopen(VALUE object) {
        return true;
    }

    @Override
    public void clearPool(TNetworkAddress addr) {
    }

    @Override
    public boolean peak(VALUE object) {
        return true;
    }

    @Override
    public VALUE borrowObject(TNetworkAddress address) throws Exception {
        return null;
    }

    @Override
    public VALUE borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
        return borrowObject(address);
    }

    @Override
    public void returnObject(TNetworkAddress address, VALUE object) {
    }

    @Override
    public void invalidateObject(TNetworkAddress address, VALUE object) {
    }
}
