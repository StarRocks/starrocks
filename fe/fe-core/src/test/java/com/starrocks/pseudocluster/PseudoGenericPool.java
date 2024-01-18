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

package com.starrocks.pseudocluster;

import com.starrocks.common.concurrent.GenericPool;
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
