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


package com.starrocks.utframe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.common.concurrent.GenericPool;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.HeartbeatService;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

import java.util.Map;

public class MockGenericPool<VALUE extends org.apache.thrift.TServiceClient> extends GenericPool<VALUE> {
    protected Map<TNetworkAddress, MockedBackend> backendMap = Maps.newConcurrentMap();

    public MockGenericPool(String name) {
        super(name, new GenericKeyedObjectPoolConfig(), 100);
    }

    public void register(MockedBackend backend) {
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

    public static class HeatBeatPool extends MockGenericPool<HeartbeatService.Client> {
        public HeatBeatPool(String name) {
            super(name);
        }

        @Override
        public void register(MockedBackend backend) {
            backendMap.put(new TNetworkAddress(backend.getHost(), backend.getHeartBeatPort()), backend);
        }

        @Override
        public HeartbeatService.Client borrowObject(TNetworkAddress address) throws Exception {
            Preconditions.checkState(backendMap.containsKey(address));
            return backendMap.get(address).heatBeatClient;
        }
    }

    public static class BackendThriftPool extends MockGenericPool<BackendService.Client> {
        public BackendThriftPool(String name) {
            super(name);
        }

        @Override
        public void register(MockedBackend backend) {
            backendMap.put(new TNetworkAddress(backend.getHost(), backend.getBeThriftPort()), backend);
        }

        @Override
        public BackendService.Client borrowObject(TNetworkAddress address) throws Exception {
            Preconditions.checkState(backendMap.containsKey(address));
            return backendMap.get(address).thriftClient;
        }
    }
}
