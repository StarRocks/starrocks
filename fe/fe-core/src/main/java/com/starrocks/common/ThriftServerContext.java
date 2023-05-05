// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/ThriftServerContext.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common;

import com.starrocks.thrift.TNetworkAddress;
import org.apache.thrift.server.ServerContext;

public class ThriftServerContext implements ServerContext {
    private TNetworkAddress client;

    public ThriftServerContext(TNetworkAddress clientAddress) {
        this.client = clientAddress;
    }

    public TNetworkAddress getClient() {
        return client;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        try {
            if (isWrapperFor(iface)) {
                return iface.cast(client);
            } else {
                throw new RuntimeException("The context is not a wrapper for " + iface.getName());
            }
        } catch (Exception e) {
            throw new RuntimeException("The context is not a wrapper and does not implement the interface");
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(client);
    }
}
