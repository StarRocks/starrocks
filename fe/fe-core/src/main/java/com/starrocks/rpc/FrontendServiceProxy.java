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

import com.starrocks.common.ClientPool;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketTimeoutException;

public class FrontendServiceProxy {
    private static final Logger LOG = LogManager.getLogger(FrontendServiceProxy.class);

    public static <T> T call(TNetworkAddress address, int timeoutMs, int retryTimes, MethodCallable<T> callable)
            throws Exception {
        FrontendService.Client client = ClientPool.frontendPool.borrowObject(address, timeoutMs);
        boolean isConnValid = false;
        try {
            for (int i = 0; i < retryTimes; i++) {
                try {
                    T t = callable.invoke(client);
                    isConnValid = true;
                    return t;
                } catch (TTransportException te) {
                    // The frontendPool may return a broken conn,
                    // because there is no validation of the conn in the frontendPool.
                    // In this case we should reopen the conn and retry the rpc call,
                    // but we do not retry for the timeout exception, because it may be a network timeout
                    // or the target server may be running slow.
                    isConnValid = ClientPool.frontendPool.reopen(client, timeoutMs);
                    if (i == retryTimes - 1 ||
                            !isConnValid ||
                            (te.getCause() instanceof SocketTimeoutException)) {
                        LOG.warn("call frontend thrift rpc failed, addr: {}, retried: {}", address, i, te);
                        throw te;
                    } else {
                        LOG.debug("call frontend thrift rpc failed, addr: {}, retried: {}", address, i, te);
                    }
                }
            }
        } finally {
            if (isConnValid) {
                ClientPool.frontendPool.returnObject(address, client);
            } else {
                ClientPool.frontendPool.invalidateObject(address, client);
            }
        }

        throw new Exception("unexpected");
    }

    public interface MethodCallable<T> {
        T invoke(FrontendService.Client client) throws TException;
    }
}
