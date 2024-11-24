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

import com.starrocks.common.Config;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketTimeoutException;

public class ThriftRPCRequestExecutor {
    private static final Logger LOG = LogManager.getLogger(ThriftRPCRequestExecutor.class);

    public static <RESULT, SERVER_CLIENT extends org.apache.thrift.TServiceClient> RESULT
            call(ThriftConnectionPool<SERVER_CLIENT> genericPool,
                 TNetworkAddress address,
                 MethodCallable<SERVER_CLIENT, RESULT> callable) throws TException {
        return call(genericPool, address, genericPool.getDefaultTimeoutMs(), Config.thrift_rpc_retry_times, callable);
    }

    public static <RESULT, SERVER_CLIENT extends org.apache.thrift.TServiceClient> RESULT
            callNoRetry(ThriftConnectionPool<SERVER_CLIENT> genericPool,
                TNetworkAddress address,
                MethodCallable<SERVER_CLIENT, RESULT> callable) throws TException {
        return call(genericPool, address, genericPool.getDefaultTimeoutMs(), 1, callable);
    }

    public static <RESULT, SERVER_CLIENT extends org.apache.thrift.TServiceClient> RESULT
            call(ThriftConnectionPool<SERVER_CLIENT> genericPool,
                 TNetworkAddress address,
                 int timeoutMs,
                 MethodCallable<SERVER_CLIENT, RESULT> callable) throws TException {
        return call(genericPool, address, timeoutMs, Config.thrift_rpc_retry_times, callable);
    }

    public static <RESULT, SERVER_CLIENT extends org.apache.thrift.TServiceClient> RESULT
            call(ThriftConnectionPool<SERVER_CLIENT> genericPool,
            TNetworkAddress address,
            int timeoutMs, int tryTimes,
            MethodCallable<SERVER_CLIENT, RESULT> callable) throws TException {
        SERVER_CLIENT client;
        try {
            client = genericPool.borrowObject(address, timeoutMs);
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }

        boolean isConnValid = false;
        try {
            for (int i = 0; i < tryTimes; i++) {
                try {
                    RESULT t = callable.apply(client);
                    isConnValid = true;
                    return t;
                } catch (TTransportException te) {
                    // The connection pool may return a broken conn,
                    // because there is no validation of the connection in the connection pool.
                    // In this case we should reopen the connection and retry the rpc call,
                    // but we do not retry for the timeout exception, because it may be a network timeout
                    // or the target server may be running slow.
                    isConnValid = genericPool.reopen(client, timeoutMs);
                    if (i == tryTimes - 1 ||
                            !isConnValid ||
                            (te.getCause() instanceof SocketTimeoutException)) {
                        LOG.warn("Call thrift rpc failed, addr: {}, retried: {}", address, i, te);
                        throw te;
                    } else {
                        LOG.debug("Call thrift rpc failed, addr: {}, retried: {}", address, i, te);
                    }
                }
            }
        } finally {
            if (isConnValid) {
                genericPool.returnObject(address, client);
            } else {
                genericPool.invalidateObject(address, client);
            }
        }

        throw new TException("Call thrift rpc failed");
    }

    @FunctionalInterface
    public interface MethodCallable<T, R> {
        R apply(T client) throws TException;
    }
}
