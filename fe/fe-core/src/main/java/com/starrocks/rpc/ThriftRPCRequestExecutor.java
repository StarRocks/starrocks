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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import com.starrocks.metric.MetricRepo;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketTimeoutException;
import java.util.concurrent.ThreadLocalRandom;

public class ThriftRPCRequestExecutor {
    private static final Logger LOG = LogManager.getLogger(ThriftRPCRequestExecutor.class);

    // Before reconnecting after a broken connection, the n-th retry (from 0) sleeps a uniformly random
    // time in [d/2, d] ms, d = min(RETRY_BACKOFF_MAX_MS, RETRY_BACKOFF_BASE_MS * 2^n): 100-200, 200-400,
    // 400-800, then 500-1000. Without the pause, every caller that lost its connection to a saturated
    // server reconnects and resends at the same moment, so the retries land on that server as one more
    // synchronized wave. The floor keeps a retry from coming back before the server had any time to
    // recover; the jitter keeps the callers apart.
    @VisibleForTesting
    static final long RETRY_BACKOFF_BASE_MS = 200;
    @VisibleForTesting
    static final long RETRY_BACKOFF_MAX_MS = 1000;

    @VisibleForTesting
    static Sleeper sleeper = Thread::sleep;

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
                    // The connection pool may still return a broken conn: it only replaces connections
                    // idle past what the peer allows (see ThriftConnectionPool.frontendPool), not ones
                    // the peer dropped for another reason, such as a restart.
                    // In this case we should reopen the connection and retry the rpc call,
                    // but we do not retry for the timeout exception, because it may be a network timeout
                    // or the target server may be running slow.
                    if (i < tryTimes - 1 && !(te.getCause() instanceof SocketTimeoutException)) {
                        // Back off before reopening, not after: the reopen is itself a new connection
                        // to the server. The conn counts as broken until reopened, so an interrupted
                        // backoff invalidates it instead of returning it to the pool.
                        isConnValid = false;
                        if (backOffBeforeRetry(address, i, te)) {
                            isConnValid = genericPool.reopen(client, timeoutMs);
                            if (isConnValid) {
                                // Counted only now that the request will be resent: a backoff cut
                                // short by an interrupt, or a failed reopen, ends the call instead.
                                if (MetricRepo.hasInit) {
                                    MetricRepo.COUNTER_THRIFT_CLIENT_RETRIES.increase(1L);
                                }
                                continue;
                            }
                        }
                    } else {
                        isConnValid = genericPool.reopen(client, timeoutMs);
                    }
                    LOG.warn("Call thrift rpc failed, addr: {}, retried: {}", address, i, te);
                    throw te;
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

    /**
     * Sleeps before the given retry. Returns false, with the thread's interrupt status restored, if
     * the sleep was interrupted, in which case the caller must give up instead of retrying.
     */
    private static boolean backOffBeforeRetry(TNetworkAddress address, int retry, TTransportException cause) {
        long boundMs = retryBackoffBoundMs(retry);
        long backoffMs = boundMs / 2 + ThreadLocalRandom.current().nextLong(boundMs - boundMs / 2 + 1);
        LOG.debug("Call thrift rpc failed, addr: {}, retried: {}, retry in {} ms", address, retry, backoffMs, cause);
        try {
            sleeper.sleep(backoffMs);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cause.addSuppressed(e);
            return false;
        }
    }

    @VisibleForTesting
    static long retryBackoffBoundMs(int retry) {
        // The clamp only keeps the shift from overflowing for a large tryTimes; the bound has long
        // reached RETRY_BACKOFF_MAX_MS by then.
        return Math.min(RETRY_BACKOFF_MAX_MS, RETRY_BACKOFF_BASE_MS << Math.min(retry, 16));
    }

    @FunctionalInterface
    @VisibleForTesting
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    @FunctionalInterface
    public interface MethodCallable<T, R> {
        R apply(T client) throws TException;
    }
}
