// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.starrocks.common.ClientPool;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

public class FrontendServiceProxy {
    private static final Logger LOG = LogManager.getLogger(FrontendServiceProxy.class);

    public static <T> T call(TNetworkAddress address, int timeoutMs, MethodCallable<T> callable) throws Exception {
        FrontendService.Client client = ClientPool.frontendPool.borrowObject(address, timeoutMs);

        boolean returnToPool = false;
        T res;
        try {
            res = callable.invoke(client);
            returnToPool = true;
        } finally {
            if (returnToPool) {
                ClientPool.frontendPool.returnObject(address, client);
            } else {
                ClientPool.frontendPool.invalidateObject(address, client);
            }
        }
        return res;
    }

    public static <T> T call(TNetworkAddress address, int timeoutMs, int retryTimes, MethodCallable<T> callable)
            throws Exception {
        for (int i = 0; i < retryTimes; i++) {
            try {
                return call(address, timeoutMs, callable);
            } catch (Exception e) {
                LOG.warn("call frontend [{}] rpc failed, retried: {}", address, i, e);
                if (i == retryTimes - 1) {
                    throw e;
                }
                Thread.sleep(1000);
            }
        }
        throw new Exception("unexpected");
    }

    public interface MethodCallable<T> {
        T invoke(FrontendService.Client client) throws TException;
    }
}
