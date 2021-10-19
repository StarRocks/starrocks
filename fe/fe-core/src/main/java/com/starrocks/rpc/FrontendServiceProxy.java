// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.rpc;

import com.starrocks.common.ClientPool;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.thrift.TException;

public class FrontendServiceProxy {

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

    public interface MethodCallable<T> {
        T invoke(FrontendService.Client client) throws TException;
    }
}
