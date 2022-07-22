// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.starrocks.lake.proto.AbortTxnRequest;
import com.starrocks.lake.proto.AbortTxnResponse;
import com.starrocks.lake.proto.CompactRequest;
import com.starrocks.lake.proto.CompactResponse;
import com.starrocks.lake.proto.DropTabletRequest;
import com.starrocks.lake.proto.DropTabletResponse;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class LakeServiceClient {
    private static final Logger LOG = LogManager.getLogger(LakeServiceClient.class);

    TNetworkAddress serverAddress;
    int maxRetries;
    int retryIntervalMs;

    public LakeServiceClient(TNetworkAddress serverAddress) {
        this(serverAddress, 3, 10);
    }

    public LakeServiceClient(TNetworkAddress serverAddress, int maxRetries, int retryIntervalMs) {
        this.serverAddress = serverAddress;
        this.maxRetries = maxRetries;
        this.retryIntervalMs = retryIntervalMs;
    }

    public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) throws RpcException {
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).publishVersionAsync(request));
    }

    public Future<AbortTxnResponse> abortTxn(AbortTxnRequest request) throws RpcException {
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).abortTxnAsync(request));
    }

    public Future<CompactResponse> compact(CompactRequest request) throws RpcException {
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).compactAsync(request));
    }

    public Future<DropTabletResponse> dropTablet(DropTabletRequest request) throws RpcException {
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).dropTabletAsync(request));
    }

    private <T> T run(Supplier<T> function) throws RpcException {
        try {
            return function.get();
        } catch (NoSuchElementException e) {
            return retry(function);
        }
    }

    private <T> T retry(Supplier<T> function) throws RpcException {
        LOG.info("RPC failed, will be retried {} times", maxRetries);
        int count = 0;
        while (true) {
            try {
                return function.get();
            } catch (NoSuchElementException e) {
                count++;
                LOG.warn("RPC failed on {} of {} retries", count, maxRetries);
                if (count >= maxRetries) {
                    LOG.warn("max retries exceeded");
                    throw new RpcException(serverAddress.hostname, e.getMessage());
                }
                sleep(retryIntervalMs);
            } catch (Throwable e) {
                throw new RpcException(serverAddress.hostname, e.getMessage());
            }
        }
    }

    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
            // do nothing
        }
    }
}
