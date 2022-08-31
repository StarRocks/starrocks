// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.starrocks.lake.proto.AbortTxnRequest;
import com.starrocks.lake.proto.AbortTxnResponse;
import com.starrocks.lake.proto.CompactRequest;
import com.starrocks.lake.proto.CompactResponse;
import com.starrocks.lake.proto.DeleteDataRequest;
import com.starrocks.lake.proto.DeleteDataResponse;
import com.starrocks.lake.proto.DeleteTabletRequest;
import com.starrocks.lake.proto.DeleteTabletResponse;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;
import com.starrocks.lake.proto.TabletStatRequest;
import com.starrocks.lake.proto.TabletStatResponse;
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

    public Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request) throws RpcException {
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).deleteTablet(request));
    }

    public Future<DeleteDataResponse> deleteData(DeleteDataRequest request) throws RpcException {
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).deleteData(request));
    }

    public Future<TabletStatResponse> getTabletStats(TabletStatRequest request) throws RpcException {
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).getTabletStats(request));
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
