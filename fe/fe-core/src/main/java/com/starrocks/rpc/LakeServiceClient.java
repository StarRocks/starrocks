// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.RpcCallback;
import com.starrocks.lake.proto.AbortTxnRequest;
import com.starrocks.lake.proto.AbortTxnResponse;
import com.starrocks.lake.proto.CompactRequest;
import com.starrocks.lake.proto.CompactResponse;
import com.starrocks.lake.proto.DeleteDataRequest;
import com.starrocks.lake.proto.DeleteDataResponse;
import com.starrocks.lake.proto.DeleteTabletRequest;
import com.starrocks.lake.proto.DeleteTabletResponse;
import com.starrocks.lake.proto.DropTableRequest;
import com.starrocks.lake.proto.DropTableResponse;
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
        RpcCallback<PublishVersionResponse> callback = new EmptyRpcCallback<PublishVersionResponse>();
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).publishVersion(request, callback));
    }

    public Future<AbortTxnResponse> abortTxn(AbortTxnRequest request) throws RpcException {
        RpcCallback<AbortTxnResponse> callback = new EmptyRpcCallback<AbortTxnResponse>();
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).abortTxn(request, callback));
    }

    public Future<CompactResponse> compact(CompactRequest request) throws RpcException {
        RpcCallback<CompactResponse> callback = new EmptyRpcCallback<CompactResponse>();
        RpcContext rpcContext = RpcContext.getContext();
        rpcContext.setReadTimeoutMillis(1800000);
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).compact(request, callback));
    }

    public Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request) throws RpcException {
        RpcCallback<DeleteTabletResponse> callback = new EmptyRpcCallback<DeleteTabletResponse>();
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).deleteTablet(request, callback));
    }

    public Future<DeleteDataResponse> deleteData(DeleteDataRequest request) throws RpcException {
        RpcCallback<DeleteDataResponse> callback = new EmptyRpcCallback<DeleteDataResponse>();
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).deleteData(request, callback));
    }

    public Future<TabletStatResponse> getTabletStats(TabletStatRequest request) throws RpcException {
        RpcCallback<TabletStatResponse> callback = new EmptyRpcCallback<>();
        RpcContext rpcContext = RpcContext.getContext();
        rpcContext.setReadTimeoutMillis(600000);
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).getTabletStats(request, callback));
    }

    public Future<DropTableResponse> dropTable(DropTableRequest request) throws RpcException {
        RpcCallback<DropTableResponse> callback = new EmptyRpcCallback<DropTableResponse>();
        return run(() -> BrpcProxy.getInstance().getLakeService(serverAddress).dropTable(request, callback));
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
