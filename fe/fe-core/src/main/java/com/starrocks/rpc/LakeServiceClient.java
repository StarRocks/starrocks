// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.rpc;

import com.starrocks.lake.proto.AbortTxnRequest;
import com.starrocks.lake.proto.AbortTxnResponse;
import com.starrocks.lake.proto.CompactRequest;
import com.starrocks.lake.proto.CompactResponse;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;
import com.starrocks.thrift.TNetworkAddress;

import java.util.NoSuchElementException;
import java.util.concurrent.Future;

public class LakeServiceClient {
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
        int count = 0;
        while (true) {
            try {
                LakeService service = BrpcProxy.getInstance().getLakeService(serverAddress);
                return service.publishVersionAsync(request);
            } catch (NoSuchElementException e) {
                if (++count >= maxRetries) {
                    throw new RpcException(serverAddress.hostname, e.getMessage());
                }
                sleep(retryIntervalMs);
            } catch (Throwable e) {
                throw new RpcException(serverAddress.hostname, e.getMessage());
            }
        }
    }

    public Future<AbortTxnResponse> abortTxn(AbortTxnRequest request) throws RpcException {
        int count = 0;
        while (true) {
            try {
                LakeService service = BrpcProxy.getInstance().getLakeService(serverAddress);
                return service.abortTxnAsync(request);
            } catch (NoSuchElementException e) {
                if (++count >= maxRetries) {
                    throw new RpcException(serverAddress.hostname, e.getMessage());
                }
                sleep(retryIntervalMs);
            } catch (Throwable e) {
                throw new RpcException(serverAddress.hostname, e.getMessage());
            }
        }
    }

    public Future<CompactResponse> compact(CompactRequest request) throws RpcException {
        int count = 0;
        while (true) {
            try {
                LakeService service = BrpcProxy.getInstance().getLakeService(serverAddress);
                return service.compactAsync(request);
            } catch (NoSuchElementException e) {
                if (++count >= maxRetries) {
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
