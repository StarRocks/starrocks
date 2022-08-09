// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rpc/BackendServiceProxy.java

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

package com.starrocks.rpc;

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.RpcCallback;
import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PExecBatchPlanFragmentsRequest;
import com.starrocks.proto.PExecBatchPlanFragmentsResult;
import com.starrocks.proto.PExecPlanFragmentRequest;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PFetchDataRequest;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.PTriggerProfileReportRequest;
import com.starrocks.proto.PTriggerProfileReportResult;
import com.starrocks.proto.PUniqueId;
import com.starrocks.thrift.TExecBatchPlanFragmentsParams;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.NoSuchElementException;
import java.util.concurrent.Future;

public class BackendServiceClient {
    private static final Logger LOG = LogManager.getLogger(BackendServiceClient.class);

    private BackendServiceClient() {
    }

    public static BackendServiceClient getInstance() {
        return BackendServiceClient.SingletonHolder.INSTANCE;
    }

    public Future<PExecPlanFragmentResult> execPlanFragmentAsync(
            TNetworkAddress address, TExecPlanFragmentParams tRequest)
            throws TException, RpcException {
        final PExecPlanFragmentRequest pRequest = new PExecPlanFragmentRequest();
        TSerializer serializer = new TSerializer();
        byte[] serializedRequest = serializer.serialize(tRequest);
        RpcContext rpcContext = RpcContext.getContext();
        rpcContext.setReadTimeoutMillis(60000);
        rpcContext.setRequestBinaryAttachment(serializedRequest);
        RpcCallback<PExecPlanFragmentResult> callback = new EmptyRpcCallback<PExecPlanFragmentResult>();
        try {
            final PBackendServiceAsync service = BrpcProxy.getBackendService(address);
            return service.execPlanFragment(pRequest, callback);
        } catch (NoSuchElementException e) {
            try {
                // retry
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    // do nothing
                }
                final PBackendServiceAsync service = BrpcProxy.getBackendService(address);
                return service.execPlanFragment(pRequest, callback);
            } catch (NoSuchElementException noSuchElementException) {
                LOG.warn("Execute plan fragment retry failed, address={}:{}",
                        address.getHostname(), address.getPort(), noSuchElementException);
                throw new RpcException(address.hostname, e.getMessage());
            }
        } catch (Throwable e) {
            LOG.warn("Execute plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PExecBatchPlanFragmentsResult> execBatchPlanFragmentsAsync(
            TNetworkAddress address, TExecBatchPlanFragmentsParams tRequest)
            throws TException, RpcException {
        final PExecBatchPlanFragmentsRequest pRequest = new PExecBatchPlanFragmentsRequest();
        TSerializer serializer = new TSerializer();
        byte[] serializedRequest = serializer.serialize(tRequest);
        RpcContext rpcContext = RpcContext.getContext();
        rpcContext.setReadTimeoutMillis(600000);
        rpcContext.setRequestBinaryAttachment(serializedRequest);
        RpcCallback<PExecBatchPlanFragmentsResult> callback = new EmptyRpcCallback<PExecBatchPlanFragmentsResult>();
        Future<PExecBatchPlanFragmentsResult> resultFuture = null;
        for (int i = 1; i <= Config.max_query_retry_time && resultFuture == null; ++i) {
            try {
                final PBackendServiceAsync service = BrpcProxy.getBackendService(address);
                resultFuture = service.execBatchPlanFragments(pRequest, callback);
            } catch (NoSuchElementException e) {
                // Retry `RETRY_TIMES`, when NoSuchElementException occurs.
                if (i >= Config.max_query_retry_time) {
                    LOG.warn("Execute batch plan fragments retry failed, address={}:{}",
                            address.getHostname(), address.getPort(), e);
                    throw new RpcException(address.hostname, e.getMessage());
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
            } catch (Throwable e) {
                LOG.warn("Execute batch plan fragments catch a exception, address={}:{}",
                        address.getHostname(), address.getPort(), e);
                throw new RpcException(address.hostname, e.getMessage());
            }
        }

        Preconditions.checkState(resultFuture != null);
        return resultFuture;
    }

    public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(
            TNetworkAddress address, TUniqueId queryId, TUniqueId finstId, PPlanFragmentCancelReason cancelReason,
            boolean isPipeline) throws RpcException {
        final PCancelPlanFragmentRequest pRequest = new PCancelPlanFragmentRequest();
        PUniqueId uid = new PUniqueId();
        uid.hi = finstId.hi;
        uid.lo = finstId.lo;
        pRequest.finstId = uid;
        pRequest.cancelReason = cancelReason;
        pRequest.isPipeline = isPipeline;
        PUniqueId qid = new PUniqueId();
        qid.hi = queryId.hi;
        qid.lo = queryId.lo;
        pRequest.queryId = qid;
        RpcCallback<PCancelPlanFragmentResult> callback = new EmptyRpcCallback<PCancelPlanFragmentResult>();
        try {
            final PBackendServiceAsync service = BrpcProxy.getBackendService(address);
            return service.cancelPlanFragment(pRequest, callback);
        } catch (NoSuchElementException e) {
            // retry
            try {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    // do nothing
                }
                final PBackendServiceAsync service = BrpcProxy.getBackendService(address);
                return service.cancelPlanFragment(pRequest, callback);
            } catch (NoSuchElementException noSuchElementException) {
                LOG.warn("Cancel plan fragment retry failed, address={}:{}",
                        address.getHostname(), address.getPort(), noSuchElementException);
                throw new RpcException(address.hostname, e.getMessage());
            }
        } catch (Throwable e) {
            LOG.warn("Cancel plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PFetchDataResult> fetchDataAsync(TNetworkAddress address, PFetchDataRequest request) throws RpcException {
        RpcCallback<PFetchDataResult> callback = new EmptyRpcCallback<PFetchDataResult>();
        RpcContext rpcContext = RpcContext.getContext();
        rpcContext.setReadTimeoutMillis(86400000);
        try {
            PBackendServiceAsync service = BrpcProxy.getBackendService(address);
            return service.fetchData(request, callback);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PTriggerProfileReportResult> triggerProfileReportAsync(
            TNetworkAddress address, PTriggerProfileReportRequest request) throws RpcException {
        RpcCallback<PTriggerProfileReportResult> callback = new EmptyRpcCallback<PTriggerProfileReportResult>();
        RpcContext rpcContext = RpcContext.getContext();
        rpcContext.setReadTimeoutMillis(10000);
        try {
            final PBackendServiceAsync service = BrpcProxy.getBackendService(address);
            return service.triggerProfileReport(request, callback);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PProxyResult> getInfo(TNetworkAddress address, PProxyRequest request) throws RpcException {
        RpcCallback<PProxyResult> callback = new EmptyRpcCallback<PProxyResult>();
        RpcContext rpcContext = RpcContext.getContext();
        rpcContext.setReadTimeoutMillis(10000);
        try {
            final PBackendServiceAsync service = BrpcProxy.getBackendService(address);
            return service.getInfo(request, callback);
        } catch (Throwable e) {
            LOG.warn("failed to get info, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    private static class SingletonHolder {
        private static final BackendServiceClient INSTANCE = new BackendServiceClient();
    }
}
