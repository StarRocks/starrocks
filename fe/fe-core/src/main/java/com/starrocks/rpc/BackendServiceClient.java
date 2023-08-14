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

import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import com.starrocks.proto.ExecuteCommandRequestPB;
import com.starrocks.proto.ExecuteCommandResultPB;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PCollectQueryStatisticsResult;
import com.starrocks.proto.PExecBatchPlanFragmentsResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PGetFileSchemaResult;
import com.starrocks.proto.PListFailPointResponse;
import com.starrocks.proto.PMVMaintenanceTaskResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.PPulsarProxyRequest;
import com.starrocks.proto.PPulsarProxyResult;
import com.starrocks.proto.PTriggerProfileReportResult;
import com.starrocks.proto.PUniqueId;
import com.starrocks.proto.PUpdateFailPointStatusRequest;
import com.starrocks.proto.PUpdateFailPointStatusResponse;
import com.starrocks.rpc.PGetFileSchemaRequest;
import com.starrocks.thrift.TExecBatchPlanFragmentsParams;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TMVMaintenanceTasks;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

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
        pRequest.setRequest(tRequest);
        try {
            final PBackendService service = BrpcProxy.getBackendService(address);
            return service.execPlanFragmentAsync(pRequest);
        } catch (NoSuchElementException e) {
            try {
                // retry
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    // do nothing
                }
                final PBackendService service = BrpcProxy.getBackendService(address);
                return service.execPlanFragmentAsync(pRequest);
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
        pRequest.setRequest(tRequest);

        Future<PExecBatchPlanFragmentsResult> resultFuture = null;
        for (int i = 1; i <= Config.max_query_retry_time && resultFuture == null; ++i) {
            try {
                final PBackendService service = BrpcProxy.getBackendService(address);
                resultFuture = service.execBatchPlanFragmentsAsync(pRequest);
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
        try {
            final PBackendService service = BrpcProxy.getBackendService(address);
            return service.cancelPlanFragmentAsync(pRequest);
        } catch (NoSuchElementException e) {
            // retry
            try {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    // do nothing
                }
                final PBackendService service = BrpcProxy.getBackendService(address);
                return service.cancelPlanFragmentAsync(pRequest);
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
        try {
            PBackendService service = BrpcProxy.getBackendService(address);
            return service.fetchDataAsync(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PTriggerProfileReportResult> triggerProfileReportAsync(
            TNetworkAddress address, PTriggerProfileReportRequest request) throws RpcException {
        try {
            final PBackendService service = BrpcProxy.getBackendService(address);
            return service.triggerProfileReport(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PCollectQueryStatisticsResult> collectQueryStatisticsAsync(
            TNetworkAddress address, PCollectQueryStatisticsRequest request) throws RpcException {
        try {
            final PBackendService service = BrpcProxy.getBackendService(address);
            return service.collectQueryStatistics(request);
        } catch (Throwable e) {
            LOG.warn("collect query statistics catch an exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PProxyResult> getInfo(TNetworkAddress address, PProxyRequest request) throws RpcException {
        try {
            final PBackendService service = BrpcProxy.getBackendService(address);
            return service.getInfo(request);
        } catch (Throwable e) {
            LOG.warn("failed to get info, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PPulsarProxyResult> getPulsarInfo(
            TNetworkAddress address, PPulsarProxyRequest request) throws RpcException {
        try {
            final PBackendService service = BrpcProxy.getBackendService(address);
            return service.getPulsarInfo(request);
        } catch (Throwable e) {
            LOG.warn("failed to get info, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PGetFileSchemaResult> getFileSchema(
            TNetworkAddress address, PGetFileSchemaRequest request) throws RpcException {
        try {
            final PBackendService service = BrpcProxy.getBackendService(address);
            return service.getFileSchema(request);
        } catch (Throwable e) {
            LOG.warn("failed to get file schema, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PMVMaintenanceTaskResult> submitMVMaintenanceTaskAsync(
            TNetworkAddress address, TMVMaintenanceTasks tRequest)
            throws TException, RpcException {
        PMVMaintenanceTaskRequest pRequest = new PMVMaintenanceTaskRequest();
        pRequest.setRequest(tRequest);

        Future<PMVMaintenanceTaskResult> resultFuture = null;
        for (int i = 1; i <= Config.max_query_retry_time && resultFuture == null; ++i) {
            try {
                final PBackendService service = BrpcProxy.getBackendService(address);
                resultFuture = service.submitMVMaintenanceTaskAsync(pRequest);
            } catch (NoSuchElementException e) {
                // Retry `RETRY_TIMES`, when NoSuchElementException occurs.
                if (i >= Config.max_query_retry_time) {
                    LOG.warn("Submit MV Maintenance Task failed, address={}:{}",
                            address.getHostname(), address.getPort(), e);
                    throw new RpcException(address.hostname, e.getMessage());
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
            } catch (Throwable e) {
                LOG.warn("Submit MV Maintenance Task got an exception, address={}:{}",
                        address.getHostname(), address.getPort(), e);
                throw new RpcException(address.hostname, e.getMessage());
            }
        }

        Preconditions.checkState(resultFuture != null);
        return resultFuture;
    }

    public Future<ExecuteCommandResultPB> executeCommand(TNetworkAddress address, ExecuteCommandRequestPB request)
            throws RpcException {
        try {
            final PBackendService service = BrpcProxy.getBackendService(address);
            return service.executeCommandAsync(request);
        } catch (Throwable e) {
            LOG.warn("execute command exception, address={}:{} command:{}",
                    address.getHostname(), address.getPort(), request.command, e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PUpdateFailPointStatusResponse> updateFailPointStatusAsync(
            TNetworkAddress address, PUpdateFailPointStatusRequest request) throws RpcException {
        try {
            final PBackendService service = BrpcProxy.getBackendService(address);
            return service.updateFailPointStatusAsync(request);
        } catch (Throwable e) {
            LOG.warn("update failpoint status exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PListFailPointResponse> listFailPointAsync(
            TNetworkAddress address, PListFailPointRequest request) throws RpcException {
        try {
            final PBackendService service = BrpcProxy.getBackendService(address);
            return service.listFailPointAsync(request);
        } catch (Throwable e) {
            LOG.warn("list failpoint exception, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    private static class SingletonHolder {
        private static final BackendServiceClient INSTANCE = new BackendServiceClient();
    }
}
