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

package com.starrocks.failpoint;

import com.google.api.client.util.Lists;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.proto.PUpdateFailPointStatusRequest;
import com.starrocks.proto.PUpdateFailPointStatusResponse;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateFailPointStatusStatement;
import com.starrocks.system.Backend;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUpdateFailPointResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FailPointExecutor {
    private static final Logger LOG = LogManager.getLogger(FailPointExecutor.class);

    private StatementBase stmt;
    private SystemInfoService clusterInfoService;

    public FailPointExecutor(StatementBase stmt) {
        this.stmt = stmt;
        clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
    }

    public void execute() throws Exception {
        if (stmt instanceof UpdateFailPointStatusStatement) {
            handleUpdateFailPointStatus();
        }
    }

    public void handleUpdateFailPointStatus() throws Exception {
        UpdateFailPointStatusStatement updateStmt = (UpdateFailPointStatusStatement) stmt;
        if (updateStmt.isForFrontend()) {
            updateFrontendFailPoint(updateStmt);
        } else {
            updateBackendFailPoint(updateStmt);
        }
    }

    private void updateBackendFailPoint(UpdateFailPointStatusStatement updateStmt) throws Exception {
        PUpdateFailPointStatusRequest request = updateStmt.toProto();

        List<TNetworkAddress> backends = new LinkedList<>();
        if (updateStmt.getBackends() == null || updateStmt.getBackends().isEmpty()) {
            // effect all backends
            List<Long> backendIds = clusterInfoService.getBackendIds(true);
            if (backendIds == null) {
                throw new DdlException("No alive backends");
            }
            for (long backendId : backendIds) {
                Backend backend = clusterInfoService.getBackend(backendId);
                if (backend == null) {
                    continue;
                }
                backends.add(backend.getBrpcAddress());
            }
        } else {
            for (String backendAddr : updateStmt.getBackends()) {
                String[] tmp = backendAddr.split(":");
                if (tmp.length != 2) {
                    throw new SemanticException("invalid backend addr");
                }
                Backend backend = clusterInfoService.getBackendWithBePort(tmp[0], Integer.parseInt(tmp[1]));
                if (backend == null) {
                    throw new SemanticException("cannot find backend with addr " + backendAddr);
                }
                backends.add(backend.getBrpcAddress());
            }
        }

        // send request
        List<Pair<TNetworkAddress, Future<PUpdateFailPointStatusResponse>>> futures = Lists.newArrayList();
        for (TNetworkAddress address : backends) {
            try {
                futures.add(Pair.create(address,
                        BackendServiceClient.getInstance().updateFailPointStatusAsync(address, request)));
            } catch (RpcException e) {
                throw new DdlException("sending update failPoint status request failed");
            }
        }
        // handle response
        for (Pair<TNetworkAddress, Future<PUpdateFailPointStatusResponse>> future : futures) {
            try {
                final PUpdateFailPointStatusResponse result = future.second.get(10, TimeUnit.SECONDS);
                if (result != null && result.status.statusCode != TStatusCode.OK.getValue()) {
                    TNetworkAddress address = future.first;
                    String errMsg = String.format("update failPoint status failed, backend: %s:%d, error: %s",
                            address.getHostname(), address.getPort(), result.status.errorMsgs.get(0));
                    LOG.warn(errMsg);
                    throw new DdlException(errMsg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                LOG.warn("update failPoint for backend: {} failed", future.first, e);
                throw e;
            }
        }
    }

    private void updateFrontendFailPoint(UpdateFailPointStatusStatement updateStmt) throws DdlException {
        if (!FailPoint.isEnabled()) {
            throw new DdlException("fail point is not enabled, please start fe with --failpoint option");
        }
        if (updateStmt.getIsEnable()) {
            FailPoint.setTriggerPolicy(updateStmt.getName(), updateStmt.getTriggerPolicy());
        } else {
            FailPoint.removeTriggerPolicy(updateStmt.getName());
        }

        StringBuilder errorMsg = new StringBuilder();
        List<Frontend> frontends = GlobalStateMgr.getCurrentState().getNodeMgr().getOtherFrontends();
        for (Frontend fe : frontends) {
            if (!fe.isAlive()) {
                errorMsg.append("failed to update failPoint for frontend: ").append(fe.getHost())
                        .append(", because not alive. ");
                continue;
            }

            try {
                TUpdateFailPointResponse resp = ThriftRPCRequestExecutor.call(ThriftConnectionPool.frontendPool,
                        new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                        client -> client.updateFailPointStatus(updateStmt.toThrift()));
                TStatus status = resp.getStatus();
                if (status.getStatus_code() != TStatusCode.OK) {
                    if (status.getError_msgs() != null) {
                        errorMsg.append("failed to update failPoint for frontend: ").append(fe.getHost())
                                .append(", because: ").append(String.join(",", status.getError_msgs())).append(". ");
                    }
                }
            } catch (TException e) {
                errorMsg.append("failed to update failPoint for frontend: ")
                        .append(fe.getHost()).append(", because: ").append(e.getMessage()).append(". ");
            }
        }
        if (!errorMsg.isEmpty()) {
            LOG.warn(errorMsg.toString());
            throw new DdlException(errorMsg.toString());
        }
    }
}
