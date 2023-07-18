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

package com.starrocks.qe;

import com.google.api.client.util.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.proto.PUpdateFailPointStatusRequest;
import com.starrocks.proto.PUpdateFailPointStatusResponse;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateFailPointStatusStatement;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FailPointExecutor {
    private static final Logger LOG = LogManager.getLogger(FailPointExecutor.class);

    private ConnectContext connectContext;
    private StatementBase stmt;
    private SystemInfoService clusterInfoService;

    public FailPointExecutor(ConnectContext connectContext, StatementBase stmt) {
        this.connectContext = connectContext;
        this.stmt = stmt;
        clusterInfoService = GlobalStateMgr.getCurrentSystemInfo();
    }

    public void execute() throws Exception {
        if (stmt instanceof UpdateFailPointStatusStatement) {
            handleUpdateFailPointStatus();
        }
    }

    public void handleUpdateFailPointStatus() throws Exception {
        UpdateFailPointStatusStatement updateStmt = (UpdateFailPointStatusStatement) stmt;
        PUpdateFailPointStatusRequest request = new PUpdateFailPointStatusRequest();
        request.failPointName = updateStmt.getName();
        request.triggerMode = updateStmt.getFailPointMode();

        List<TNetworkAddress> backends = new LinkedList<TNetworkAddress>();
        if (updateStmt.getBackends() == null) {
            // effect all backends
            List<Long> backendIds = clusterInfoService.getBackendIds(true);
            if (backendIds == null) {
                throw new AnalysisException("No alive backends");
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
                    throw new AnalysisException("invalid backend addr");
                }
                Backend backend = clusterInfoService.getBackendWithBePort(tmp[0], Integer.parseInt(tmp[1]));
                if (backend == null) {
                    throw new AnalysisException("cannot find backend with addr " + backendAddr);
                }
                backends.add(backend.getBrpcAddress());
            }
        }

        // send request
        List<Pair<TNetworkAddress, Future<PUpdateFailPointStatusResponse>>> futures = Lists.newArrayList();
        for (TNetworkAddress address : backends) {
            try {
                LOG.info("send request to " + address.getHostname() + ":" + address.getPort());
                futures.add(Pair.create(address,
                        BackendServiceClient.getInstance().updateFailPointStatusAsync(address, request)));
            } catch (RpcException e) {
                throw new AnalysisException("sending update failpoint status request fails");
            }
        }
        // handle response
        for (Pair<TNetworkAddress, Future<PUpdateFailPointStatusResponse>> future : futures) {
            try {
                final PUpdateFailPointStatusResponse result = future.second.get(10, TimeUnit.SECONDS);
                if (result != null && result.status.statusCode != TStatusCode.OK.getValue()) {
                    TNetworkAddress address = future.first;
                    String errMsg = String.format("update failpoint status failed, backend: %s:%d, error: %s",
                            address.getHostname(), address.getPort(), result.status.errorMsgs.get(0));
                    LOG.warn(errMsg);
                    throw new AnalysisException(errMsg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                throw new AnalysisException(e.getMessage());
            }
        }
    }

}
