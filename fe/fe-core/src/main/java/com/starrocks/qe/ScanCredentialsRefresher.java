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

import com.starrocks.common.util.DebugUtil;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TRefreshScanCredentialsRequest;
import com.starrocks.thrift.TRefreshScanCredentialsResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

/**
 * Serves FrontendService.refreshScanCredentials: a backend whose scan credentials are due for refresh asks
 * the coordinator of its query for the scan node's current ones.
 *
 * <p>Any failure leaves the backend on the credentials it already has, which is how every scan behaved before
 * credentials could be refreshed at all.
 */
public final class ScanCredentialsRefresher {
    private static final Logger LOG = LogManager.getLogger(ScanCredentialsRefresher.class);

    private ScanCredentialsRefresher() {
    }

    public static TRefreshScanCredentialsResponse refresh(TRefreshScanCredentialsRequest request) {
        return refresh(request, QeProcessorImpl.INSTANCE::getCoordinator);
    }

    static TRefreshScanCredentialsResponse refresh(TRefreshScanCredentialsRequest request,
                                                   Function<TUniqueId, Coordinator> coordinators) {
        if (request == null || !request.isSetQuery_id() || !request.isSetPlan_node_id()
                || !request.isSetRefresh_token()) {
            return failed(TStatusCode.INVALID_ARGUMENT, "query_id, plan_node_id and refresh_token are required");
        }
        String queryId = DebugUtil.printId(request.getQuery_id());
        Coordinator coordinator = coordinators.apply(request.getQuery_id());
        if (!(coordinator instanceof DefaultCoordinator defaultCoordinator)) {
            return failed(TStatusCode.NOT_FOUND, "query " + queryId + " is not running here");
        }
        ScanNode scanNode = coordinator.getScanNodes().stream()
                .filter(node -> node.getId().asInt() == request.getPlan_node_id())
                .findFirst().orElse(null);
        // The token is the proof: ids are derivable and printed in logs, the token only travels with the
        // credentials it renews. Same answer for an unknown node, so neither can be probed.
        if (scanNode == null || !scanNode.acceptsCredentialRefreshToken(request.getRefresh_token())) {
            return failed(TStatusCode.NOT_FOUND,
                    "query " + queryId + " has no scan node " + request.getPlan_node_id() + " with that token");
        }
        try {
            TCloudConfiguration configuration = scanNode.refreshScanCredentials(defaultCoordinator.getConnectContext());
            if (configuration == null) {
                return failed(TStatusCode.NOT_IMPLEMENTED_ERROR,
                        "scan node " + request.getPlan_node_id() + " has no refreshable credentials");
            }
            return new TRefreshScanCredentialsResponse().setStatus(new TStatus(TStatusCode.OK))
                    .setCloud_configuration(configuration);
        } catch (Exception e) {
            LOG.warn("Failed to refresh the credentials of scan node {} of query {}; the backend keeps its current"
                    + " ones", request.getPlan_node_id(), queryId, e);
            return failed(TStatusCode.INTERNAL_ERROR, e.getMessage());
        }
    }

    private static TRefreshScanCredentialsResponse failed(TStatusCode code, String message) {
        TStatus status = new TStatus(code);
        status.addToError_msgs(message == null ? code.name() : message);
        return new TRefreshScanCredentialsResponse().setStatus(status);
    }
}
