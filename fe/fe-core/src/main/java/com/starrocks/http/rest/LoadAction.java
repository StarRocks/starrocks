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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/LoadAction.java

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

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.load.batchwrite.RequestCoordinatorBackendResult;
import com.starrocks.load.batchwrite.TableId;
import com.starrocks.load.streamload.StreamLoadHttpHeader;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class LoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(LoadAction.class);

    public LoadAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.PUT,
                "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load",
                new LoadAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException, AccessDeniedException {
        try {
            executeWithoutPasswordInternal(request, response);
        } catch (DdlException e) {
            TransactionResult resp = new TransactionResult();
            resp.status = ActionStatus.FAILED;
            resp.msg = e.getClass() + ": " + e.getMessage();
            String firstStackTrace = "<null>";
            Optional<StackTraceElement> stElem = Arrays.stream(e.getStackTrace()).findFirst();
            if (stElem.isPresent()) {
                firstStackTrace = stElem.get().toString();
            }
            LOG.warn("Failed to execute executeWithoutPasswordInternal: {}, The most inner stack: {}",
                    e.getMessage(), firstStackTrace);

            sendResult(request, response, resp);
        }
    }

    // Basically a complete copy of the private interface HttpUtil.isExpectHeaderValid.
    private static boolean isExpectHeaderValid(final HttpRequest message) {
        /*
         * Expect: 100-continue is for requests only and it works only on HTTP/1.1 or later. Note further that RFC 7231
         * section 5.1.1 says "A server that receives a 100-continue expectation in an HTTP/1.0 request MUST ignore
         * that expectation."
         */
        return message.protocolVersion().compareTo(HttpVersion.HTTP_1_1) >= 0;
    }

    public void executeWithoutPasswordInternal(BaseRequest request, BaseResponse response) throws DdlException,
            AccessDeniedException {

        // A 'Load' request must have "Expect: 100-continue" header for HTTP/1.1 and onward.
        // Skip the "Expect" header check for HTTP/1.0 and earlier versions.
        if (isExpectHeaderValid(request.getRequest()) && !HttpUtil.is100ContinueExpected(request.getRequest())) {
            // TODO: should respond "HTTP 417 Expectation Failed"
            response.setForceCloseConnection(true);
            throw new DdlException("There is no 100-continue header");
        }
        // close the connection forcibly after the request, so the `Expect: 100-Continue` won't
        // affect subsequent requests processing.
        response.setForceCloseConnection(true);

        boolean enableBatchWrite = "true".equalsIgnoreCase(
                request.getRequest().headers().get(StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE));
        if (enableBatchWrite && redirectToLeader(request, response)) {
            return;
        }

        String dbName = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No database selected.");
        }

        String tableName = request.getSingleParameter(TABLE_KEY);
        if (Strings.isNullOrEmpty(tableName)) {
            throw new DdlException("No table selected.");
        }

        Authorizer.checkTableAction(ConnectContext.get(), dbName, tableName, PrivilegeType.INSERT);

        if (!enableBatchWrite) {
            processNormalStreamLoad(request, response, dbName, tableName);
        } else {
            processBatchWriteStreamLoad(request, response, dbName, tableName);
        }
    }

    private void processNormalStreamLoad(
            BaseRequest request, BaseResponse response, String dbName, String tableName) throws DdlException {
        String label = request.getRequest().headers().get(LABEL_KEY);

        String warehouseName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        if (request.getRequest().headers().contains(WAREHOUSE_KEY)) {
            warehouseName = request.getRequest().headers().get(WAREHOUSE_KEY);
        }

        // Choose a backend sequentially, or choose a cn in shared_data mode
        List<Long> nodeIds = new ArrayList<>();
        if (RunMode.isSharedDataMode()) {
            List<Long> computeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(warehouseName);
            for (long nodeId : computeIds) {
                ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                if (node != null && node.isAvailable()) {
                    nodeIds.add(nodeId);
                }
            }
            Collections.shuffle(nodeIds);
        } else {
            SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            nodeIds = systemInfoService.getNodeSelector().seqChooseBackendIds(1, true, false, null);
        }

        if (CollectionUtils.isEmpty(nodeIds)) {
            throw new DdlException("No backend alive.");
        }

        // TODO: need to refactor after be split into cn + dn
        ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeIds.get(0));
        if (node == null) {
            throw new DdlException("No backend or compute node alive.");
        }

        TNetworkAddress redirectAddr = new TNetworkAddress(node.getHost(), node.getHttpPort());

        LOG.info("redirect load action to destination={}, db: {}, tbl: {}, label: {}, warehouse: {}",
                redirectAddr.toString(), dbName, tableName, label, warehouseName);
        redirectTo(request, response, redirectAddr);
    }

    private void processBatchWriteStreamLoad(
            BaseRequest request, BaseResponse response, String dbName, String tableName) throws DdlException {
        TableId tableId = new TableId(dbName, tableName);
        StreamLoadKvParams params = StreamLoadKvParams.fromHttpHeaders(request.getRequest().headers());
        RequestCoordinatorBackendResult result = GlobalStateMgr.getCurrentState()
                .getBatchWriteMgr().requestCoordinatorBackends(tableId, params);
        if (!result.isOk()) {
            BatchWriteResponseResult responseResult = new BatchWriteResponseResult(
                    result.getStatus().status_code.name(), ActionStatus.FAILED,
                    result.getStatus().error_msgs.get(0));
            sendResult(request, response, responseResult);
            return;
        }

        List<ComputeNode> nodes = result.getValue();
        int index = ThreadLocalRandom.current().nextInt(nodes.size());
        ComputeNode node = nodes.get(index);
        TNetworkAddress redirectAddr = new TNetworkAddress(node.getHost(), node.getHttpPort());
        LOG.info("redirect batch write to destination={}, db: {}, tbl: {}", redirectAddr, dbName, tableName);
        redirectTo(request, response, redirectAddr);
    }

    public static class BatchWriteResponseResult extends RestBaseResult {

        public BatchWriteResponseResult(String code, ActionStatus status, String msg) {
            super(code, status, msg);
        }
    }
}

