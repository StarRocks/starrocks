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

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
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
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StreamLoadMetaAction extends RestBaseAction {

    private static final Logger LOG = LogManager.getLogger(StreamLoadMetaAction.class);

    // batch write stream load parameters =====================================================

    // The parameter name to specify the type of metas.
    private static final String TYPE_PARAM_NAME = "type";

    // The type name for nodes meta. The response will return available BE nodes that can accept load requests.
    // You can also set the parameter NODE_SERVICE_PARAM to specify the required service the nodes can provide.
    // There will be a key named "nodes" in the response json, and the value is also a json which contains fields
    // for each service type. For example, if you want both http and brpc service, the response will be like:
    // "nodes": {
    //      "http":"be_ip1:http_port1,be_ip2:http_port2",
    //      "brpc":"be_ip1:brpc_port1,be_ip2:brpc_port2"
    // }
    private static final String NODES_TYPE = "nodes";

    // The parameter name to specify the service type of the nodes. The value can be "http" or "brpc".
    // If not set this parameter, the response will return all service types. Each type will be a field
    // in the json, such as "http":"be_ip1:http_port1,be_ip2:http_port2" for http service, and
    // "brpc":"be_ip1:brpc_port1,be_ip2:brpc_port2" for brpc service
    private static final String NODE_SERVICE_PARAM = "node_service";

    public StreamLoadMetaAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET,
                "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load_meta",
                new StreamLoadMetaAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException,
            AccessDeniedException {
        boolean enableBatchWrite = "true".equalsIgnoreCase(
                request.getRequest().headers().get(StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE));
        if (enableBatchWrite && redirectToLeader(request, response)) {
            return;
        }

        String dbName = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            StreamLoadMetaResult responseResult = new StreamLoadMetaResult(
                    TStatusCode.INVALID_ARGUMENT.name(), ActionStatus.FAILED, "No database selected");
            sendResult(request, response, responseResult);
        }

        String tableName = request.getSingleParameter(TABLE_KEY);
        if (Strings.isNullOrEmpty(tableName)) {
            StreamLoadMetaResult responseResult = new StreamLoadMetaResult(
                    TStatusCode.INVALID_ARGUMENT.name(), ActionStatus.FAILED, "No table selected");
            sendResult(request, response, responseResult);
        }

        Authorizer.checkTableAction(ConnectContext.get(), dbName, tableName, PrivilegeType.INSERT);

        if (!enableBatchWrite) {
            processNormalStreamLoad(request, response, dbName, tableName);
        } else {
            processBatchWriteStreamLoad(request, response, dbName, tableName);
        }
    }

    private void processNormalStreamLoad(BaseRequest request, BaseResponse response, String dbName, String tableName) {
        // currently there is no meta for normal stream load, and just return a success response.
        StreamLoadMetaResult responseResult =
                new StreamLoadMetaResult(TStatusCode.OK.name(), ActionStatus.OK, "");
        sendResult(request, response, responseResult);
    }

    private void processBatchWriteStreamLoad(
            BaseRequest request, BaseResponse response, String dbName, String tableName) {
        TableId tableId = new TableId(dbName, tableName);
        StreamLoadKvParams params = StreamLoadKvParams.fromHttpHeaders(request.getRequest().headers());
        RequestCoordinatorBackendResult result = GlobalStateMgr.getCurrentState()
                .getBatchWriteMgr().requestCoordinatorBackends(tableId, params);
        if (!result.isOk()) {
            StreamLoadMetaResult responseResult = new StreamLoadMetaResult(
                    result.getStatus().status_code.name(), ActionStatus.FAILED,
                    result.getStatus().error_msgs.get(0));
            sendResult(request, response, responseResult);
            return;
        }

        List<ComputeNode> nodes = result.getValue();
        StreamLoadMetaResult responseResult =
                new StreamLoadMetaResult(TStatusCode.OK.name(), ActionStatus.OK, "");
        List<String> metaTypeList = request.getArrayParameter(TYPE_PARAM_NAME);
        boolean allMetas = metaTypeList == null || metaTypeList.isEmpty();

        if (allMetas || metaTypeList.contains(NODES_TYPE)) {
            List<String> serviceTypeList = request.getArrayParameter(NODE_SERVICE_PARAM);
            boolean allServiceTypes = serviceTypeList == null || serviceTypeList.isEmpty();
            if (allServiceTypes || serviceTypeList.contains("http")) {
                String httpNodes = nodes.stream()
                        .map(node -> node.getHost() + ":" + node.getHttpPort())
                        .collect(Collectors.joining(","));
                responseResult.addNodesMeta("http", httpNodes);
            }
            if (allServiceTypes || serviceTypeList.contains("brpc")) {
                String brpcNodes = nodes.stream()
                        .map(node -> node.getHost() + ":" + node.getBrpcPort())
                        .collect(Collectors.joining(","));
                responseResult.addNodesMeta("brpc", brpcNodes);
            }
        }
        sendResult(request, response, responseResult);
    }

    public static class StreamLoadMetaResult extends RestBaseResult {

        // metas for batch write ========================================

        // service type -> nodes list in format "be_ip1:port1,be_ip2:port2"
        @SerializedName(NODES_TYPE)
        private Map<String, String> nodesMap;

        public StreamLoadMetaResult(String code, ActionStatus status, String msg) {
            super(code, status, msg);
        }

        public void addNodesMeta(String serviceType, String nodes) {
            if (nodesMap == null) {
                nodesMap = new HashMap<>();
            }
            nodesMap.put(serviceType, nodes);
        }
    }
}
