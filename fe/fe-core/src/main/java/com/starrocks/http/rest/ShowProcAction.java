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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/ShowProcAction.java

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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.proc.ProcService;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.LeaderOpExecutor;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

// Format:
//   http://username:password@192.168.1.1:8030/api/show_proc?path=/
public class ShowProcAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(ShowProcAction.class);

    public ShowProcAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/show_proc", new ShowProcAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        // check authority
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        String path = request.getSingleParameter("path");
        String forward = request.getSingleParameter("forward");
        boolean isForward = false;
        if (!Strings.isNullOrEmpty(forward) && forward.equals("true")) {
            isForward = true;
        }

        // forward to master if necessary
        if (!GlobalStateMgr.getCurrentState().isLeader() && isForward) {
            String showProcStmt = "SHOW PROC \"" + path + "\"";
            // ConnectContext build in RestBaseAction
            ConnectContext context = ConnectContext.get();
            LeaderOpExecutor leaderOpExecutor = new LeaderOpExecutor(new OriginStatement(showProcStmt, 0), context,
                    RedirectStatus.FORWARD_NO_SYNC);
            LOG.debug("need to transfer to Leader. stmt: {}", context.getStmtId());

            try {
                leaderOpExecutor.execute();
            } catch (Exception e) {
                LOG.warn("failed to forward stmt", e);
                response.appendContent("Failed to forward stmt: " + e.getMessage());
                sendResult(request, response);
                return;
            }

            ShowResultSet resultSet = leaderOpExecutor.getProxyResultSet();
            if (resultSet == null) {
                response.appendContent("Failed to get result set");
                sendResult(request, response);
                return;
            }

            List<String> columnNames = resultSet.getMetaData().getColumns()
                    .stream().map(Column::getName).collect(Collectors.toList());
            List<List<String>> rows = resultSet.getResultRows();

            response.setContentType("application/json");
            response.getContent().append(formatResultToJson(columnNames, rows));

        } else {
            ProcNodeInterface procNode = null;
            ProcService instance = ProcService.getInstance();
            try {
                if (Strings.isNullOrEmpty(path)) {
                    procNode = instance.open("/");
                } else {
                    procNode = instance.open(path);
                }
            } catch (AnalysisException e) {
                LOG.warn(e.getMessage());
                response.getContent().append("[]");
            }

            if (procNode != null) {
                ProcResult result;
                try {
                    result = procNode.fetchResult();
                    List<String> columnNames = result.getColumnNames();
                    List<List<String>> rows = result.getRows();

                    response.setContentType("application/json");
                    response.getContent().append(formatResultToJson(columnNames, rows));
                } catch (AnalysisException e) {
                    LOG.warn(e.getMessage());
                    response.getContent().append("[]");
                }
            }
        }

        sendResult(request, response);
    }

    public String formatResultToJson(List<String> columnNames, List<List<String>> rows) {
        JsonArray jarray = new JsonArray();
        for (List<String> row : rows) {
            JsonObject jobject = new JsonObject();
            for (int i = 0; i < columnNames.size(); i++) {
                jobject.addProperty(columnNames.get(i), row.get(i));
            }
            jarray.add(jobject);
        }
        return jarray.toString();
    }
}
