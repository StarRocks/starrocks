// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/action/SessionAction.java

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

package com.starrocks.http.action;

import com.google.common.collect.Lists;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.FrontendOpExecutor;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.system.Frontend;
import io.netty.handler.codec.http.HttpMethod;

import java.util.ArrayList;
import java.util.List;

public class SessionAction extends WebBaseAction {
    // we make 
    private static final ArrayList<String> SESSION_TABLE_HEADER = Lists.newArrayList();

    static {
        SESSION_TABLE_HEADER.add("FeHost");
        SESSION_TABLE_HEADER.add("Id");
        SESSION_TABLE_HEADER.add("User");
        SESSION_TABLE_HEADER.add("ClientHost");
        SESSION_TABLE_HEADER.add("Db");
        SESSION_TABLE_HEADER.add("Command");
        SESSION_TABLE_HEADER.add("ConnectionStartTime");
        SESSION_TABLE_HEADER.add("Time");
        SESSION_TABLE_HEADER.add("State");
        SESSION_TABLE_HEADER.add("Info");
        SESSION_TABLE_HEADER.add("IsPending");
    }

    private boolean isShowAll;

    public SessionAction(ActionController controller) {
        super(controller);
        isShowAll = false;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/session", new SessionAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        isShowAll = Boolean.parseBoolean(request.getSingleParameter("all"));
        getPageHeader(request, response.getContent());
        appendSessionInfo(response.getContent());
        getPageFooter(response.getContent());
        writeResponse(request, response);
    }

    private void appendSessionInfo(StringBuilder buffer) {
        buffer.append("<h2>Session Info</h2>");

        List<List<String>> rowSet = Lists.newArrayList();
        List<ConnectContext.ThreadInfo> threadInfos = ExecuteEnv.getInstance().getScheduler().listConnection(
                ConnectContext.get().getQualifiedUser());
        long nowMs = System.currentTimeMillis();
        for (ConnectContext.ThreadInfo info : threadInfos) {
            rowSet.add(info.toRow(nowMs, false));
        }
        if (isShowAll) {
            List<Frontend> frontends = GlobalStateMgr.getCurrentState().getFrontends(null);
            for (Frontend frontend : frontends) {
                if (frontend.getHost().equals(GlobalStateMgr.getCurrentState().getSelfNode().first)) {
                    continue;
                }
                String showProcStmt = "SHOW PROCESSLIST";
                // ConnectContext build in RestBaseAction
                ConnectContext context = ConnectContext.get();
                FrontendOpExecutor frontendOpExecutor = new FrontendOpExecutor(frontend.getHost(), frontend.getRpcPort(),
                        new OriginStatement(showProcStmt, 0), context, RedirectStatus.FORWARD_NO_SYNC);
                try {
                    frontendOpExecutor.execute();
                    rowSet.addAll(frontendOpExecutor.getProxyResultSet().getResultRows());
                } catch (Exception ignored) {
                }
            }
        }

        buffer.append("<p>This page lists the session info, there are "
                + rowSet.size()
                + " active sessions.</p>");

        appendTableHeader(buffer, SESSION_TABLE_HEADER);
        appendTableBody(buffer, rowSet);
        appendTableFooter(buffer);
    }

}
