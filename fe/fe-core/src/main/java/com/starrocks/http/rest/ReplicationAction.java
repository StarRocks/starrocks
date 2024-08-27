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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/HealthAction.java

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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.replication.ReplicationJob;
import com.starrocks.replication.ReplicationMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.handler.codec.http.HttpMethod;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ReplicationAction extends RestBaseAction {
    public ReplicationAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/replication", new ReplicationAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws AccessDeniedException {
        // check authority
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        String type = request.getSingleParameter("type");
        if (type.equals("cancel_all")) {
            GlobalStateMgr.getCurrentState().getReplicationMgr().cancelRunningJobs();
            RestBaseResult result = new RestBaseResult();
            sendCustomResult(result, request, response);
        } else if (type.equals("cancel")) {
            long tableId = Long.parseLong(request.getSingleParameter("table_id"));
            boolean res = GlobalStateMgr.getCurrentState().getReplicationMgr().cancelRunningJob(tableId);
            if (res) {
                RestBaseResult result = new RestBaseResult();
                sendCustomResult(result, request, response);
            } else {
                RestBaseResult result = new RestBaseResult("Failed to kill the replication job of table " + tableId);
                sendCustomResult(result, request, response);
            }

        } else if (type.equals("show")) {
            ReplicationMgr replicationMgr = GlobalStateMgr.getCurrentState().getReplicationMgr();

            List<String> abortedIds = replicationMgr.getAbortedJobs().stream()
                    .map(ReplicationJob::getJobId).collect(Collectors.toList());
            List<String> committedIds = replicationMgr.getCommittedJobs().stream()
                    .map(ReplicationJob::getJobId).collect(Collectors.toList());

            JsonObject root = new JsonObject();
            addIdArray(root, "abortedIds", abortedIds);
            addIdArray(root, "committedIds", committedIds);
            addReplicationJobs(root, "running", replicationMgr.getRunningJobs());

            // send result
            response.setContentType("application/json");
            response.getContent().append(root);
            sendResult(request, response);
        }

    }

    public void sendCustomResult(RestBaseResult result, BaseRequest request, BaseResponse response) {
        response.setContentType("application/json");
        response.getContent().append(result.toJson());
        sendResult(request, response);
    }

    private void addIdArray(JsonObject root, String name, List<String> ids) {
        JsonArray jAbortedIds = new JsonArray();
        for (String id : ids) {
            jAbortedIds.add(id);
        }
        root.add(name, jAbortedIds);
    }

    private void addReplicationJobs(JsonObject root, String name, Collection<ReplicationJob> running) {
        JsonArray jRunningJobs = new JsonArray();
        for (ReplicationJob job : running) {
            String jobId = job.getJobId();
            long txnId = job.getTransactionId();
            String state = job.getState().name();
            int finishedTaskNum = job.getFinishedTaskNum();
            int totalTaskNum = job.getTaskNum();
            JsonObject o = new JsonObject();
            o.addProperty("jobId", jobId);
            o.addProperty("txnId", txnId);
            o.addProperty("state", state);
            o.addProperty("finishedTaskNum", finishedTaskNum);
            o.addProperty("totalTaskNum", totalTaskNum);
            jRunningJobs.add(o);
        }
        root.add(name, jRunningJobs);
    }
}
