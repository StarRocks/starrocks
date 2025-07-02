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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/BootstrapFinishAction.java

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
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Version;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.monitor.jvm.JvmStats;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
 * fe_host:fe_http_port/api/bootstrap
 * return:
 * {"status":"OK","msg":"Success","replayedJournal"=123456, "queryPort"=9000, "rpcPort"=9001}
 * {"status":"FAILED","msg":"err info..."}
 */
public class BootstrapFinishAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(BootstrapFinishAction.class);

    private static final String TOKEN = "token";

    public BootstrapFinishAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/bootstrap", new BootstrapFinishAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        boolean isReady = GlobalStateMgr.getCurrentState().isReady();

        // to json response
        BootstrapResult result;
        if (isReady) {
            result = new BootstrapResult();
            String token = request.getSingleParameter(TOKEN);
            if (!Strings.isNullOrEmpty(token)) {
                if (result.status == ActionStatus.OK) {
                    if (!token.equals(GlobalStateMgr.getCurrentState().getNodeMgr().getToken())) {
                        result.status = ActionStatus.FAILED;
                        LOG.info("invalid token: {}", token);
                        result.msg = "invalid parameter";
                    }
                }

                if (result.status == ActionStatus.OK) {
                    // cluster id and token are valid, return replayed journal id
                    long replayedJournalId = GlobalStateMgr.getCurrentState().getReplayedJournalId();
                    long feStartTime = GlobalStateMgr.getCurrentState().getFeStartTime();
                    result.setReplayedJournal(replayedJournalId);
                    result.setQueryPort(Config.query_port);
                    result.setRpcPort(Config.rpc_port);
                    result.setFeStartTime(feStartTime);
                    result.setFeVersion(Version.STARROCKS_VERSION + "-" + Version.STARROCKS_COMMIT_HASH);
                    result.setHeapUsedPercent(JvmStats.getJvmHeapUsedPercent());
                }
            }
        } else {
            result = new BootstrapResult("not ready");
        }

        // send result
        response.setContentType("application/json");
        response.getContent().append(result.toJson());
        sendResult(request, response);
    }

    public static class BootstrapResult extends RestBaseResult {
        @SerializedName("replayedJournalId")
        private long replayedJournalId = 0;
        @SerializedName("queryPort")
        private int queryPort = 0;
        @SerializedName("rpcPort")
        private int rpcPort = 0;
        @SerializedName("feStartTime")
        private long feStartTime = 0;
        @SerializedName("feVersion")
        private String feVersion;
        @SerializedName("heapUsedPercent")
        private float heapUsedPercent;

        public BootstrapResult() {
            super();
        }

        public BootstrapResult(String msg) {
            super(msg);
        }

        public void setReplayedJournal(long replayedJournalId) {
            this.replayedJournalId = replayedJournalId;
        }

        public long getReplayedJournal() {
            return replayedJournalId;
        }

        public void setQueryPort(int queryPort) {
            this.queryPort = queryPort;
        }

        public int getQueryPort() {
            return queryPort;
        }

        public void setRpcPort(int rpcPort) {
            this.rpcPort = rpcPort;
        }

        public int getRpcPort() {
            return rpcPort;
        }

        public long getFeStartTime() {
            return feStartTime;
        }

        public void setFeStartTime(long feStartTime) {
            this.feStartTime = feStartTime;
        }

        public String getFeVersion() {
            return feVersion;
        }

        public void setFeVersion(String feVersion) {
            this.feVersion = feVersion;
        }

        public float getHeapUsedPercent() {
            return heapUsedPercent;
        }

        public void setHeapUsedPercent(float heapUsedPercent) {
            this.heapUsedPercent = heapUsedPercent;
        }

        @Override
        public String toJson() {
            Gson gson = new Gson();
            return gson.toJson(this);
        }

        public static BootstrapResult fromJson(String jsonStr) {
            Gson gson = new Gson();
            return gson.fromJson(jsonStr, BootstrapResult.class);
        }
    }
}
