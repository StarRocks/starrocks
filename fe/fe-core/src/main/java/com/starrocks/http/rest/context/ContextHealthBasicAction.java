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

package com.starrocks.http.rest.context;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.starrocks.common.DdlException;
import com.starrocks.context.ContextMetaManager;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;

/**
 * {@code GET /api/context/health/basic}. Minimal readiness probe that exposes only the
 * leadership and internal-tables flags — no per-module counts. Authorized for any caller that
 * holds USAGE on at least one contextbase, so module tenants can poll readiness without the
 * operator-level CREATE_CONTEXTBASE privilege required by {@link ContextHealthAction}.
 */
public class ContextHealthBasicAction extends RestBaseAction {

    public ContextHealthBasicAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/context/health/basic",
                new ContextHealthBasicAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, com.starrocks.authorization.AccessDeniedException {
        if (!ContextRestAuth.hasAnyContextBaseUsage(ConnectContext.get())) {
            throw new com.starrocks.authorization.AccessDeniedException(
                    "USAGE on at least one contextbase is required to probe basic health");
        }
        BasicHealthResponse resp = new BasicHealthResponse();
        resp.requestId = ContextRestAuth.currentRequestId();
        resp.isLeader = GlobalStateMgr.getCurrentState().isLeader();
        ContextMetaManager metaMgr = GlobalStateMgr.getCurrentState().getContextMetaManager();
        resp.internalTablesReady = metaMgr != null && metaMgr.isReady();
        resp.healthy = resp.isLeader && resp.internalTablesReady;
        sendResultByJson(request, response, resp);
    }

    private static final class BasicHealthResponse {
        @JsonProperty("request_id")
        public String requestId;

        public boolean healthy;

        @JsonProperty("is_leader")
        public boolean isLeader;

        @JsonProperty("internal_tables_ready")
        public boolean internalTablesReady;
    }
}
