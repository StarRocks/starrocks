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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.system.Frontend;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class FeThreadsAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(FeThreadsAction.class);
    public static final String API_PATH = "/rest/v1/fe_threads";
    private static final String FOR_USER_PARAM = "for_user";
    private static final String SHOW_FULL_PARAM = "show_full";

    public FeThreadsAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, API_PATH, new FeThreadsAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        String forUser = request.getSingleParameter(FOR_USER_PARAM);
        if (Strings.isNullOrEmpty(forUser)) {
            forUser = null;
        }
        boolean showFull = Boolean.parseBoolean(request.getSingleParameter(SHOW_FULL_PARAM));

        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        context.setCurrentRoleIds(UserIdentity.ROOT);
        context.setThreadLocalInfo();

        List<ConnectContext.ThreadInfo> threadInfos =
                ExecuteEnv.getInstance().getScheduler().listConnection(context, forUser);
        long nowMs = System.currentTimeMillis();
        List<List<String>> rows = Lists.newArrayList();
        for (ConnectContext.ThreadInfo info : threadInfos) {
            List<String> row = info.toRow(nowMs, showFull);
            if (row != null) {
                rows.add(row);
            }
        }

        Frontend frontend = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf();
        Map<String, Object> payload = ImmutableMap.of(
                "fe_id", frontend != null ? frontend.getNodeName() : "",
                "threads", rows);

        try {
            response.setContentType(JSON_CONTENT_TYPE);
            response.getContent().append(mapper.writeValueAsString(payload));
            sendResult(request, response);
        } catch (JsonProcessingException e) {
            LOG.warn("failed to serialize fe threads response", e);
            response.getContent().append(new RestBaseResult("failed to build fe threads response").toJson());
            sendResult(request, response);
        } finally {
            ConnectContext.remove();
        }
    }
}
