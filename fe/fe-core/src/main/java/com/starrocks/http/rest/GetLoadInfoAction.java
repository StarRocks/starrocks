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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/GetLoadInfoAction.java

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
import com.starrocks.load.Load;
import com.starrocks.load.LoadJobRedaction;
import com.starrocks.load.LoadJobVisibility;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import io.netty.handler.codec.http.HttpMethod;

// Get load information of one load job
public class GetLoadInfoAction extends RestBaseAction {
    public GetLoadInfoAction(ActionController controller, boolean isStreamLoad) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        GetLoadInfoAction action = new GetLoadInfoAction(controller, false);
        controller.registerHandler(HttpMethod.GET, "/api/{" + DB_KEY + "}/_load_info", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        Load.JobInfo info = new Load.JobInfo(request.getSingleParameter(DB_KEY),
                request.getSingleParameter(LABEL_KEY));
        if (Strings.isNullOrEmpty(info.dbName)) {
            throw new DdlException("No database selected");
        }
        if (Strings.isNullOrEmpty(info.label)) {
            throw new DdlException("No label selected");
        }

        if (redirectToLeader(request, response)) {
            return;
        }

        Authorizer.checkActionInDb(ConnectContext.get(), info.dbName, PrivilegeType.INSERT);
        LoadJob loadJob = globalStateMgr.getLoadMgr().getLoadJobInfo(info);
        // The result carries the job's error-log URL and fail message, so it goes in full only to a user who may
        // read the rows the job loads. With the rule off the database-level check above is the only one, as before
        // it: the per-table check that preceded it ran while the job's tables were still unknown.
        if (LoadJobVisibility.isEnabled()) {
            LoadJobVisibility.Access access = LoadJobVisibility.accessOf(ConnectContext.get(), loadJob);
            if (access == LoadJobVisibility.Access.NONE) {
                throw new AccessDeniedException();
            }
            if (access != LoadJobVisibility.Access.FULL) {
                LoadJobRedaction.redact(info, loadJob.getFailMsg());
            }
        }

        sendResult(request, response, new Result(info));
    }

    private static class Result extends RestBaseResult {
        private Load.JobInfo jobInfo;

        public Result(Load.JobInfo info) {
            jobInfo = info;
        }
    }
}
