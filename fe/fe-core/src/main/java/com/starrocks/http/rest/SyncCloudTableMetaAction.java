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
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

// This class is responsible for sync cloud table's tablet and shard meta between fe and starmgr
public class SyncCloudTableMetaAction extends RestBaseAction {
    private static final String FORCE = "force";

    public SyncCloudTableMetaAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        SyncCloudTableMetaAction action = new SyncCloudTableMetaAction(controller);
        controller.registerHandler(HttpMethod.GET,
                "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_sync_meta", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        if (redirectToLeader(request, response)) {
            return;
        }

        boolean force = true;
        String forceStr = request.getSingleParameter(FORCE);
        if (!Strings.isNullOrEmpty(forceStr)) {
            if (forceStr.equalsIgnoreCase("true")) {
                force = true;
            } else if (forceStr.equalsIgnoreCase("false")) {
                force = false;
            } else {
                response.appendContent("Bad force parameter");
                sendResult(request, response);
                return;
            }
        }

        String dbName = request.getSingleParameter(DB_KEY);
        String tableName = request.getSingleParameter(TABLE_KEY);
        if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName)) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                    "both database and table should be provided.");
        }

        GlobalStateMgr.getCurrentState().getStarMgrMetaSyncer().syncTableMeta(dbName, tableName, force);
        response.appendContent("OK");
        sendResult(request, response);
    }
}
