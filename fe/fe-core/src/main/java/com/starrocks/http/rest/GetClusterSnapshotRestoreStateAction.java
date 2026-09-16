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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.common.Config;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.lake.snapshot.RestoreClusterSnapshotMgr;
import io.netty.handler.codec.http.HttpMethod;

public class GetClusterSnapshotRestoreStateAction extends RestBaseAction {
    public GetClusterSnapshotRestoreStateAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/v2/get_cluster_snapshot_restore_state",
                new GetClusterSnapshotRestoreStateAction(controller));
    }

    // Historically anonymous; gated for backward compatibility until enable_http_auth flips on.
    @Override
    public boolean needAuth() {
        return Config.enable_http_auth;
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws AccessDeniedException {
        requireOperateIfHttpAuthEnabled();

        response.setContentType("application/json");

        RestResult result = new RestResult();
        result.addResultEntry("cluster_snapshot_restore_state",
                RestoreClusterSnapshotMgr.isRestoring() ? "restoring" : "finished");
        sendResult(request, response, result);
    }
}
