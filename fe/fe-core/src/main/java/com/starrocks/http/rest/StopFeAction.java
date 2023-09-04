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

import com.starrocks.StarRocksFE;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.handler.codec.http.HttpMethod;

public class StopFeAction extends RestBaseAction {
    public StopFeAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/stop", new StopFeAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws AccessDeniedException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkActionOnSystem(currentUser, PrivilegeType.OPERATE);

        response.setContentType("application/json");
        RestResult result = new RestResult();

        if (StarRocksFE.stopped) {
            result.addResultEntry("Message", "FE is shutting down");
        } else {
            StarRocksFE.stopped = true;
            result.addResultEntry("Message", "Stop success");
        }

        sendResult(request, response, result);
    }
}
